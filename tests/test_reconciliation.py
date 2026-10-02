"""Reconciliation lifecycle, adapter, ownership and persistence regression tests."""

import io
import json
import logging
import os
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from inventory import BATCH_SIZE, InventoryStore, action_lock, contained
from media_library import EmbyAdapter, Identity, OwnedMedia, Snapshot
from plugin import Plugin
from reconciliation import Reconciliation

LOG = logging.getLogger(__name__)


class Query:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)

    def all(self):
        return self

    def select_related(self, *args):
        return self

    def only(self, *args):
        return self

    def order_by(self, *args):
        return self

    def count(self):
        return len(self.rows)

    def iterator(self, **kwargs):
        yield from list(self.rows)

    def filter(self, **kwargs):
        def matches(rel):
            return all(
                (
                    getattr(rel.episode, "series", None)
                    if k == "episode__series"
                    else getattr(rel, k)
                )
                == v
                for k, v in kwargs.items()
            )

        return Query([r for r in self.rows if matches(r)])


def media(id, kind="movie", **values):
    defaults = dict(
        id=id,
        uuid=f"uuid-{id}",
        name=f"Title {id}",
        year=2000,
        tmdb_id=str(id),
        imdb_id="",
        description="",
        genre="",
        rating="",
        logo=None,
    )
    defaults.update(values)
    return NS(**defaults)


def relation(obj, kind="movie", stream=None):
    return NS(
        **{kind: obj},
        m3u_account_id=1,
        m3u_account=NS(id=1),
        stream_id=stream or str(obj.id),
        category=NS(name="Action"),
        custom_properties={"episodes_fetched": True},
        external_series_id=str(obj.id),
    )


@pytest.fixture
def library(tmp_path, monkeypatch):
    monkeypatch.setenv("VOD2MLIB_STATE_DIR", str(tmp_path / "state"))
    rows = {"movies": [], "series": [], "episodes": []}
    models = NS(
        M3UMovieRelation=NS(objects=Query(rows["movies"])),
        M3USeriesRelation=NS(objects=Query(rows["series"])),
        M3UEpisodeRelation=NS(objects=Query(rows["episodes"])),
    )
    monkeypatch.setitem(sys.modules, "apps.vod.models", models)
    calls = []
    monkeypatch.setitem(
        sys.modules,
        "apps.vod.tasks",
        NS(refresh_series_episodes=lambda **kw: calls.append(kw)),
    )
    p = Plugin()
    monkeypatch.setattr(p, "_eligible_vod_relations", lambda query, _: query)
    monkeypatch.setattr(
        p, "_scan_all_vods", lambda *_: {"status": "ok", "message": "Scan complete"}
    )
    settings = dict(
        root_folder=str(tmp_path / "Movies"),
        series_root_folder=str(tmp_path / "Series"),
        dispatcharr_url="http://dispatcharr.test:9191",
        batch_size="all",
        series_batch_size="all",
        generate_nfo=True,
        generate_series_nfo=True,
    )
    state = {"snapshot": Snapshot([]), "error": None, "requests": []}

    class Adapter:
        def get_snapshot(self, libraries, media_types):
            state["requests"].append((libraries, media_types))
            if state["error"]:
                raise state["error"]
            return state["snapshot"]

        def list_libraries(self):
            return [{"Id": "lib", "Name": "Movies"}]

    monkeypatch.setattr("reconciliation.create_adapter", lambda _: Adapter())
    monkeypatch.setattr("plugin.create_adapter", lambda _: Adapter())

    def run(action="generate_movies", **updates):
        return p._run_locked_action(
            action, {}, {"logger": LOG, "settings": {**settings, **updates}}
        )

    return NS(
        p=p,
        rows=rows,
        state=state,
        settings=settings,
        run=run,
        tmp=tmp_path,
        calls=calls,
    )


def owned(obj, kind="movie", episodes=()):
    return OwnedMedia(
        Identity(kind, obj.name, obj.year, obj.tmdb_id, obj.imdb_id), set(episodes)
    )


def test_generation_acquisition_next_batch_removal_and_restoration(library):
    a, b, c = [media(i) for i in (1, 2, 3)]
    library.rows["movies"].extend([relation(a), relation(b)])
    first = library.run()
    assert first["created_strm"] == 2
    paths = list(Path(library.settings["root_folder"]).rglob("*.strm"))
    assert len(paths) == 2
    library.state["snapshot"] = Snapshot([owned(b)])
    library.rows["movies"].insert(0, relation(c))
    result = library.run(media_library_enabled=True, batch_size="1")
    assert result["created_strm"] == 1
    assert result["reconciliation"]["deleted"] == 1
    assert not next(p for p in paths if "Title 2" in str(p)).exists()
    library.state["snapshot"] = Snapshot([])
    result = library.run(media_library_enabled=True)
    assert result["created_strm"] == 1
    assert len(list(Path(library.settings["root_folder"]).rglob("*.strm"))) == 3


@pytest.mark.parametrize("mode,remaining", [("show", 0), ("episodes", 1)])
def test_tv_modes_and_existing_folder_shortcut(library, mode, remaining):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    for n in (1, 2):
        ep = media(n, series=show, season_number=0, episode_number=n)
        library.rows["episodes"].append(relation(ep, "episode"))
    assert library.run("generate_series")["episodes_created"] == 2
    library.calls.clear()
    library.state["snapshot"] = Snapshot([owned(show, "series", [(0, 1)])])
    result = library.run(
        "generate_series", media_library_enabled=True, media_tv_mode=mode
    )
    assert result["reconciliation"]["deleted"] == 2 - remaining
    assert (
        len(list(Path(library.settings["series_root_folder"]).rglob("*.strm")))
        == remaining
    )
    if mode == "show":
        assert not library.calls  # no provider episode fetch for an owned show
    else:
        library.rows["episodes"].append(
            relation(
                media(3, series=show, season_number=0, episode_number=3), "episode"
            )
        )
        result = library.run(
            "generate_series", media_library_enabled=True, media_tv_mode=mode
        )
        assert result["episodes_created"] == 1


@pytest.mark.parametrize(
    "policy,status,created", [("continue", "ok", 1), ("stop", "error", 0)]
)
def test_failure_policy_no_server_deletion(library, policy, status, created):
    library.rows["movies"].append(relation(media(1)))
    library.run()
    library.rows["movies"].append(relation(media(2)))
    library.state["error"] = RuntimeError("Offline")
    result = library.run(media_library_enabled=True, media_server_failure=policy)
    assert result["status"] == status
    assert (
        len(list(Path(library.settings["root_folder"]).rglob("*.strm"))) == 1 + created
    )
    if policy == "continue":
        assert "WARNING" in result["message"]
        assert result["reconciliation"]["deleted"] == 0


def test_selected_libraries_and_shared_full_rescan_snapshot(library):
    result = library.run(
        "rescan_all",
        media_library_enabled=True,
        media_library_scope="selected",
        media_library_ids="a, b",
    )
    assert result["status"] == "ok"
    assert library.state["requests"] == [(["a", "b"], ["movie", "series"])]
    result = library.run(
        media_library_enabled=True,
        media_library_scope="selected",
        media_library_ids="",
        media_server_failure="stop",
    )
    assert result["status"] == "error"


@pytest.mark.parametrize(
    "timing,action,deletions",
    [
        ("disabled", "generate_movies", 0),
        ("rescan", "generate_movies", 0),
        ("rescan", "rescan_all", 1),
    ],
)
def test_duplicate_cleanup_timing(library, timing, action, deletions):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    library.state["snapshot"] = Snapshot([owned(obj)])
    result = library.run(
        action, media_library_enabled=True, media_duplicate_cleanup=timing
    )
    assert result["reconciliation"]["deleted"] == deletions
    assert result.get("created_strm", 0) == 0


def test_preview_then_selective_cleanup(library):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    library.state["snapshot"] = Snapshot([owned(obj)])
    result = library.run("preview_cleanup", media_library_enabled=True)
    assert result["reconciliation"]["candidate"] == 1
    assert list(Path(library.settings["root_folder"]).rglob("*.strm"))
    result = library.run("selective_cleanup", media_library_enabled=True)
    assert result["reconciliation"]["deleted"] == 1


def test_immediate_m3u_removal_reappearance_uuid_change(library):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    # UUID changes but account/provider source ID stays live.
    obj.uuid = "replacement-uuid"
    assert (
        library.run("selective_cleanup", m3u_cleanup_enabled=True)["reconciliation"][
            "deleted"
        ]
        == 0
    )
    library.rows["movies"].clear()
    result = library.run("selective_cleanup", m3u_cleanup_enabled=True)
    assert result["reconciliation"]["deleted"] == 1
    library.rows["movies"].append(relation(obj))
    assert library.run()["created_strm"] == 1


def test_m3u_ignores_category_and_generation_batch(library):
    library.rows["movies"].extend([relation(media(1)), relation(media(2))])
    library.run()
    library.rows["movies"].pop()
    result = library.run("rescan_all", m3u_cleanup_enabled=True, batch_size="1")
    assert result["reconciliation"]["deleted"] == 1
    assert len(list(Path(library.settings["root_folder"]).rglob("*.strm"))) == 1


def test_m3u_manual_timing(library):
    library.rows["movies"].append(relation(media(1)))
    library.run()
    library.rows["movies"].clear()
    assert (
        library.run(
            "rescan_all", m3u_cleanup_enabled=True, m3u_cleanup_timing="manual"
        )["reconciliation"]["deleted"]
        == 0
    )
    assert (
        library.run(
            "selective_cleanup", m3u_cleanup_enabled=True, m3u_cleanup_timing="manual"
        )["reconciliation"]["deleted"]
        == 1
    )


def test_failed_m3u_refresh_preserves_files(library, monkeypatch):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].append(
        relation(media(1, series=show, season_number=1, episode_number=1), "episode")
    )
    library.run("generate_series")
    library.rows["episodes"].clear()
    monkeypatch.setattr(
        Reconciliation,
        "refresh_complete",
        lambda *a: (_ for _ in ()).throw(RuntimeError("Incomplete")),
    )
    result = library.run("selective_cleanup", m3u_cleanup_enabled=True)
    assert result["reconciliation"]["deleted"] == 0
    assert result["reconciliation"]["warnings"]
    assert list(Path(library.settings["series_root_folder"]).rglob("*.strm"))


def test_server_failure_and_m3u_cleanup_independent(library):
    library.rows["movies"].append(relation(media(1)))
    library.run()
    library.rows["movies"].clear()
    library.state["error"] = RuntimeError("Offline")
    result = library.run(
        "selective_cleanup", media_library_enabled=True, m3u_cleanup_enabled=True
    )
    assert result["reconciliation"]["deleted"] == 1
    assert result["reconciliation"]["warnings"]


def test_legacy_adoption_and_ambiguous_preservation(library):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    folder, filename, _, _ = library.p._movie_target_paths(
        obj, library.settings["root_folder"]
    )
    Path(folder).mkdir(parents=True)
    strm = Path(folder) / filename
    strm.write_text(
        library.p._build_proxy_url(
            library.settings["dispatcharr_url"], "movie", obj.uuid, "1"
        )
    )
    nfo = strm.with_suffix(".nfo")
    nfo.write_text("<movie><title>Custom</title></movie>")
    other = Path(folder) / "user.strm"
    other.write_text("http://unrelated/video")
    library.state["snapshot"] = Snapshot([owned(obj)])
    result = library.run(
        "selective_cleanup", media_library_enabled=True, deletion_scope="strm_nfo"
    )
    assert result["reconciliation"]["deleted"] == 1
    assert nfo.exists() and other.exists()
    assert result["reconciliation"]["preserved"] == 1


def test_edited_nfo_and_artwork_preserved(library):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    strm = next(Path(library.settings["root_folder"]).rglob("*.strm"))
    nfo = strm.with_suffix(".nfo")
    nfo.write_text("Edited")
    artwork = strm.parent / "poster.jpg"
    artwork.write_bytes(b"picture")
    subtitle = strm.parent / "movie.srt"
    subtitle.write_text("subtitle")
    library.state["snapshot"] = Snapshot([owned(obj)])
    result = library.run(media_library_enabled=True, deletion_scope="strm_nfo")
    assert result["reconciliation"]["deleted"] == 1
    assert nfo.read_text() == "Edited" and artwork.exists() and subtitle.exists()


def test_unchanged_generated_nfo_deleted(library):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    library.state["snapshot"] = Snapshot([owned(obj)])
    library.run(media_library_enabled=True, deletion_scope="strm_nfo")
    assert not list(Path(library.settings["root_folder"]).rglob("*.nfo"))


def test_edited_strm_preserved_even_full_refresh(library):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    strm = next(Path(library.settings["root_folder"]).rglob("*.strm"))
    strm.write_text("http://user-custom/video")
    library.state["snapshot"] = Snapshot([owned(obj)])
    result = library.run("rescan_all", media_library_enabled=True)
    assert result["reconciliation"]["preserved"] == 1
    assert strm.read_text() == "http://user-custom/video"
    library.state["snapshot"] = Snapshot([])
    library.run("rescan_all", media_library_enabled=True)
    assert strm.read_text() == "http://user-custom/video"


def test_schema_persistence_missing_files_and_rollback(tmp_path):
    store = InventoryStore(tmp_path / "state")
    file = tmp_path / "movie.strm"
    file.write_text("url")
    identity = Identity("movie", "A", 2000, "1")
    store.record(str(file), identity, "source")
    assert store.db.execute("PRAGMA user_version").fetchone()[0] == 1
    store.close()
    store = InventoryStore(tmp_path / "state")
    assert list(store.rows())[0]["tmdb"] == "1"
    with pytest.raises(sqlite3.IntegrityError):
        with store.db:
            store.db.execute("DELETE FROM files")
            store.db.execute("INSERT INTO files(path) VALUES (?)", ("invalid",))
    assert len(list(store.rows())) == 1
    file.unlink()
    assert store.delete(list(store.rows())[0], [str(tmp_path)]) == "missing"
    assert not list(store.rows())
    store.close()


def test_path_containment(tmp_path):
    root = tmp_path / "media"
    root.mkdir()
    outside = tmp_path / "media-other"
    outside.mkdir()
    file = outside / "movie.strm"
    file.write_text("url")
    store = InventoryStore(tmp_path / "state")
    store.record(str(file), Identity("movie", "A", 2000))
    assert store.delete(list(store.rows())[0], [str(root)]) == "preserved"
    assert not contained(str(root / ".." / "media-other" / "movie.strm"), [str(root)])
    assert file.exists()
    store.close()


def test_symlink_containment(tmp_path):
    root = tmp_path / "media"
    root.mkdir()
    outside = tmp_path / "other"
    outside.mkdir()
    try:
        (root / "linked").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("Symlinks unavailable on this Windows host")
    assert not contained(str(root / "linked" / "movie.strm"), [str(root)])


def test_process_lock(tmp_path):
    with action_lock(tmp_path):
        with pytest.raises(RuntimeError, match="Another"):
            with action_lock(tmp_path):
                pass
    with action_lock(tmp_path):
        pass


@pytest.mark.parametrize(
    "candidate,expected",
    [
        (Identity("movie", "Different title", None, "1"), True),
        (Identity("series", "A", 2000, "1"), False),
        (Identity("movie", "A", 2000, "2"), False),
        (Identity("movie", "A", 2000, "1", "tt2"), False),
        (Identity("movie", "A", 2000), True),
        (Identity("movie", "A", None), False),
        (Identity("movie", "A", 2001), False),
        (Identity("movie", "A extended", 2000), False),
    ],
)
def test_matching_rules(candidate, expected):
    snapshot = Snapshot([OwnedMedia(Identity("movie", "A", 2000, "1", "tt1"))])
    assert snapshot.owns(candidate) == expected


def test_ids_unavailable_fallback_clean_title():
    p = Plugin()
    snapshot = Snapshot(
        [OwnedMedia(Identity("movie", "A", 2000, imdb="tt1"))],
        lambda title: p._extract_clean_name_and_year(title)[0],
    )
    assert snapshot.owns(Identity("movie", "EN - A 4K", 2000, tmdb="1"))
    assert snapshot.text is not None


def test_id_matching_does_not_build_title_index():
    snapshot = Snapshot([OwnedMedia(Identity("movie", "A", 2000, "1"))])
    assert snapshot.owns(Identity("movie", "A", 2000, "1"))
    assert snapshot.text is None


def test_adapter_paginated_snapshot_mixed_sources_and_specials(monkeypatch):
    adapter = EmbyAdapter("http://emby", "secret")
    adapter.PAGE_SIZE = 2
    items = [
        {
            "Id": "s",
            "Type": "Series",
            "Name": "Show",
            "ProductionYear": 2000,
            "ProviderIds": {"Tmdb": "10"},
        },
        {
            "Id": "e",
            "Type": "Episode",
            "SeriesId": "s",
            "ParentIndexNumber": 0,
            "IndexNumber": 1,
            "IndexNumberEnd": 2,
            "Path": "/real.mkv",
        },
        {"Id": "strm", "Type": "Movie", "Name": "Fake", "Path": "/fake.strm"},
        {
            "Id": "mixed",
            "Type": "Movie",
            "Name": "Movie",
            "ProviderIds": {"Imdb": "tt1"},
            "MediaSources": [
                {"Path": "/a.strm"},
                {"Path": "/a.mkv", "Protocol": "File"},
            ],
        },
        {
            "Id": "remote",
            "Type": "Movie",
            "Name": "Remote",
            "Path": "http://example/video.mp4",
        },
    ]
    requests = []

    def get(path, params):
        requests.append(params)
        return {
            "Items": items[
                params["StartIndex"] : params["StartIndex"] + params["Limit"]
            ],
            "TotalRecordCount": len(items)
            if params["EnableTotalRecordCount"] == "true"
            else 0,
        }

    monkeypatch.setattr(adapter, "_get", get)
    snapshot = adapter.get_snapshot(["library"], ["movie", "series"])
    assert len(requests) == 4
    assert all(
        r["ParentId"] == "library"
        and r["EnableImages"] == "false"
        and r["EnableUserData"] == "false"
        for r in requests
    )
    assert len(snapshot.media) == 2
    show = Identity("series", "Show", 2000, "10")
    assert snapshot.owns(show, (0, 2), "episodes")
    assert not snapshot.owns(show, (0, 3), "episodes")


@pytest.mark.parametrize("failure", ["short", "repeated", "changed", "invalid"])
def test_adapter_discards_incomplete_snapshot(monkeypatch, failure):
    adapter = EmbyAdapter("http://emby", "secret")
    adapter.PAGE_SIZE = 1

    def get(path, params):
        if params["Limit"] == 0:
            return {"Items": [], "TotalRecordCount": 1 if failure == "changed" else 2}
        if params["StartIndex"] == 0:
            return {
                "Items": [{"Id": "a", "Type": "Movie", "Path": "/a.mkv"}],
                "TotalRecordCount": 2,
            }
        if failure == "short":
            return {"Items": [], "TotalRecordCount": 0}
        if failure == "repeated":
            return {"Items": [{"Id": "a"}], "TotalRecordCount": 0}
        if failure == "changed":
            return {
                "Items": [{"Id": "b", "Type": "Movie", "Path": "/b.mkv"}],
                "TotalRecordCount": 0,
            }
        return {"Items": None, "TotalRecordCount": 0}

    monkeypatch.setattr(adapter, "_get", get)
    with pytest.raises(ValueError):
        adapter.get_snapshot([], ["movie"])


def test_adapter_http_header_and_timeout(monkeypatch):
    requests = []

    class Response(io.BytesIO):
        pass

    def open(request, timeout):
        requests.append((request, timeout))
        return Response(b'{"Items":[{"Id":"1","Name":"Movies"}]}')

    monkeypatch.setattr("media_library.urlopen", open)
    adapter = EmbyAdapter("http://emby/emby", "secret")
    assert adapter.list_libraries()[0]["Id"] == "1"
    request, timeout = requests[0]
    assert request.get_header("X-emby-token") == "secret" and timeout == 30
    assert "secret" not in request.full_url


def test_http_failure_does_not_leak_token(monkeypatch):
    monkeypatch.setattr(
        "media_library.urlopen",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("secret")),
    )
    with pytest.raises(RuntimeError) as exc:
        EmbyAdapter("http://emby", "secret").list_libraries()
    assert "secret" not in str(exc.value)


def test_runtime_manifest_agreement_and_schedule_snapshot():
    manifest = json.loads(Path("plugin.json").read_text(encoding="utf8"))
    assert [
        (f["id"], f.get("default"), f.get("options")) for f in manifest["fields"]
    ] == [(f["id"], f.get("default"), f.get("options")) for f in Plugin.fields]
    assert [a["id"] for a in manifest["actions"]] == [a["id"] for a in Plugin.actions]
    assert manifest["version"] == Plugin.version
    fields = {f["id"] for f in Plugin.fields}
    settings = {f["id"]: f.get("default") for f in Plugin.fields if "default" in f}
    snapshot = {k: v for k, v in settings.items() if not k.startswith("schedule_")}
    assert {
        "media_library_enabled",
        "media_server_token",
        "m3u_cleanup_enabled",
        "deletion_scope",
    } <= fields & snapshot.keys()


def test_movie_inventory_flushes_bulk_batches(library, monkeypatch):
    sizes = []
    original = InventoryStore.record_many

    def record_many(store, records):
        sizes.append(len(records))
        return original(store, records)

    monkeypatch.setattr(InventoryStore, "record_many", record_many)
    library.rows["movies"].extend(relation(media(i)) for i in range(1, BATCH_SIZE + 2))
    assert library.run(generate_nfo=False)["created_strm"] == BATCH_SIZE + 1
    assert sizes == [BATCH_SIZE, 1]


def test_episode_queue_bounded_without_deadlock(library):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].extend(
        relation(media(i, series=show, season_number=1, episode_number=i), "episode")
        for i in range(1, BATCH_SIZE + 102)
    )
    result = library.run("generate_series", generate_series_nfo=False)
    assert result["episodes_created"] == BATCH_SIZE + 101
    store = InventoryStore(library.tmp / "state")
    assert len(list(store.rows())) == BATCH_SIZE + 101
    store.close()


def test_record_many_uses_executemany(tmp_path):
    store = InventoryStore(tmp_path / "state")
    calls = []

    class Proxy:
        def __init__(self, db):
            self.db = db

        def execute(self, *args):
            return self.db.execute(*args)

        def executemany(self, sql, rows):
            calls.append((sql, len(rows)))
            return self.db.executemany(sql, rows)

        def __enter__(self):
            self.db.__enter__()
            return self

        def __exit__(self, *args):
            return self.db.__exit__(*args)

        def close(self):
            return self.db.close()

    store.db = Proxy(store.db)
    records = []
    for i in range(3):
        path = tmp_path / f"{i}.strm"
        path.write_text("url")
        records.append((str(path), Identity("movie", str(i), 2000), str(i), None, None))
    store.record_many(records)
    assert len(calls) == 2 and all(count == 3 for _, count in calls)
    store.close()


def test_m3u_removed_show_with_orphan_episode_rows(library):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].append(
        relation(media(1, series=show, season_number=1, episode_number=1), "episode")
    )
    assert library.run("generate_series")["episodes_created"] == 1
    library.rows["series"].clear()
    result = library.run("selective_cleanup", m3u_cleanup_enabled=True)
    assert result["reconciliation"]["deleted"] == 1


def test_m3u_episode_removal_and_reappearance(library, monkeypatch):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    ep1 = relation(media(1, series=show, season_number=1, episode_number=1), "episode")
    ep2 = relation(media(2, series=show, season_number=1, episode_number=2), "episode")
    library.rows["episodes"].extend([ep1, ep2])
    library.run("generate_series")

    def refresh_complete(*args):
        library.rows["episodes"][:] = [ep1]

    monkeypatch.setattr(Reconciliation, "refresh_complete", refresh_complete)
    result = library.run("selective_cleanup", m3u_cleanup_enabled=True)
    assert result["reconciliation"]["deleted"] == 1
    library.rows["episodes"].append(ep2)
    result = library.run("generate_series", refresh_existing=True)
    assert result["episodes_created"] == 1


def test_m3u_refresh_only_shows_with_generated_output(library, monkeypatch):
    show = media(10)
    other = media(20)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].append(
        relation(media(1, series=show, season_number=1, episode_number=1), "episode")
    )
    library.run("generate_series")
    library.rows["series"].append(relation(other, "series"))
    refreshed = []
    monkeypatch.setattr(
        Reconciliation,
        "refresh_complete",
        lambda self, rel, _: refreshed.append(rel.series.id),
    )
    result = library.run("selective_cleanup", m3u_cleanup_enabled=True)
    assert result["status"] == "ok"
    assert refreshed == [10]


def test_m3u_reuses_complete_census_when_no_provider_refresh(library, monkeypatch):
    library.rows["movies"].append(relation(media(1)))
    library.run()
    passes = []
    census = Reconciliation.census

    def counted(self):
        passes.append(True)
        return census(self)

    monkeypatch.setattr(Reconciliation, "census", counted)
    library.rows["movies"].clear()
    result = library.run("selective_cleanup", m3u_cleanup_enabled=True)
    assert passes == [True]
    assert result["reconciliation"]["deleted"] == 1


def test_bulk_listing_matches_separate_strm_and_file_versions(monkeypatch):
    adapter = EmbyAdapter("http://emby", "secret")
    versions = [
        {
            "Id": "strm",
            "Type": "Movie",
            "Name": "Movie",
            "ProviderIds": {"Tmdb": "1"},
            "Path": "/movie.strm",
        },
        {
            "Id": "file",
            "Type": "Movie",
            "Name": "Movie",
            "ProviderIds": {"Tmdb": "1"},
            "Path": "/movie.mkv",
        },
    ]

    def get(path, params):
        return {"Items": versions if params["Limit"] else [], "TotalRecordCount": 2}

    monkeypatch.setattr(adapter, "_get", get)
    snapshot = adapter.get_snapshot([], ["movie"])
    assert len(snapshot.media) == 1
    assert snapshot.owns(Identity("movie", "Movie", tmdb="1"))


@pytest.mark.parametrize(
    "response,completion,error",
    [
        ({"episodes": {"0": []}}, True, None),
        ({"episodes": {"1": [{"id": "1", "episode_num": 1}]}}, True, None),
        (
            {"episodes": {"1": [{"id": "1", "episode_num": 1}]}},
            False,
            "did not complete",
        ),
        ({}, True, "Incomplete"),
        ({"episodes": {"1": [{"id": "1", "episode_num": "invalid"}]}}, True, None),
    ],
)
def test_refresh_completion_verification(monkeypatch, response, completion, error):
    calls = []

    class Client:
        def __init__(self, *args):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def get_series_info(self, id):
            return response

    monkeypatch.setitem(sys.modules, "core.xtream_codes", NS(Client=Client))

    class Values:
        def iterator(self, **kwargs):
            return iter(
                [
                    str(e["id"])
                    for eps in response.get("episodes", {}).values()
                    for e in eps
                ]
            )

    class Episodes:
        def filter(self, **kwargs):
            return self

        def values_list(self, *args, **kwargs):
            return Values()

    monkeypatch.setitem(
        sys.modules, "apps.vod.models", NS(M3UEpisodeRelation=NS(objects=Episodes()))
    )
    rel = NS(
        m3u_account=NS(
            server_url="http://provider",
            username="u",
            password="p",
            get_user_agent_string=lambda: "agent",
        ),
        external_series_id="1",
        series=NS(),
        last_episode_refresh=1,
        refresh_from_db=lambda: None,
    )

    def refresher(**kwargs):
        calls.append(kwargs)
        if completion:
            rel.last_episode_refresh = 2

    invalid_number = (
        response.get("episodes", {}).get("1", [{}])[0].get("episode_num") == "invalid"
    )
    if error or invalid_number:
        with pytest.raises(ValueError, match=error or "invalid literal"):
            Reconciliation.refresh_complete(rel, refresher)
    else:
        Reconciliation.refresh_complete(rel, refresher)
        assert calls[0]["episodes_data"]  # complete empty response must not refetch


def test_unknown_schema_is_rejected(tmp_path):
    store = InventoryStore(tmp_path)
    store.db.execute("PRAGMA user_version=99")
    store.close()
    with pytest.raises(ValueError, match="Unsupported"):
        InventoryStore(tmp_path)


def test_generated_nfo_is_preserved_by_default(library):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    library.state["snapshot"] = Snapshot([owned(obj)])
    library.run(media_library_enabled=True)
    assert list(Path(library.settings["root_folder"]).rglob("*.nfo"))


def test_edited_generated_nfo_shared_show_cleanup(library):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].extend(
        relation(media(i, series=show, season_number=1, episode_number=i), "episode")
        for i in (1, 2)
    )
    library.run("generate_series")
    library.state["snapshot"] = Snapshot([owned(show, "series", [(1, 1)])])
    library.run(
        "generate_series",
        media_library_enabled=True,
        media_tv_mode="episodes",
        deletion_scope="strm_nfo",
    )
    root = Path(library.settings["series_root_folder"])
    assert len(list(root.rglob("tvshow.nfo"))) == 1
    assert len(list(root.rglob("*.nfo"))) == 2  # shared show and one retained episode
    library.run(
        "generate_series",
        media_library_enabled=True,
        media_tv_mode="show",
        deletion_scope="strm_nfo",
    )
    assert not list(root.rglob("*.nfo"))


def test_strm_inventory_stores_raw_url_without_hash(library, monkeypatch):
    # Movie generation with NFOs off must not call the hash helper at all.
    def unexpected_hash(*args):
        raise AssertionError("STRMs should use URL equality")

    monkeypatch.setattr("plugin.file_hash", unexpected_hash)
    monkeypatch.setattr("inventory.file_hash", unexpected_hash)
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    assert library.run(generate_nfo=False)["created_strm"] == 1
    store = InventoryStore(library.tmp / "state")
    row = next(iter(store.rows()))
    assert row["strm_url"] == library.p._build_proxy_url(
        library.settings["dispatcharr_url"], "movie", obj.uuid, "1"
    )
    assert "strm_hash" not in row.keys()
    store.close()
    library.state["snapshot"] = Snapshot([owned(obj)])
    assert (
        library.run(media_library_enabled=True, generate_nfo=False)["reconciliation"][
            "deleted"
        ]
        == 1
    )


def test_apply_schedule_persists_reconciliation_settings(library, monkeypatch):
    captured = {}

    def update_or_create(**kwargs):
        captured.update(kwargs)
        return NS(), True

    models = NS(
        PeriodicTask=NS(objects=NS(update_or_create=update_or_create)),
        CrontabSchedule=NS(objects=NS(get_or_create=lambda **kwargs: (NS(), True))),
    )
    monkeypatch.setitem(sys.modules, "django_celery_beat.models", models)
    result = library.run(
        "apply_schedule",
        media_library_enabled=True,
        media_server_token="key",
        media_tv_mode="episodes",
        m3u_cleanup_enabled=True,
        deletion_scope="strm_nfo",
    )
    assert result["status"] == "ok"
    snapshot = json.loads(captured["defaults"]["kwargs"])["settings"]
    assert snapshot["media_library_enabled"] is True
    assert snapshot["media_server_token"] == "key"
    assert snapshot["media_tv_mode"] == "episodes"
    assert snapshot["m3u_cleanup_enabled"] is True
    assert snapshot["deletion_scope"] == "strm_nfo"
    assert captured["defaults"]["queue"] == "dvr"


def test_whole_show_mode_never_fetches_owned_provider_episodes(library, monkeypatch):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].append(
        relation(media(1, series=show, season_number=1, episode_number=1), "episode")
    )
    library.run("generate_series")
    library.state["snapshot"] = Snapshot([owned(show, "series", [(1, 1)])])

    def forbidden(*args):
        raise AssertionError("Owned shows must be excluded before fetching episodes")

    monkeypatch.setattr(Reconciliation, "refresh_complete", forbidden)
    result = library.run(
        "rescan_all", media_library_enabled=True, m3u_cleanup_enabled=True
    )
    assert result["reconciliation"]["deleted"] == 1
    assert not result["reconciliation"]["warnings"]


def test_existing_binary_nfo_does_not_prevent_episode_generation(library):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].append(
        relation(media(1, series=show, season_number=1, episode_number=1), "episode")
    )
    folder, _, _ = library.p._series_target_folder(
        show, library.settings["series_root_folder"]
    )
    Path(folder).mkdir(parents=True)
    (Path(folder) / "tvshow.nfo").write_bytes(b"\xff\xfeCustom")
    result = library.run("generate_series")
    assert result["episodes_created"] == 1
    assert (Path(folder) / "tvshow.nfo").read_bytes() == b"\xff\xfeCustom"


def test_sidecar_deletion_failure_is_reported_and_retryable(library, monkeypatch):
    obj = media(1)
    library.rows["movies"].append(relation(obj))
    library.run()
    library.state["snapshot"] = Snapshot([owned(obj)])
    real_remove = os.remove

    def remove(path):
        if str(path).endswith(".nfo"):
            raise PermissionError("Sidecar is read-only")
        return real_remove(path)

    monkeypatch.setattr("inventory.os.remove", remove)
    result = library.run(media_library_enabled=True, deletion_scope="strm_nfo")
    assert result["reconciliation"]["deleted"] == 1
    assert result["reconciliation"]["errors"] == 1
    monkeypatch.setattr("inventory.os.remove", real_remove)
    result = library.run(media_library_enabled=True, deletion_scope="strm_nfo")
    assert result["reconciliation"]["deleted_nfo"] == 1
    assert not list(Path(library.settings["root_folder"]).rglob("*.nfo"))


def test_inventory_failure_cancels_bounded_workers(library, monkeypatch):
    show = media(10)
    library.rows["series"].append(relation(show, "series"))
    library.rows["episodes"].extend(
        relation(media(i, series=show, season_number=1, episode_number=i), "episode")
        for i in range(1, BATCH_SIZE + 102)
    )

    def failure(*args):
        raise sqlite3.OperationalError("Disk full")

    monkeypatch.setattr(InventoryStore, "record_many", failure)
    result = library.run("generate_series", generate_series_nfo=False)
    assert result["status"] == "error"
    assert "Disk full" in result["message"]
