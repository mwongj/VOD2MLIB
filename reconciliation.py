"""Action lifecycle: one snapshot, catalogue census, adoption and selective cleanup."""

import json
import os
import re
import sqlite3
import threading
from dataclasses import asdict
from pathlib import Path
from queue import Empty, Full, Queue
from urllib.parse import parse_qs, urlparse

try:
    from .inventory import BATCH_SIZE, InventoryStore, contained, strm_contents
    from .media_library import Identity, create_adapter
except ImportError:
    from inventory import BATCH_SIZE, InventoryStore, contained, strm_contents
    from media_library import Identity, create_adapter


def identity_for(plugin, obj, kind):
    title, title_year = plugin._extract_clean_name_and_year(obj.name or "")
    title, year = plugin._strip_redundant_trailing_year(
        title, getattr(obj, "year", None) or title_year
    )
    return Identity(
        kind,
        title,
        year,
        str(getattr(obj, "tmdb_id", "") or "").strip().lower(),
        str(getattr(obj, "imdb_id", "") or "").strip().lower(),
    )


def source_for(rel, kind):
    account = getattr(rel, "m3u_account_id", None) or getattr(
        getattr(rel, "m3u_account", None), "id", None
    )
    external = (
        getattr(rel, "external_series_id", None)
        if kind == "series"
        else getattr(rel, "stream_id", None)
    )
    return (
        json.dumps([kind, str(account), str(external)])
        if account is not None and external is not None
        else ""
    )


class Reconciliation:
    def __init__(self, plugin, settings, logger, directory):
        self.plugin, self.settings, self.logger = plugin, settings, logger
        self.store = InventoryStore(directory)
        self.snapshot = None
        self.queue = Queue(maxsize=BATCH_SIZE)
        self.counter_lock = threading.Lock()
        self.cancelled = threading.Event()
        self.report = {
            "excluded": 0,
            "deleted": 0,
            "deleted_nfo": 0,
            "removed_dirs": 0,
            "candidate": 0,
            "preserved": 0,
            "missing": 0,
            "errors": 0,
            "warnings": [],
        }
        self.roots = [
            settings.get("root_folder", "/VODS/Movies"),
            settings.get("series_root_folder", "/VODS/Series"),
        ]
        self.m3u_complete = False

    def warning(self, message):
        self.report["warnings"].append(message)
        self.logger.warning(message)

    def prepare(self, action):
        if self.settings.get("media_library_enabled", False):
            try:
                libraries = [
                    s.strip()
                    for s in self.settings.get("media_library_ids", "").split(",")
                    if s.strip()
                ]
                if (
                    self.settings.get("media_library_scope", "all") == "selected"
                    and not libraries
                ):
                    raise ValueError("Selected library scope requires library IDs")
                self.snapshot = create_adapter(self.settings).get_snapshot(
                    libraries
                    if self.settings.get("media_library_scope", "all") == "selected"
                    else [],
                    ["movie", "series"],
                )
                self.snapshot.clean_title = lambda title: (
                    self.plugin._extract_clean_name_and_year(title)[0]
                )
            except Exception as error:
                self.snapshot = None
                if self.settings.get("media_server_failure", "continue") == "stop":
                    raise
                self.warning(
                    f"Media library check failed; server cleanup disabled for this run: {error}"
                )
        m3u = self.settings.get("m3u_cleanup_enabled", False) and (
            action in ("preview_cleanup", "selective_cleanup")
            or action == "rescan_all"
            and self.settings.get("m3u_cleanup_timing", "rescan") == "rescan"
        )
        # Refresh failures must never establish episode absence.
        try:
            self.census()
            self.adopt()
            if m3u:
                for table in ("live", "live_series", "catalogue"):
                    self.store.db.execute(f"DELETE FROM {table}")
                self.census(refresh=True)
            self.m3u_complete = m3u
        except Exception as error:
            self.m3u_complete = False
            self.warning(
                f"Catalogue check incomplete; M3U cleanup disabled ({type(error).__name__})"
            )
        timing = self.settings.get("media_duplicate_cleanup", "every")
        server = self.snapshot is not None and (
            action in ("preview_cleanup", "selective_cleanup")
            or action in ("generate_movies", "generate_series", "rescan_all")
            and (timing == "every" or timing == "rescan" and action == "rescan_all")
        )
        self.cleanup(server, self.m3u_complete, action == "preview_cleanup")

    def census(self, refresh=False):
        from apps.vod.models import (
            M3UEpisodeRelation,
            M3UMovieRelation,
            M3USeriesRelation,
        )

        series_batch = []
        if refresh:
            from apps.vod.tasks import refresh_series_episodes
        for rel in (
            M3USeriesRelation.objects.select_related("series", "m3u_account")
            .all()
            .iterator(chunk_size=BATCH_SIZE)
        ):
            owned_show = (
                self.snapshot is not None
                and self.settings.get("media_tv_mode", "show") == "show"
                and self.snapshot.owns(identity_for(self.plugin, rel.series, "series"))
            )
            identity = identity_for(self.plugin, rel.series, "series")
            # Refresh only shows with generated output. The full census still
            # includes every account/category and is never limited by batch size.
            tracked = self.store.db.execute(
                """SELECT 1 FROM files WHERE kind='series' AND
                ((? != '' AND tmdb=?) OR (? != '' AND imdb=?) OR
                 (title=? AND year=? AND ? IS NOT NULL
                  AND (tmdb='' OR ?='' OR tmdb=?)
                  AND (imdb='' OR ?='' OR imdb=?))) LIMIT 1""",
                (identity.tmdb, identity.tmdb, identity.imdb, identity.imdb,
                 identity.title, str(identity.year) if identity.year else None,
                 identity.year, identity.tmdb, identity.tmdb, identity.imdb, identity.imdb),
            ).fetchone() if refresh else None
            if refresh and tracked and not owned_show:
                self.refresh_complete(rel, refresh_series_episodes)
            series_batch.append((str(rel.series.uuid), str(rel.m3u_account_id)))
            if len(series_batch) == BATCH_SIZE:
                with self.store.db:
                    self.store.db.executemany(
                        "INSERT OR IGNORE INTO live_series VALUES (?,?)", series_batch
                    )
                series_batch = []
        if series_batch:
            with self.store.db:
                self.store.db.executemany(
                    "INSERT OR IGNORE INTO live_series VALUES (?,?)", series_batch
                )
        # Full catalogue deliberately ignores native category eligibility and batches.
        batch = []
        for model, attr, kind in (
            (M3UMovieRelation, "movie", "movie"),
            (M3UEpisodeRelation, "episode", "series"),
        ):
            for rel in (
                model.objects.select_related(
                    attr,
                    "m3u_account",
                    *(["episode__series"] if attr == "episode" else []),
                )
                .all()
                .iterator(chunk_size=BATCH_SIZE)
            ):
                obj = getattr(rel, attr)
                if (
                    attr == "episode"
                    and not self.store.db.execute(
                        "SELECT 1 FROM live_series WHERE uuid=? AND account=?",
                        (str(obj.series.uuid), str(rel.m3u_account_id)),
                    ).fetchone()
                ):
                    continue
                identity = identity_for(
                    self.plugin, obj.series if attr == "episode" else obj, kind
                )
                position = (
                    (obj.season_number, obj.episode_number)
                    if attr == "episode"
                    else (None, None)
                )
                source = source_for(rel, "episode" if attr == "episode" else "movie")
                batch.append(
                    (
                        str(obj.uuid),
                        str(rel.stream_id),
                        json.dumps(asdict(identity)),
                        source,
                        *position,
                    )
                )
                if len(batch) == BATCH_SIZE:
                    self._census_batch(batch)
                    batch = []
        if batch:
            self._census_batch(batch)

    @staticmethod
    def refresh_complete(rel, refresher):
        # Dispatcharr's task swallows exceptions. Verify the provider response
        # and the persisted completion timestamp rather than trusting its return.
        from core.xtream_codes import Client

        account = rel.m3u_account
        with Client(
            account.server_url,
            account.username,
            account.password,
            account.get_user_agent_string(),
        ) as client:
            info = client.get_series_info(rel.external_series_id)
        if not isinstance(info, dict) or not isinstance(info.get("episodes"), dict):
            raise ValueError("Incomplete provider episode response")
        expected = set()
        for season, episodes in info["episodes"].items():
            if int(season) < 0:
                raise ValueError("Invalid provider season")
            if not isinstance(episodes, list) or any(
                not isinstance(e, dict) or not e.get("id") for e in episodes
            ):
                raise ValueError("Invalid provider episode response")
            for episode in episodes:
                if int(episode.get("episode_num", -1)) < 0:
                    raise ValueError("Invalid provider episode number")
                expected.add(str(episode["id"]))
        previous = rel.last_episode_refresh
        # A truthy empty season avoids Dispatcharr fetching an empty response again.
        refresher(
            account=account,
            series=rel.series,
            external_series_id=rel.external_series_id,
            episodes_data=info["episodes"] or {"0": []},
        )
        rel.refresh_from_db()
        if rel.last_episode_refresh is None or rel.last_episode_refresh == previous:
            raise ValueError("Provider episode refresh did not complete")
        from apps.vod.models import M3UEpisodeRelation

        actual = {
            str(value)
            for value in M3UEpisodeRelation.objects.filter(
                m3u_account=account, episode__series=rel.series
            )
            .values_list("stream_id", flat=True)
            .iterator(chunk_size=BATCH_SIZE)
        }
        if actual != expected:
            raise ValueError("Provider episode refresh was incomplete")

    def writable(self, path, uuid, kind):
        if not contained(path, self.roots):
            return False
        if not os.path.lexists(path):
            return True
        if os.path.islink(path):
            return False
        # Workers use short read-only connections; all writes stay on the parent.
        uri = Path(self.store.db_path).resolve().as_uri() + "?mode=ro"
        with sqlite3.connect(uri, uri=True) as reader:
            row = reader.execute(
                "SELECT strm_url FROM files WHERE path=?", (os.path.abspath(path),)
            ).fetchone()
        if row:
            return strm_contents(path) == row[0]
        try:
            if os.path.getsize(path) > 8192:
                return False
            parsed = urlparse(Path(path).read_text(encoding="utf-8").strip())
            host = urlparse(self.settings.get("dispatcharr_url", ""))
            return (parsed.scheme, parsed.netloc) == (
                host.scheme,
                host.netloc,
            ) and parsed.path == f"/proxy/vod/{kind}/{uuid}"
        except (OSError, UnicodeError):
            return False

    def _census_batch(self, batch):
        with self.store.db:
            self.store.db.executemany(
                "INSERT INTO catalogue VALUES (?,?,?,?,?,?)", batch
            )
            self.store.db.executemany(
                "INSERT OR IGNORE INTO live VALUES (?)",
                [(r[3],) for r in batch if r[3]],
            )

    def adopt(self):
        host = urlparse(self.settings.get("dispatcharr_url", ""))
        records = []
        for root in self.roots:
            for folder, dirs, files in os.walk(root):
                dirs[:] = [
                    d for d in dirs if not os.path.islink(os.path.join(folder, d))
                ]
                for name in files:
                    if not name.endswith(".strm"):
                        continue
                    path = os.path.abspath(os.path.join(folder, name))
                    if not contained(path, self.roots) or os.path.islink(path):
                        continue
                    if self.store.db.execute(
                        "SELECT 1 FROM files WHERE path=?", (path,)
                    ).fetchone():
                        continue
                    try:
                        if os.path.getsize(path) > 8192:
                            continue
                        parsed = urlparse(
                            Path(path).read_text(encoding="utf-8").strip()
                        )
                        match = re.fullmatch(
                            r"/proxy/vod/(movie|episode)/([^/]+)", parsed.path
                        )
                        if not match or (parsed.scheme, parsed.netloc) != (
                            host.scheme,
                            host.netloc,
                        ):
                            self.report["preserved"] += 1
                            continue
                        stream = parse_qs(parsed.query).get("stream_id", [None])[0]
                        sql = "SELECT * FROM catalogue WHERE uuid=?"
                        args = [match[2]]
                        if stream is not None:
                            sql += " AND stream=?"
                            args.append(stream)
                        rows = self.store.db.execute(sql, args).fetchall()
                        identities = {r["identity"] for r in rows}
                        if len(identities) != 1:
                            self.report["preserved"] += 1
                            continue
                        for row in rows:
                            records.append(
                                (
                                    path,
                                    Identity(**json.loads(row["identity"])),
                                    row["source"],
                                    (row["season"], row["episode"])
                                    if match[1] == "episode"
                                    else None,
                                    None,
                                )
                            )
                            if len(records) == BATCH_SIZE:
                                self.store.record_many(records)
                                records = []
                        # Legacy NFOs have no recorded generated hash: preserve them.
                    except (OSError, UnicodeError, ValueError):
                        self.report["preserved"] += 1

        if records:
            self.store.record_many(records)

    def cleanup(self, server, m3u, dry_run=False):
        for row in self.store.rows():
            identity = Identity(
                row["kind"], row["title"], row["year"], row["tmdb"], row["imdb"]
            )
            position = (
                (row["season"], row["episode"]) if row["kind"] == "series" else None
            )
            duplicate = server and self.snapshot.owns(
                identity, position, self.settings.get("media_tv_mode", "show")
            )
            absent = m3u and self.store.absent(row["path"])
            if not duplicate and not absent:
                if not os.path.lexists(row["path"]) and not dry_run:
                    self.store.forget(row["path"])
                    self.report["missing"] += 1
                continue
            was_existing = os.path.lexists(row["path"])
            try:
                outcome = self.store.delete(
                    row,
                    self.roots,
                    self.settings.get("deletion_scope", "strm") == "strm_nfo",
                    dry_run,
                    stats=self.report,
                )
                self.report[outcome] += 1
                self.logger.info(
                    "%s: %s (%s)",
                    outcome,
                    row["path"],
                    "media server" if duplicate else "M3U removal",
                )
            except OSError as error:
                if was_existing and not os.path.lexists(row["path"]):
                    self.report["deleted"] += 1
                self.report["errors"] += 1
                self.logger.error("Cleanup failed for %s: %s", row["path"], error)

    def owns(self, obj, kind, position=None):
        owned = self.snapshot is not None and self.snapshot.owns(
            identity_for(self.plugin, obj, kind),
            position,
            self.settings.get("media_tv_mode", "show"),
        )
        if owned:
            with self.counter_lock:
                self.report["excluded"] += 1
        return owned

    def record(self, path, obj, kind, rel, position=None, nfos=None):
        record = (
            path,
            identity_for(self.plugin, obj, kind),
            source_for(rel, "episode" if position else kind),
            position,
            nfos,
        )
        while not self.cancelled.is_set():
            try:
                self.queue.put(record, timeout=0.1)
                return
            except Full:
                continue
        raise RuntimeError("Inventory writes cancelled after action failure")

    def drain(self):
        records = []
        while len(records) < BATCH_SIZE:
            try:
                records.append(self.queue.get_nowait())
            except Empty:
                break
        if records:
            self.store.record_many(records)
