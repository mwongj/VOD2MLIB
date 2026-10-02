"""Action lifecycle: one snapshot, catalogue census, adoption and selective cleanup."""

import json
import os
import re
import sqlite3
import stat
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict
from functools import lru_cache
from itertools import islice
from pathlib import Path
from queue import Empty, Full, Queue
from urllib.parse import parse_qs, urlparse

try:
    from .inventory import BATCH_SIZE, InventoryStore, contained, strm_contents
    from .media_library import (
        Identity,
        LibrarySelectionError,
        create_adapter,
        resolve_library_ids,
    )
except ImportError:
    from inventory import BATCH_SIZE, InventoryStore, contained, strm_contents
    from media_library import (
        Identity,
        LibrarySelectionError,
        create_adapter,
        resolve_library_ids,
    )


def identity_from_fields(plugin, kind, name, year, tmdb, imdb):
    title, title_year = plugin._extract_clean_name_and_year(name or "")
    title, year = plugin._strip_redundant_trailing_year(title, year or title_year)
    return Identity(
        kind, title, year,
        str(tmdb or "").strip().lower(),
        str(imdb or "").strip().lower(),
    )


def identity_for(plugin, obj, kind):
    return identity_from_fields(
        plugin, kind, obj.name, getattr(obj, "year", None),
        getattr(obj, "tmdb_id", ""), getattr(obj, "imdb_id", ""),
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
        self.started_wall, self.started_cpu = time.perf_counter(), time.process_time()
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
            "timings": {},
            "worker_pid": os.getpid(),
            "adopted": 0,
            "discovery_scanned_roots": 0,
            "discovery_skipped_roots": 0,
        }
        self.roots = [
            settings.get("root_folder", "/VODS/Movies"),
            settings.get("series_root_folder", "/VODS/Series"),
        ]
        self.m3u_complete = False

    @contextmanager
    def measure(self, name, items=0):
        wall, cpu = time.perf_counter(), time.process_time()
        try:
            yield
        finally:
            with self.counter_lock:
                timing = self.report["timings"].setdefault(name, {
                    "wall_seconds": 0.0, "cpu_seconds": 0.0, "calls": 0, "items": 0,
                })
                timing["wall_seconds"] += time.perf_counter() - wall
                timing["cpu_seconds"] += time.process_time() - cpu
                timing["calls"] += 1
                timing["items"] += items

    def finish(self):
        timings = self.report["timings"]
        if "catalogue" in timings:
            children = [value for name, value in timings.items()
                        if name.startswith("catalogue_")]
            timings["catalogue_processing"] = {
                key: max(0.0, timings["catalogue"][key] - sum(t[key] for t in children))
                for key in ("wall_seconds", "cpu_seconds")
            }
        self.report["timings"]["total"] = {
            "wall_seconds": time.perf_counter() - self.started_wall,
            "cpu_seconds": time.process_time() - self.started_cpu,
            "calls": 1,
        }
        for timing in self.report["timings"].values():
            for key in ("wall_seconds", "cpu_seconds"):
                timing[key] = round(timing[key], 4)
        telemetry = {
            "worker_pid": self.report["worker_pid"], "timings": self.report["timings"],
            "recorded_at_unix": time.time(),
        }
        self.logger.info("VOD2MLIB timing summary: %s", json.dumps(telemetry, sort_keys=True))
        # Supervised workers have detached stdout. Keep the latest metrics in
        # persistent plugin state as well as the completed action result.
        path = Path(self.store.db_path).parent / "timings.json"
        temporary = path.with_suffix(".tmp")
        try:
            fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
            with os.fdopen(fd, "w", encoding="utf8") as output:
                json.dump(telemetry, output)
            os.replace(temporary, path)
        except OSError:
            self.warning("Could not save the timing report; action results still include timings")

    def catalogue_rows(self, query, name):
        iterator = query.iterator(chunk_size=BATCH_SIZE)
        while True:
            # Time batches rather than every row; includes cursor fetch/decoding.
            with self.measure(name):
                rows = list(islice(iterator, BATCH_SIZE))
            if not rows:
                break
            self.report["timings"][name]["items"] += len(rows)
            yield from rows

    def progress(self, message):
        callback = getattr(self.plugin, "_progress", None)
        if callback:
            callback(message)

    def warning(self, message):
        self.report["warnings"].append(message)
        self.logger.warning(message)

    def prepare(self, action):
        if action == "rebuild_inventory":
            # Discover output without any media-server checks, provider refreshes
            # or deletion. Existing ownership and generated NFO hashes survive.
            with self.measure("catalogue"):
                self.census()
            with self.measure("legacy_adoption"):
                self.adopt(force=True)
            return
        if self.settings.get("media_library_enabled", False):
            libraries = list(dict.fromkeys(
                s.strip() for s in self.settings.get("media_library_ids", "").split(",")
                if s.strip()
            ))
            # Configuration errors must not be treated as a transient server outage.
            if not libraries:
                raise ValueError(
                    "Enter library names or IDs before enabling media-library integration; "
                    "run List media libraries to find them"
                )
            try:
                self.progress("Fetching Emby snapshot")
                adapter = create_adapter(self.settings)
                adapter.progress = self.progress
                with self.measure("emby_library_listing"):
                    libraries = resolve_library_ids(adapter.list_libraries(), libraries)
                with self.measure("emby_snapshot"):
                    self.snapshot = adapter.get_snapshot(libraries, ["movie", "series"])
                self.snapshot.clean_title = lambda title: (
                    self.plugin._extract_clean_name_and_year(title)[0]
                )
            except LibrarySelectionError:
                raise
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
            with self.measure("catalogue"):
                self.census()
            with self.measure("legacy_adoption"):
                self.adopt()
            refreshed = False
            if m3u:
                with self.measure("m3u_tracked_show_check"):
                    refreshed = self.refresh_tracked_series()
            if refreshed:
                for table in ("live", "live_series", "catalogue"):
                    self.store.db.execute(f"DELETE FROM {table}")
                with self.measure("catalogue"):
                    self.census()
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
        with self.measure("cleanup"):
            self.cleanup(server, self.m3u_complete, action == "preview_cleanup")

    def census(self):
        from apps.vod.models import (
            M3UEpisodeRelation,
            M3UMovieRelation,
            M3USeriesRelation,
        )

        self.progress("Checking complete Dispatcharr catalogue")
        self.census_count = 0
        # No catalogue lookups happen while loading: sort/build this index once
        # after the complete census instead of updating it for every insert.
        self.store.db.execute("DROP INDEX IF EXISTS temp.catalogue_uuid")
        series_batch = []
        for account, uuid in self.catalogue_rows(
            M3USeriesRelation.objects.values_list("m3u_account_id", "series__uuid"),
            "catalogue_series_read",
        ):
            series_batch.append((str(uuid), str(account)))
            if len(series_batch) == BATCH_SIZE:
                with self.measure("catalogue_sqlite_write", len(series_batch)), self.store.db:
                    self.store.db.executemany(
                        "INSERT OR IGNORE INTO live_series VALUES (?,?)", series_batch
                    )
                series_batch = []
        if series_batch:
            with self.measure("catalogue_sqlite_write", len(series_batch)), self.store.db:
                self.store.db.executemany(
                    "INSERT OR IGNORE INTO live_series VALUES (?,?)", series_batch
                )
        # Cache only this census: bounded memory, no stale data across actions.
        @lru_cache(maxsize=8192)
        def live_show(uuid, account):
            return bool(self.store.db.execute(
                "SELECT 1 FROM live_series WHERE uuid=? AND account=?",
                (uuid, account),
            ).fetchone())

        @lru_cache(maxsize=8192)
        def identity_json(kind, name, year, tmdb, imdb):
            return json.dumps(asdict(identity_from_fields(
                self.plugin, kind, name, year, tmdb, imdb,
            )))

        # Full catalogue deliberately ignores native category eligibility and batches.
        # Project tuples so Django never constructs hundreds of thousands of models.
        batch = []
        for model, attr, kind in (
            (M3UMovieRelation, "movie", "movie"),
            (M3UEpisodeRelation, "episode", "series"),
        ):
            fields = ["m3u_account_id", "stream_id", f"{attr}__uuid"]
            identity_path = "episode__series" if attr == "episode" else "movie"
            fields.extend(
                f"{identity_path}__{name}"
                for name in ("name", "year", "tmdb_id", "imdb_id")
            )
            if attr == "episode":
                fields.extend([
                    "episode__series__uuid", "episode__season_number",
                    "episode__episode_number",
                ])
            for row in self.catalogue_rows(
                model.objects.values_list(*fields), f"catalogue_{attr}_read",
            ):
                account, stream, uuid, name, year, tmdb, imdb = row[:7]
                account = str(account)
                if attr == "episode" and not live_show(str(row[7]), account):
                    continue
                position = row[8:10] if attr == "episode" else (None, None)
                source = (
                    json.dumps([attr, account, str(stream)])
                    if row[0] is not None and stream is not None else ""
                )
                batch.append((
                    str(uuid), str(stream),
                    identity_json(kind, name, year, tmdb, imdb), source, *position,
                ))
                if len(batch) == BATCH_SIZE:
                    self._census_batch(batch)
                    batch = []
        if batch:
            self._census_batch(batch)
        with self.measure("catalogue_index_build"), self.store.db:
            self.store.db.execute("CREATE INDEX temp.catalogue_uuid ON catalogue(uuid,stream)")
        self.progress(f"Checked Dispatcharr catalogue: {self.census_count:,} rows")

    def refresh_tracked_series(self):
        if not self.store.db.execute(
            "SELECT 1 FROM files WHERE kind='series' LIMIT 1"
        ).fetchone():
            self.progress("No tracked shows require M3U episode refresh")
            return False
        from apps.vod.models import M3USeriesRelation
        from apps.vod.tasks import refresh_series_episodes

        self.progress("Checking M3U removals and refreshing tracked shows")
        refreshed = False
        for rel in (
            M3USeriesRelation.objects.select_related("series")
            .only(
                "m3u_account_id",
                "external_series_id",
                "last_episode_refresh",
                "series__name",
                "series__year",
                "series__tmdb_id",
                "series__imdb_id",
            )
            .all()
            .iterator(chunk_size=BATCH_SIZE)
        ):
            identity = identity_for(self.plugin, rel.series, "series")
            owned_show = (
                self.snapshot is not None
                and self.settings.get("media_tv_mode", "show") == "show"
                and self.snapshot.owns(identity)
            )
            if self.tracked_series(identity) and not owned_show:
                with self.measure("provider_episode_refresh", 1):
                    self.refresh_complete(rel, refresh_series_episodes)
                refreshed = True
        return refreshed

    def tracked_series(self, identity):
        # Separate probes use all columns of the existing indices. A combined
        # OR only used their kind prefix, scanning every tracked episode per show.
        for column, value in (("tmdb", identity.tmdb), ("imdb", identity.imdb)):
            if (
                value
                and self.store.db.execute(
                    f"SELECT 1 FROM files WHERE kind='series' AND {column}=? LIMIT 1",
                    (value,),
                ).fetchone()
            ):
                return True
        if not identity.year:
            return False
        return bool(
            self.store.db.execute(
                """SELECT 1 FROM files WHERE kind='series' AND title=? AND year=?
               AND (tmdb='' OR ?='' OR tmdb=?)
               AND (imdb='' OR ?='' OR imdb=?) LIMIT 1""",
                (
                    identity.title,
                    str(identity.year),
                    identity.tmdb,
                    identity.tmdb,
                    identity.imdb,
                    identity.imdb,
                ),
            ).fetchone()
        )

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
        previous = getattr(self, "census_count", 0)
        self.census_count = previous + len(batch)
        if self.census_count // 10000 > previous // 10000:
            self.progress(
                f"Checking Dispatcharr catalogue: {self.census_count:,} rows processed"
            )
        with self.measure("catalogue_sqlite_write", len(batch)), self.store.db:
            self.store.db.executemany(
                "INSERT INTO catalogue VALUES (?,?,?,?,?,?)", batch
            )
            self.store.db.executemany(
                "INSERT OR IGNORE INTO live VALUES (?)",
                [(r[3],) for r in batch if r[3]],
            )

    @staticmethod
    def generated_paths(root, missing_ok=True):
        # DirEntry uses the directory listing's type information. os.walk plus
        # path-based islink checks used multiple network filesystem stats per dir.
        try:
            with os.scandir(root) as entries:
                for entry in entries:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False):
                        yield from Reconciliation.generated_paths(entry.path, missing_ok=False)
                    elif entry.name.endswith(".strm"):
                        yield entry.path
        except FileNotFoundError:
            if not missing_ok:
                raise
            return

    def adopt(self, force=False):
        self.progress("Adopting recognizable generated output")
        host = urlparse(self.settings.get("dispatcharr_url", ""))
        context = json.dumps([host.scheme, host.netloc])
        for root in dict.fromkeys(self.roots):
            root_key = os.path.normcase(os.path.realpath(root))
            if not force and self.store.discovery_complete(root_key, context):
                self.report["discovery_skipped_roots"] += 1
                continue
            # Invalidate before scanning, including forced scans: failures retry.
            self.store.invalidate_discovery(root_key)
            try:
                root_stat = os.stat(root)
            except FileNotFoundError:
                # Do not mark a missing root; discover it if it appears later.
                continue
            if not stat.S_ISDIR(root_stat.st_mode):
                raise ValueError("An output root is not a directory")
            records, incomplete = [], False
            self.report["discovery_scanned_roots"] += 1
            for discovered in self.generated_paths(root, missing_ok=False):
                path = os.path.abspath(discovered)
                if self.store.db.execute(
                    "SELECT 1 FROM files WHERE path=?", (path,)
                ).fetchone():
                    continue
                if not contained(path, self.roots) or os.path.islink(path):
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
                    self.report["adopted"] += 1
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
                except OSError:
                    incomplete = True
                    self.report["preserved"] += 1
                except (UnicodeError, ValueError):
                    self.report["preserved"] += 1
            if records:
                self.store.record_many(records)
            if incomplete:
                raise OSError("Output discovery incomplete; retry pending")
            self.store.mark_discovered(root_key, context)

    def cleanup(self, server, m3u, dry_run=False):
        self.progress(
            "Previewing cleanup" if dry_run else "Reconciling generated files"
        )
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
                if not dry_run and not os.path.lexists(row["path"]):
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
