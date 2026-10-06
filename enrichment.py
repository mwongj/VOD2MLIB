"""Missing-only provider enrichment, durable replay and verified native population.

All requests/imports run on the isolated action thread, before output workers.
Successful responses never expire; failed native writes replay without networking.
"""
import copy
import hashlib
import json
import importlib
from email.utils import parsedate_to_datetime
import time
from contextlib import ExitStack
from itertools import islice
from types import SimpleNamespace
from unittest.mock import patch

try:
    from .inventory import BATCH_SIZE, LOOKUP_BATCH_SIZE
    from .metadata_filters import configuration, score, year, genre_names, evaluate_metadata
except ImportError:
    from inventory import BATCH_SIZE, LOOKUP_BATCH_SIZE
    from metadata_filters import configuration, score, year, genre_names, evaluate_metadata

MAX_RESPONSE_BYTES = 16 * 1024 * 1024
KINDS = {'generate_movies': ('movie',), 'generate_series': ('series',),
         'rescan_all': ('movie', 'series'), 'scan_all_vods': ('movie', 'series'),
         'preview_cleanup': ('movie', 'series'), 'selective_cleanup': ('movie', 'series')}


def source_key(kind, account, external):
    return json.dumps([kind, str(account), str(external)])


def missing_fields(rules, obj):
    return tuple(field for field, enabled, valid in (
        ('rating', rules.minimum_score is not None, score(getattr(obj, 'rating', None)) is not None),
        ('year', rules.earliest_year is not None or rules.latest_year is not None,
         year(getattr(obj, 'year', None)) is not None),
        ('genre', bool(rules.include or rules.exclude), bool(genre_names(getattr(obj, 'genre', None)))),
        ) if enabled and not valid)


def normalize(payload, kind):
    if not isinstance(payload, dict) or not payload or not any(
            key in payload for key in ('info', 'movie_data', 'episodes')):
        raise ValueError('Invalid provider details response')
    result = copy.deepcopy(payload)
    for key in ('info', 'movie_data'):
        value = result.get(key, {})
        if isinstance(value, list):
            value = value[0] if value else {}
        if not isinstance(value, dict):
            raise ValueError('Invalid provider metadata')
        result[key] = value
    if isinstance(result['info'].get('genre'), list):
        result['info']['genre'] = ', '.join(str(v) for v in result['info']['genre'] if v)
    if kind == 'series':
        raw = result.get('episodes', {})
        if raw is None: raw = {}
        if isinstance(raw, list) and all(isinstance(items, list) for items in raw):
            raw = {str(season): items for season, items in enumerate(raw)}
        if isinstance(raw, list):
            grouped = {}
            for ep in raw:
                if not isinstance(ep, dict): raise ValueError('Invalid episode')
                season = ep.get('season', ep.get('season_number'))
                grouped.setdefault(str(season), []).append(ep)
            raw = grouped
        if not isinstance(raw, dict): raise ValueError('Invalid episode list')
        episodes, streams = {}, set()
        for season, items in raw.items():
            if isinstance(items, dict): items = list(items.values())
            if not isinstance(items, list): raise ValueError('Invalid season')
            season = int(season)
            if season < 0: raise ValueError('Invalid season')
            episodes[str(season)] = []
            for ep in items:
                if not isinstance(ep, dict) or ep.get('id') in (None, ''):
                    raise ValueError('Missing episode source')
                number = int(ep.get('episode_num', 0))
                stream = str(ep['id'])
                if number < 0 or stream in streams: raise ValueError('Ambiguous episode source')
                streams.add(stream)
                ep['episode_num'] = number
                episodes[str(season)].append(ep)
        result['episodes'] = episodes
    return result


def metadata(payload):
    info = payload['info']
    release_year = year(info.get('year'))
    for key in ('releasedate', 'release_date', 'releaseDate', 'first_air_date', 'air_date'):
        if release_year is None:
            release_year = year(str(info.get(key, '')).split('-')[0])
    genre = info.get('genre')
    if isinstance(genre, list): genre = ', '.join(str(v) for v in genre if v)
    return {'rating': score(info.get('rating')), 'year': release_year,
            'genre': genre if isinstance(genre, str) and genre_names(genre) else None}


def source_episodes(rel):
    """Account AND provider-show membership; ambiguous legacy NULLs are not proof."""
    from apps.vod.models import M3UEpisodeRelation, M3USeriesRelation
    parents = M3USeriesRelation.objects.filter(m3u_account_id=rel.m3u_account_id,
                                               series_id=rel.series.id)
    first = None
    unique_parent = True
    for (external,) in parents.values_list('external_series_id').iterator(chunk_size=BATCH_SIZE):
        if first is None: first = external
        elif external != first:
            unique_parent = False
            break
    unique_parent = unique_parent and first is not None
    canonical = next(parents.filter(external_series_id=rel.external_series_id).order_by('pk').iterator(chunk_size=BATCH_SIZE), None)
    parent_id = canonical.id if canonical else rel.id
    rows = M3UEpisodeRelation.objects.filter(m3u_account_id=rel.m3u_account_id,
                                             episode__series=rel.series).select_related('episode', 'series_relation').order_by(
                                                 'episode__season_number', 'episode__episode_number', 'id')
    for episode in rows.iterator(chunk_size=BATCH_SIZE):
        parent = getattr(episode, 'series_relation_id', None)
        native_parent = getattr(episode, 'series_relation', None)
        same_source = native_parent is not None and str(native_parent.external_series_id) == str(rel.external_series_id)
        if parent in (rel.id, parent_id) or same_source or (parent is None and unique_parent): yield episode


class NativeAdapter:
    def __init__(self, preparation):
        self.preparation = preparation
        self.last_request = 0.0

    def fetch(self, rel, kind):
        from core.xtream_codes import Client
        account = rel.m3u_account
        ua = (account.get_user_agent_string() if hasattr(account, 'get_user_agent_string')
              else account.get_user_agent().user_agent)
        # Instrument the client's requests session, including login/authentication.
        # Both supported versions use requests.Session; no network wait holds a DB lock.
        import requests
        original = requests.sessions.Session.request
        def paced(session, *args, **kwargs):
            self.preparation.check_cancelled()
            delay = max(0, self.last_request + 0.5 - time.monotonic())
            if self.preparation.rec.cancelled.wait(delay): raise RuntimeError('Action cancelled')
            self.preparation.count('requests')
            try:
                from urllib3.util.retry import Retry
                for adapter in session.adapters.values(): adapter.max_retries = Retry(total=0)
                kwargs['stream'] = True
                kwargs['allow_redirects'] = False
                response = original(session, *args, **kwargs)
                try:
                    contents = bytearray()
                    for chunk in response.iter_content(65536):
                        self.preparation.check_cancelled()
                        contents.extend(chunk)
                        if len(contents) > MAX_RESPONSE_BYTES:
                            raise ValueError('Provider response too large')
                    response._content = bytes(contents)
                    response._content_consumed = True
                finally:
                    response.close()
                if 300 <= response.status_code < 400:
                    raise RuntimeError('Provider redirect refused')
                if response.status_code == 429 or response.status_code >= 500:
                    error = RuntimeError('Provider HTTP failure')
                    error.retry_after = response.headers.get('Retry-After')
                    raise error
                return response
            finally:
                self.last_request = time.monotonic()
        with patch.object(requests.sessions.Session, 'request', paced), patch.object(
                importlib.import_module('core.xtream_codes').logger, 'disabled', True):
            with Client(server_url=account.server_url, username=account.username,
                        password=account.password, user_agent=ua) as client:
                return (client.get_vod_info(rel.stream_id) if kind == 'movie'
                        else client.get_series_info(rel.external_series_id))

    def reload(self, rel, kind):
        from apps.vod.models import M3UMovieRelation, M3USeriesRelation
        model = M3UMovieRelation if kind == 'movie' else M3USeriesRelation
        external = 'stream_id' if kind == 'movie' else 'external_series_id'
        rows = model.objects.filter(m3u_account_id=rel.m3u_account_id,
                    **{external: getattr(rel, external)}).select_related(kind, 'm3u_account').order_by('pk')
        first = None
        for row in rows.iterator(chunk_size=BATCH_SIZE):
            if first is None: first = row
            elif getattr(row, kind).id != getattr(first, kind).id:
                raise ValueError('Ambiguous native source')
        if first is None: raise ValueError('Missing native source')
        return first

    def populate(self, rel, kind, payload):
        tasks = importlib.import_module("apps.vod.tasks")
        xtream = importlib.import_module("core.xtream_codes")
        from django.db import transaction
        errors = []
        class Replay:
            def __init__(self, *args, **kwargs): pass
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def get_vod_info(self, *args): return copy.deepcopy(payload)
            def get_series_info(self, *args): return copy.deepcopy(payload)
        rel = self.reload(rel, kind)
        freshness = 'last_advanced_refresh' if kind == 'movie' else 'last_episode_refresh'
        before = getattr(self.reload(rel, kind), freshness, None)
        with transaction.atomic(), ExitStack() as stack:
            if kind == 'series':
                self.check_existing_episodes(rel, payload)
            stack.enter_context(patch.object(xtream, 'Client', Replay))
            stack.enter_context(patch.object(tasks, 'XtreamCodesClient', Replay))
            stack.enter_context(patch.object(tasks.logger, 'disabled', True))
            # Native helpers swallow failures; capture their error reports too.
            stack.enter_context(patch.object(tasks.logger, 'error', lambda *a, **k: errors.append(True)))
            if kind == 'movie':
                tasks.refresh_movie_advanced_data(rel.id, force_refresh=True)
            else:
                tasks.refresh_series_episodes(rel.m3u_account, rel.series, rel.external_series_id)
            if errors: raise ValueError('Native population reported a failure')
            current = self.reload(rel, kind)
            if getattr(current, freshness, None) is None or getattr(current, freshness) == before:
                raise ValueError('Native population did not persist completion')
            obj = getattr(current, kind)
            values = metadata(payload)
            changed = []
            required = (missing_fields(self.preparation.rules[kind], SimpleNamespace(rating=None, year=None, genre=None))
                        if self.preparation else tuple(values))
            # Narrow missing-value correction; identity merging/import stay native.
            for field, value in values.items():
                if field not in required: continue
                valid = (score(obj.rating) is not None if field == 'rating' else
                         year(obj.year) is not None if field == 'year' else bool(genre_names(obj.genre)))
                if value is not None and not valid:
                    setattr(obj, field, value)
                    changed.append(field)
            if changed: obj.save(update_fields=changed)
            obj.refresh_from_db()
            for field, expected in values.items():
                if expected is None or field not in required: continue
                actual = getattr(obj, field)
                valid = (score(actual) is not None if field == 'rating' else
                         year(actual) is not None if field == 'year' else bool(genre_names(actual)))
                if not valid: raise ValueError('Native metadata persistence unverified')
            if kind == 'series': self.verify_episodes(current, payload)
        # Read again after commit; completion is written in SQLite only afterwards.
        committed = self.reload(current, kind)
        obj = getattr(committed, kind)
        values = metadata(payload)
        for field in missing_fields(self.preparation.rules[kind], obj) if self.preparation else ():
            if values[field] is not None: raise ValueError('Post-commit metadata readback failed')
        if kind == 'series': self.verify_episodes(committed, payload)
        return committed

    def check_existing_episodes(self, rel, payload):
        from apps.vod.models import M3UEpisodeRelation
        streams = [str(ep['id']) for items in payload['episodes'].values() for ep in items]
        for offset in range(0, len(streams), LOOKUP_BATCH_SIZE):
            for series, parent in M3UEpisodeRelation.objects.filter(
                    m3u_account_id=rel.m3u_account_id, stream_id__in=streams[offset:offset + LOOKUP_BATCH_SIZE]
                    ).values_list('episode__series_id', 'series_relation_id'):
                if series != rel.series.id or parent not in (None, rel.id):
                    raise ValueError('Provider episode belongs to another source')

    def verify_episodes(self, rel, payload):
        from apps.vod.models import M3UEpisodeRelation
        expected = [(str(ep['id']), int(season), ep['episode_num'], ep.get('title', 'Unknown Episode'))
                    for season, items in payload['episodes'].items() for ep in items]
        for offset in range(0, len(expected), LOOKUP_BATCH_SIZE):
            batch = expected[offset:offset + LOOKUP_BATCH_SIZE]
            rows = M3UEpisodeRelation.objects.filter(m3u_account_id=rel.m3u_account_id,
                        stream_id__in=[r[0] for r in batch]).values_list(
                        'stream_id', 'episode__series_id', 'episode__season_number',
                        'episode__episode_number', 'series_relation_id', 'episode__name')
            found = {}
            for stream, series, season, number, parent, name in rows:
                if str(stream) in found or series != rel.series_id or parent != rel.id:
                    raise ValueError('Incorrect episode source mapping')
                found[str(stream)] = (season, number, name)
            if found != {stream: (season, number, name) for stream, season, number, name in batch}:
                raise ValueError('Incomplete episode import')


class Preparation:
    def __init__(self, rec):
        self.rec, self.db = rec, rec.store.db
        self.rules = configuration(rec.settings)
        self.adapter = NativeAdapter(self)
        self.db.execute('CREATE TEMP TABLE prepared(source TEXT PRIMARY KEY, passed INTEGER)')
        self.db.execute('CREATE TEMP TABLE attempted(source TEXT PRIMARY KEY)')
        rec.report['enrichment'] = dict(requests=0, cache_reuse=0, verified_omissions=0,
                                      failed=0, deferred=0, episode_imports=0, protected=0)

    def count(self, key): self.rec.report['enrichment'][key] += 1

    def check_cancelled(self):
        if self.rec.cancelled.is_set(): raise RuntimeError('Action cancelled')

    def key(self, rel, kind):
        return source_key(kind, rel.m3u_account_id,
                          rel.stream_id if kind == 'movie' else rel.external_series_id)

    def ensure(self, rel, kind, episodes=False):
        self.check_cancelled()
        key = self.key(rel, kind)
        obj = getattr(rel, kind)
        missing = missing_fields(self.rules[kind], obj)
        if not missing and not episodes: return rel
        account = rel.m3u_account
        identity = hashlib.sha256(json.dumps([getattr(account, 'server_url', ''),
                         getattr(account, 'username', '')]).encode()).hexdigest()
        evidence = self.db.execute('SELECT * FROM fetch_evidence WHERE source=?', (key,)).fetchone()
        if evidence and evidence['identity'] != identity:
            with self.db: self.db.execute('DELETE FROM fetch_evidence WHERE source=?', (key,))
            evidence = None
        if self.db.execute('SELECT 1 FROM attempted WHERE source=?', (key,)).fetchone():
            if evidence and evidence['persistence'] == 'verified': return rel
            return None
        with self.db: self.db.execute('INSERT INTO attempted VALUES (?)', (key,))
        failures = evidence['failures'] if evidence else 0
        if evidence and evidence['retry_at'] > time.time():
            self.count('deferred'); return None
        response = evidence['response'] if evidence else None
        try:
            if response:
                payload = normalize(json.loads(response), kind)
                self.count('cache_reuse')
                if evidence['persistence'] == 'verified':
                    available = metadata(payload)
                    # Verified omissions are final; flags/timestamps never trigger a request.
                    if all(available[f] is None for f in missing) and (not episodes or evidence['episodes'] == 'empty'):
                        self.count('verified_omissions'); return rel
            else:
                payload = normalize(self.adapter.fetch(rel, kind), kind)
                serialized = json.dumps(payload, ensure_ascii=False)
                if len(serialized.encode()) > MAX_RESPONSE_BYTES: raise ValueError('Provider response too large')
                response = serialized
                with self.db:
                    self.db.execute('INSERT OR REPLACE INTO fetch_evidence(source,identity,response,response_state,persistence,episodes,failures,retry_at) VALUES (?,?,?,?,?,?,?,?)',
                        (key, identity, response, 'success', 'pending', 'unverified', failures, 0))
            self.check_cancelled()
            current = self.adapter.populate(rel, kind, payload)
            state = ('populated' if any(payload['episodes'].values()) else 'empty') if kind == 'series' else 'unverified'
            with self.db:
                self.db.execute('UPDATE fetch_evidence SET persistence=?, episodes=?, fields=?, failures=0, retry_at=0 WHERE source=?',
                                ('verified', state, json.dumps({field: 'available' if value is not None else 'omitted'
                                                              for field, value in metadata(payload).items()}), key))
            if kind == 'series': self.count('episode_imports')
            if missing_fields(self.rules[kind], getattr(current, kind)): self.count('verified_omissions')
            return current
        except Exception as error:
            self.check_cancelled()
            if type(error).__name__ in ('OperationalError', 'DatabaseError', 'InterfaceError'):
                raise
            failures += 1
            delay = min(86400, 900 * 2 ** min(failures - 1, 7))
            try: delay = max(delay, float(getattr(error, 'retry_after', 0) or 0))
            except (ValueError, TypeError):
                try:
                    delay = max(delay, parsedate_to_datetime(error.retry_after).timestamp() - time.time())
                except (ValueError, TypeError, AttributeError, OverflowError): pass
            with self.db:
                self.db.execute('INSERT OR REPLACE INTO fetch_evidence(source,identity,response,response_state,persistence,episodes,failures,retry_at) VALUES (?,?,?,?,?,?,?,?)',
                    (key, identity, response, 'success' if response else 'failed', 'failed', 'unverified',
                     failures, time.time() + delay))
            self.count('failed'); self.count('deferred')
            self.rec.logger.warning('Provider enrichment deferred (%s); output protected', type(error).__name__)
            return None

    def prepare(self, action):
        from apps.vod.models import M3UMovieRelation, M3USeriesRelation
        try:
            from .plugin import VODType
        except ImportError:
            from plugin import VODType
        for kind in KINDS.get(action, ()):
            model = M3UMovieRelation if kind == 'movie' else M3USeriesRelation
            query = self.rec.plugin._eligible_vod_relations(model.objects.all(),
                          VODType.MOVIE if kind == 'movie' else VODType.SERIES)
            external = 'stream_id' if kind == 'movie' else 'external_series_id'
            # Stream only fields needed for work selection; hydrate missing sources only.
            rows = query.values_list('id', 'm3u_account_id', external, f'{kind}_id',
                        f'{kind}__rating', f'{kind}__year', f'{kind}__genre', f'{kind}__name').iterator(chunk_size=BATCH_SIZE)
            self.db.execute('CREATE TEMP TABLE IF NOT EXISTS preparation_work('
                'pk INTEGER, account TEXT, external TEXT, native_id TEXT, rating TEXT, year TEXT, '
                'genre TEXT, title TEXT, ambiguous INTEGER DEFAULT 0, PRIMARY KEY(account,external))')
            self.db.execute('DELETE FROM preparation_work')
            while True:
                batch = list(islice(rows, BATCH_SIZE))
                if not batch: break
                self.check_cancelled()
                with self.db:
                    self.db.executemany('INSERT INTO preparation_work(pk,account,external,native_id,rating,year,genre,title) '
                        'VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(account,external) DO UPDATE SET '
                        'ambiguous=MAX(ambiguous, native_id != excluded.native_id)', batch)
            cursor = self.db.execute('SELECT * FROM preparation_work')
            while True:
                batch = cursor.fetchmany(LOOKUP_BATCH_SIZE)
                if not batch: break
                for row in batch:
                    self.check_cancelled()
                    account, provider = row['account'], row['external']
                    key = source_key(kind, account, provider)
                    obj = SimpleNamespace(rating=row['rating'], year=row['year'], genre=row['genre'], name=row['title'])
                    passed = None
                    if not row['ambiguous']:
                        if missing_fields(self.rules[kind], obj):
                            relations = query.filter(m3u_account_id=int(account), **{external: provider}).select_related(kind, 'm3u_account')
                            rel = next(relations.iterator(chunk_size=BATCH_SIZE), None)
                            if rel is not None:
                                current = self.ensure(rel, kind)
                                if current is not None:
                                    obj = getattr(current, kind)
                                    passed = int(self.rules[kind].evaluate(obj.rating, obj.year, obj.genre, obj.name)[0])
                            else: self.count('deferred')
                        else:
                            passed = int(self.rules[kind].evaluate(obj.rating, obj.year, obj.genre, obj.name)[0])
                    if row['ambiguous']: self.count('deferred')
                    with self.db:
                        self.db.execute('INSERT OR REPLACE INTO prepared VALUES (?,?)', (key, passed))
            # Re-read every eligible source after ALL imports. Native merging can change
            # shared models, source relations, identity fields or category membership.
            self.db.execute('CREATE TEMP TABLE IF NOT EXISTS final_prepared(source TEXT PRIMARY KEY, passed INTEGER)')
            self.db.execute('DELETE FROM final_prepared')
            finals = query.values_list('m3u_account_id', external, f'{kind}__rating',
                    f'{kind}__year', f'{kind}__genre', f'{kind}__name').iterator(chunk_size=BATCH_SIZE)
            while True:
                batch = list(islice(finals, BATCH_SIZE))
                if not batch: break
                self.check_cancelled()
                decisions = []
                for account, provider, rating, release_year, genre, title in batch:
                    key = source_key(kind, account, provider)
                    previous = self.db.execute('SELECT passed FROM prepared WHERE source=?', (key,)).fetchone()
                    passed = (int(self.rules[kind].evaluate(rating, release_year, genre, title)[0])
                              if previous and previous[0] is not None else None)
                    obj = SimpleNamespace(rating=rating, year=release_year, genre=genre)
                    unknown = missing_fields(self.rules[kind], obj)
                    if passed is not None and unknown:
                        evidence = self.db.execute('SELECT * FROM fetch_evidence WHERE source=?', (key,)).fetchone()
                        if not evidence or evidence['persistence'] != 'verified':
                            passed = None
                        else:
                            available = metadata(json.loads(evidence['response']))
                            if any(available[field] is not None for field in unknown): passed = None
                    decisions.append((key, passed))
                with self.db:
                    self.db.executemany('INSERT INTO final_prepared VALUES (?,?) ON CONFLICT(source) '
                        'DO UPDATE SET passed=CASE WHEN passed IS NULL OR excluded.passed IS NULL THEN NULL '
                        'ELSE MAX(passed,excluded.passed) END', decisions)
            with self.db:
                self.db.execute('DELETE FROM prepared WHERE source LIKE ?', (f'["{kind}",%',))
                self.db.execute('INSERT INTO prepared SELECT * FROM final_prepared')

    def counts(self, query, kind):
        self.db.execute('CREATE TEMP TABLE IF NOT EXISTS snapshot_counts(native_id TEXT, passed INTEGER, reasons INTEGER, unknown INTEGER)')
        self.db.execute('DELETE FROM snapshot_counts')
        external = 'stream_id' if kind == 'movie' else 'external_series_id'
        fields = (f'{kind}_id', 'm3u_account_id', external, f'{kind}__rating',
                  f'{kind}__year', f'{kind}__genre', f'{kind}__name')
        for row in query.values_list(*fields).iterator(chunk_size=BATCH_SIZE):
            self.check_cancelled()
            decision = self.verified_decision(kind, row[1], row[2], row[3:])
            passed, reasons, unknown = evaluate_metadata(self.rules[kind], kind, row[3:])
            mask = sum(1 << ('score', 'year', 'genre', 'title').index(r) for r in reasons)
            self.db.execute('INSERT INTO snapshot_counts VALUES (?,?,?,?)',
                            (str(row[0]), decision, mask, int(bool(unknown))))
        result = dict(eligible=0, passing=0, rejected_score=0, rejected_year=0,
                      rejected_genre=0, rejected_title=0, retained_unknown=0, unresolved=0)
        for _, passed, unresolved, unknown, mask in self.db.execute(
                'SELECT native_id, MAX(passed), SUM(passed IS NULL), MAX(unknown), MAX(reasons) '
                'FROM snapshot_counts GROUP BY native_id'):
            result['eligible'] += 1
            if passed == 1:
                result['passing'] += 1
                result['retained_unknown'] += unknown
            elif unresolved: result['unresolved'] += 1
            else:
                for bit, reason in enumerate(('score', 'year', 'genre', 'title')):
                    result[f'rejected_{reason}'] += bool(mask & (1 << bit))
        return result

    def decision(self, kind, account, external):
        row = self.db.execute('SELECT passed FROM prepared WHERE source=?',
                             (source_key(kind, account, external),)).fetchone()
        return row[0] if row else None

    def verified_decision(self, kind, account, external, values):
        if self.decision(kind, account, external) is None: return None
        rating, release_year, genre, title = values
        obj = SimpleNamespace(rating=rating, year=release_year, genre=genre)
        unknown = missing_fields(self.rules[kind], obj)
        if unknown:
            evidence = self.db.execute('SELECT * FROM fetch_evidence WHERE source=?',
                                      (source_key(kind, account, external),)).fetchone()
            if not evidence or evidence['persistence'] != 'verified': return None
            available = metadata(json.loads(evidence['response']))
            if any(available[f] is not None for f in unknown): return None
        return int(self.rules[kind].evaluate(rating, release_year, genre, title)[0])

    def allows(self, rel, kind):
        return self.decision(kind, rel.m3u_account_id,
                    rel.stream_id if kind == 'movie' else rel.external_series_id) == 1

    def episodes(self, rel):
        from apps.vod.models import M3UEpisodeRelation
        rows = source_episodes(rel)
        if next(iter(rows), None) is not None: return rel
        key = self.key(rel, 'series')
        evidence = self.db.execute('SELECT * FROM fetch_evidence WHERE source=?', (key,)).fetchone()
        current = self.ensure(rel, 'series', episodes=True)
        evidence = self.db.execute('SELECT * FROM fetch_evidence WHERE source=?', (key,)).fetchone()
        if evidence and evidence['persistence'] == 'verified' and evidence['episodes'] == 'empty':
            self.count('verified_omissions'); return None
        return current
