"""Preparation regressions with provider responses and output bytes held fixed."""
import json
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from enrichment import NativeAdapter, Preparation, metadata, normalize, missing_fields
from metadata_filters import configuration
from tests.test_reconciliation import library, media, relation, Query


@pytest.fixture
def provider(library, monkeypatch):
    state = NS(requests=[], imports=[], payload={'info': {'year': 2021, 'rating': '8', 'genre': 'Drama'}}, fail=False)
    def fetch(adapter, rel, kind):
        adapter.preparation.count('requests')
        state.requests.append((kind, rel.m3u_account_id, rel.stream_id if kind == 'movie' else rel.external_series_id))
        if state.fail: raise ValueError('failure with private provider details')
        return state.payload
    def populate(adapter, rel, kind, payload):
        state.imports.append((kind, rel.id))
        for field, value in metadata(payload).items():
            if value is not None: setattr(getattr(rel, kind), field, str(value) if field == 'rating' else value)
        if kind == 'series':
            for season, items in payload['episodes'].items():
                for item in items:
                    ep = media(int(item['id']), series=rel.series, season_number=int(season), episode_number=item['episode_num'])
                    child = relation(ep, 'episode', stream=str(item['id']))
                    child.m3u_account_id = rel.m3u_account_id
                    child.series_relation_id = rel.id
                    library.rows['episodes'].append(child)
        return rel
    monkeypatch.setattr(NativeAdapter, 'fetch', fetch)
    monkeypatch.setattr(NativeAdapter, 'populate', populate)
    return state


@pytest.mark.parametrize('reject', [dict(movie_earliest_year='2025'), dict(movie_minimum_score='9'),
                                   dict(movie_genre_exclude='Drama'), dict(movie_title_exclude='Title')])
def test_enrichment_precedes_every_rejection(library, provider, reject):
    obj = media(1, year=None, rating='0', genre='')
    library.rows['movies'].append(relation(obj))
    result = library.run(**{**dict(movie_earliest_year='2000', movie_minimum_score='7', movie_genre_include='Drama'), **reject})
    assert result['status'] == 'ok'
    assert len(provider.requests) == 1 and len(provider.imports) == 1
    assert result['created_strm'] == 0 and obj.year == 2021


def test_complete_and_disabled_fields_do_not_fetch(library, provider):
    library.rows['movies'].append(relation(media(1, year=2021, rating='8', genre='Drama')))
    library.run(movie_minimum_score='7', movie_genre_include='Drama')
    library.rows['movies'].append(relation(media(2, year=None, rating='0', genre='')))
    library.run()
    assert provider.requests == []


@pytest.mark.parametrize('policy,created', [('keep', 1), ('reject', 0)])
def test_verified_omissions_follow_policy_without_repeat(library, provider, policy, created):
    provider.payload = {'info': {}}
    rel = relation(media(1, year=None))
    library.rows['movies'].append(rel)
    for _ in range(2):
        result = library.run(movie_earliest_year='2000', movie_missing_metadata=policy)
    assert len(provider.requests) == 1 and len(provider.imports) == 1
    assert len(list(Path(library.settings['root_folder']).rglob('*.strm'))) == created
    assert result['reconciliation']['enrichment']['verified_omissions'] == 1


def test_failure_protects_existing_output_and_defers_title_rejection(library, provider):
    obj = media(1)
    library.rows['movies'].append(relation(obj))
    library.run()
    path = next(Path(library.settings['root_folder']).rglob('*.strm'))
    before = path.read_bytes(), path.stat().st_mtime_ns
    obj.year = None
    provider.fail = True
    result = library.run(movie_earliest_year='2000', movie_missing_metadata='reject', movie_title_exclude='Title')
    assert result['reconciliation']['filter_deleted'] == 0
    assert result['reconciliation']['enrichment']['failed'] == 1
    assert before == (path.read_bytes(), path.stat().st_mtime_ns)
    library.run(movie_earliest_year='2000', movie_missing_metadata='reject')
    assert len(provider.requests) == 1  # cooldown


def test_duplicate_categories_and_cached_restoration_survive_rebuild_rescan(library, provider):
    obj = media(1, year=None)
    first, duplicate = relation(obj), relation(obj)
    duplicate.category.name = 'Other'
    library.rows['movies'].extend([first, duplicate])
    library.run(movie_earliest_year='2000')
    assert len(provider.requests) == 1
    library.run('rebuild_inventory')
    obj.year = None
    first.custom_properties = {}
    library.run('rescan_all', movie_earliest_year='2000')
    assert len(provider.requests) == 1 and obj.year == 2021
    assert len(provider.imports) == 2


def test_cached_response_survives_population_failure(library, provider, monkeypatch):
    library.rows['movies'].append(relation(media(1, year=None)))
    original = NativeAdapter.populate
    monkeypatch.setattr(NativeAdapter, 'populate', lambda *a: (_ for _ in ()).throw(ValueError('failed save')))
    library.run(movie_earliest_year='2000')
    from inventory import InventoryStore
    store = InventoryStore(library.tmp / 'state')
    row = store.db.execute('SELECT * FROM fetch_evidence').fetchone()
    assert row['response_state'] == 'success' and row['persistence'] == 'failed'
    with store.db: store.db.execute('UPDATE fetch_evidence SET retry_at=0')
    store.close()
    monkeypatch.setattr(NativeAdapter, 'populate', original)
    result = library.run(movie_earliest_year='2000')
    assert result['created_strm'] == 1 and len(provider.requests) == 1


def test_series_enrichment_can_import_episodes_before_title_rejection(library, provider):
    show = relation(media(10, genre=''), 'series')
    library.rows['series'].append(show)
    provider.payload = {'info': {'genre': 'Drama'}, 'episodes': {'1': [{'id': '101', 'episode_num': 1}]}}
    result = library.run('generate_series', series_genre_include='Drama', series_title_exclude='Title')
    assert len(provider.requests) == 1 and len(library.rows['episodes']) == 1
    assert result['episodes_created'] == 0


def test_selected_missing_episode_fetch_obeys_batch_and_empty_has_no_ttl(library, provider):
    library.rows['series'].extend([relation(media(10), 'series'), relation(media(20), 'series')])
    provider.payload = {'info': {}, 'episodes': {}}
    library.run('generate_series', series_batch_size='1', refresh_existing=True)
    assert len(provider.requests) == 1
    library.run('generate_series', series_batch_size='1', refresh_existing=True)
    assert len(provider.requests) == 1 and len(provider.imports) == 1
    library.run('rescan_all', series_batch_size='all')
    assert len(provider.requests) == 2


def test_snapshot_has_prepared_counts_and_no_output(library, provider):
    rel = relation(media(1, year=None))
    library.rows['movies'].append(rel)
    result = library.run('scan_all_vods', movie_earliest_year='2000')
    assert result['reconciliation']['enrichment']['requests'] == 1
    assert not Path(library.settings['root_folder']).exists()
    assert result['reconciliation']['adopted'] == result['reconciliation']['deleted'] == 0


def test_preview_can_enrich_without_changing_output(library, provider):
    obj = media(1)
    library.rows['movies'].append(relation(obj))
    library.run()
    root = Path(library.settings['root_folder'])
    before = {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob('*') if p.is_file()}
    obj.year = None
    provider.payload = {'info': {'year': 1914}}
    result = library.run('preview_cleanup', movie_earliest_year='2000')
    assert result['reconciliation']['filter_candidates'] == 1
    assert before == {str(p): (p.read_bytes(), p.stat().st_mtime_ns) for p in root.rglob('*') if p.is_file()}


def test_episode_titles_reuse_inventory_filename(library, provider):
    show = media(10)
    library.rows['series'].append(relation(show, 'series'))
    ep = media(101, series=show, season_number=1, episode_number=1)
    library.rows['episodes'].append(relation(ep, 'episode'))
    library.run('generate_series', refresh_existing=True)
    path = next(Path(library.settings['series_root_folder']).rglob('*.strm'))
    ep.name = 'Renamed episode'
    library.run('generate_series', refresh_existing=True)
    assert list(Path(library.settings['series_root_folder']).rglob('*.strm')) == [path]


@pytest.mark.parametrize('episodes', [{'1': [{'id': '1', 'episode_num': '2'}]},
                                      {'1': {'1': {'id': '1', 'episode_num': 2}}},
                                      [{'id': '1', 'season': 1, 'episode_num': 2}]])
def test_payload_shapes_and_date_aliases(episodes):
    payload = normalize({'info': [{'rating': '0', 'releaseDate': '2021-02-03'}], 'episodes': episodes}, 'series')
    assert metadata(payload) == {'rating': None, 'year': 2021, 'genre': None}
    assert payload['episodes']['1'][0]['episode_num'] == 2


@pytest.mark.parametrize('payload', [None, {}, {'error': 'bad'}, {'info': 'bad'},
                                      {'info': {}, 'episodes': {'1': [{'title': 'missing id'}]}}])
def test_invalid_responses_never_establish_omissions(payload):
    with pytest.raises((ValueError, TypeError)): normalize(payload, 'series')


def test_account_episode_sources_remain_independent(library, provider):
    show = media(10)
    first, second = relation(show, 'series'), relation(show, 'series')
    second.external_series_id = 'other-provider-show'
    library.rows['series'].extend([first, second])
    # Ambiguous legacy episodes from this account cannot satisfy both source lists.
    library.rows['episodes'].append(relation(media(100, series=show, season_number=1, episode_number=1), 'episode'))
    provider.payload = {'info': {}, 'episodes': {}}
    library.run('generate_series', refresh_existing=True, dedupe_series_across_categories=False)
    assert len(provider.requests) == 2


def test_cached_identity_change_requires_new_response(library, provider):
    obj = media(1, year=None)
    rel = relation(obj)
    library.rows['movies'].append(rel)
    library.run(movie_earliest_year='2000')
    obj.year = None
    rel.m3u_account.server_url = 'http://different-provider.test'
    library.run(movie_earliest_year='2000')
    assert len(provider.requests) == 2


def test_crash_retains_response_without_false_completion(library, provider, monkeypatch):
    library.rows['movies'].append(relation(media(1, year=None)))
    monkeypatch.setattr(NativeAdapter, 'populate', lambda *a: (_ for _ in ()).throw(SystemExit('interrupted')))
    with pytest.raises(SystemExit): library.run(movie_earliest_year='2000')
    from inventory import InventoryStore
    store = InventoryStore(library.tmp / 'state')
    evidence = store.db.execute('SELECT * FROM fetch_evidence').fetchone()
    assert evidence['response_state'] == 'success' and evidence['persistence'] == 'pending'
    assert not store.db.execute('SELECT * FROM generation_entries').fetchone()
    store.close()


def test_cancellation_after_fetch_caches_before_import(library, provider, monkeypatch):
    library.rows['movies'].append(relation(media(1, year=None)))
    def fetch(adapter, *args):
        adapter.preparation.rec.cancelled.set()
        return {'info': {'year': 2021}}
    monkeypatch.setattr(NativeAdapter, 'fetch', fetch)
    result = library.run(movie_earliest_year='2000')
    assert result['status'] == 'error' and not provider.imports
    from inventory import InventoryStore
    store = InventoryStore(library.tmp / 'state')
    evidence = store.db.execute('SELECT * FROM fetch_evidence').fetchone()
    assert evidence['response'] and evidence['persistence'] == 'pending'
    store.close()


def test_requests_include_authentication_and_are_paced(monkeypatch):
    import logging
    import sys
    import requests
    waits, counts = [], []
    event = NS(is_set=lambda: False, wait=lambda delay: waits.append(delay) or False)
    preparation = NS(rec=NS(cancelled=event), count=counts.append, check_cancelled=lambda: None)
    def request(session, *args, **kwargs):
        assert kwargs['stream'] and not kwargs['allow_redirects']
        assert all(adapter.max_retries.total == 0 for adapter in session.adapters.values())
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"info": {"year": 2021}}'
        response._content_consumed = True
        return response
    monkeypatch.setattr(requests.sessions.Session, 'request', request)
    class Client:
        def __init__(self, **kwargs): self.session = requests.Session()
        def __enter__(self):
            self.session.get('http://provider.test/auth')
            return self
        def __exit__(self, *args): self.session.close()
        def get_vod_info(self, *args): return self.session.get('http://provider.test/details').json()
    monkeypatch.setitem(sys.modules, 'core.xtream_codes', NS(Client=Client, logger=logging.getLogger('paced-test')))
    clock = iter([100, 100.05, 100.1, 100.2])
    monkeypatch.setattr('enrichment.time.monotonic', lambda: next(clock))
    rel = relation(media(1))
    rel.m3u_account = NS(server_url='http://provider.test', username='private', password='private', get_user_agent_string=lambda: 'agent')
    assert NativeAdapter(preparation).fetch(rel, 'movie')['info']['year'] == 2021
    assert counts == ['requests', 'requests']
    assert waits == pytest.approx([0, 0.45])


def test_retry_after_extends_cooldown(library, provider, monkeypatch):
    library.rows['movies'].append(relation(media(1, year=None)))
    def fail(*args):
        error = RuntimeError('rate limited')
        error.retry_after = '7200'
        raise error
    monkeypatch.setattr(NativeAdapter, 'fetch', fail)
    import time
    before = time.time()
    library.run(movie_earliest_year='2000')
    from inventory import InventoryStore
    store = InventoryStore(library.tmp / 'state')
    assert store.db.execute('SELECT retry_at FROM fetch_evidence').fetchone()[0] >= before + 7200
    store.close()


def test_snapshot_counts_deferred_sources_separately(library, provider):
    from reconciliation import Reconciliation
    from tests.test_reconciliation import LOG
    unknown = relation(media(1, year=None))
    passing = relation(media(2, year=2021))
    rejected = relation(media(3, year=1914))
    library.rows['movies'].extend([unknown, passing, rejected])
    provider.fail = True
    rec = Reconciliation(library.p, {**library.settings, 'movie_earliest_year': '2000'}, LOG, library.tmp / 'snapshot')
    try:
        rec.prepare('scan_all_vods')
        counts = rec.preparation.counts(Query(library.rows['movies']), 'movie')
        assert counts['eligible'] == 3 and counts['passing'] == 1
        assert counts['unresolved'] == 1 and counts['rejected_year'] == 1
        assert counts['retained_unknown'] == 0
        assert rec.report['adopted'] == rec.report['deleted'] == 0
    finally: rec.store.close()


def test_disabled_native_sources_never_fetch_or_allow_filter_deletion(library, provider, monkeypatch):
    obj = media(1)
    rel = relation(obj)
    library.rows['movies'].append(rel)
    library.run()
    path = next(Path(library.settings['root_folder']).rglob('*.strm'))
    obj.year = None
    monkeypatch.setattr(library.p, '_eligible_vod_relations', lambda query, kind: Query([]))
    result = library.run('preview_cleanup', movie_earliest_year='2000', movie_missing_metadata='reject')
    assert provider.requests == [] and result['reconciliation']['filter_candidates'] == 0
    assert path.exists()


def test_oversized_response_is_not_cached_as_success(library, provider, monkeypatch):
    library.rows['movies'].append(relation(media(1, year=None)))
    provider.payload = {'info': {'year': 2021, 'plot': 'x' * 1000}}
    monkeypatch.setattr('enrichment.MAX_RESPONSE_BYTES', 100)
    result = library.run(movie_earliest_year='2000')
    assert result['created_strm'] == 0 and provider.imports == []
    from inventory import InventoryStore
    store = InventoryStore(library.tmp / 'state')
    evidence = store.db.execute('SELECT * FROM fetch_evidence').fetchone()
    assert evidence['response'] is None and evidence['response_state'] == 'failed'
    store.close()
