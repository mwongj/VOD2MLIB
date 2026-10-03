from copy import deepcopy
from types import SimpleNamespace as NS

import pytest

from series_refresh import episodes_unchanged, details_unchanged, compatible, refresh


def fixture():
    helpers = NS(normalize_rating=lambda value: value,
                 extract_string_from_array_or_string=lambda value: value or '',
                 extract_date_from_data=lambda info: info.get('air_date'),
                 extract_year_from_data=lambda info: info.get('year'),
                 should_update_field=lambda old, new: bool(new and not old))
    data = {'id': 100, 'title': 'Episode', 'episode_num': 1, 'info': {
        'plot': 'Plot', 'rating': 8, 'duration_secs': 120, 'tmdb_id': 3,
        'imdb_id': 'tt4', 'air_date': '2026-10-03', 'crew': 'Director',
        'movie_image': 'image', 'backdrop_path': 'backdrop'}}
    ep = NS(name='Episode', description='Plot', rating=8, duration_secs=120,
            tmdb_id=3, imdb_id='tt4', air_date='2026-10-03', season_number=1,
            episode_number=1, custom_properties={'crew': 'Director',
            'movie_image': 'image', 'backdrop_path': ['backdrop']})
    ep.series_id = 3
    rel = NS(pk=1, m3u_account_id=2, series_id=3)
    row = NS(stream_id='100', series_relation_id=1, m3u_account_id=2, container_extension='mp4',
             episode=ep, custom_properties={'info': {**deepcopy(data), '_season_number': 1},
                                           'season_number': 1})
    return rel, {'1': [data]}, [row], helpers


def test_exact_imported_metadata_matches_without_mutating_provider_payload():
    rel, payload, rows, helpers = fixture()
    before = deepcopy(payload)
    assert episodes_unchanged(rel, payload, rows, helpers)
    assert payload == before


@pytest.mark.parametrize('field', ['name', 'description', 'rating', 'duration_secs',
                                  'tmdb_id', 'imdb_id', 'air_date', 'custom_properties',
                                  'season_number', 'episode_number'])
def test_changed_model_metadata_requires_native_import(field):
    rel, payload, rows, helpers = fixture()
    setattr(rows[0].episode, field, 'changed')
    assert not episodes_unchanged(rel, payload, rows, helpers)


@pytest.mark.parametrize('change', ['new', 'removed', 'duplicate', 'payload', 'fk', 'extension'])
def test_changed_relation_or_provider_inventory_requires_native_import(change):
    rel, payload, rows, helpers = fixture()
    if change == 'new':
        payload['1'].append({**payload['1'][0], 'id': 101})
    elif change == 'removed':
        payload['1'] = []
    elif change == 'duplicate':
        rows.append(rows[0])
    elif change == 'payload':
        payload['1'][0]['added'] = '1234'
    elif change == 'fk':
        rows[0].series_relation_id = None
    else:
        rows[0].container_extension = 'mkv'
    assert not episodes_unchanged(rel, payload, rows, helpers)


@pytest.mark.parametrize('field, value', [('plot', 'New plot'), ('rating', 9),
                                       ('genre', 'Drama'), ('year', 2020)])
def test_series_detail_enrichment_requires_native_task(field, value):
    helpers = fixture()[-1]
    series = NS(description='', rating='', genre='', year=None)
    assert not details_unchanged(series, {field: value}, helpers)
    assert details_unchanged(series, {}, helpers)


def test_unknown_native_importer_falls_back_without_fetching_provider(monkeypatch):
    import sys
    calls = []
    task = lambda **kwargs: calls.append(kwargs)
    monkeypatch.setitem(sys.modules, 'apps.vod.tasks', NS(batch_process_episodes=lambda: None))
    relation = NS(m3u_account=object(), series=object(), external_series_id='10')
    assert not compatible(task, lambda: None)
    assert refresh(relation, task) is None
    assert calls == [dict(account=relation.m3u_account, series=relation.series,
                          external_series_id='10')]


@pytest.mark.parametrize('changed, misplaced', [(False, False), (True, False), (False, True)])
def test_refresh_fetches_latest_payload_and_preserves_bookkeeping(monkeypatch, changed, misplaced):
    import sys
    import series_refresh
    from contextlib import nullcontext
    rel, payload, rows, helpers = fixture()
    rel.m3u_account = NS(server_url='server', username='user', password='private',
                        get_user_agent_string=lambda: 'agent')
    rel.series = NS(description='Plot', rating=8, genre='Drama', year=2020)
    rel.external_series_id = '10'
    rel.custom_properties = {'custom_setting': 'preserve'}
    rows[0].pk = 2
    fetches, imports, updates = [], [], []
    if changed:
        payload['1'][0]['title'] = 'Updated title'
    class Client:
        def __init__(self, *args): pass
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def get_series_info(self, external):
            fetches.append(external)
            return {'info': {}, 'episodes': payload}
    class Query:
        def filter(self, *args, **kwargs): return self
        def select_related(self, *args): return self
        def order_by(self, *args): return self
        def exclude(self, **kwargs): return self
        def exists(self): return misplaced
        def __iter__(self): return iter(rows)
        def update(self, **kwargs): updates.append(kwargs)
    monkeypatch.setattr(series_refresh, 'compatible', lambda *args: True)
    monkeypatch.setitem(sys.modules, 'apps.vod.tasks', NS(**vars(helpers), batch_process_episodes=object()))
    monkeypatch.setitem(sys.modules, 'core.xtream_codes', NS(Client=Client))
    monkeypatch.setitem(sys.modules, 'apps.vod.models', NS(
        M3UEpisodeRelation=NS(objects=Query()), M3USeriesRelation=NS(objects=Query())))
    monkeypatch.setitem(sys.modules, 'django.utils', NS(timezone=NS(now=lambda: 'now')))
    monkeypatch.setitem(sys.modules, 'django.db', NS(transaction=NS(atomic=nullcontext)))
    result = refresh(rel, lambda **kwargs: imports.append(kwargs))
    assert fetches == ['10']
    if changed or misplaced:
        assert result is None and len(imports) == 1 and not updates
        assert imports[0]['episodes_data'] == payload
    else:
        assert result == rows and not imports
        assert updates == [{'last_seen': 'now'}, {
            'custom_properties': {'custom_setting': 'preserve', 'episodes_fetched': True,
                                  'detailed_fetched': True}, 'last_episode_refresh': 'now'}]
