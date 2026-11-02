import copy
import sys
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

from tests.test_reconciliation import library, media, relation


def forbid_provider(monkeypatch):
    attempts = []

    def forbidden(*args, **kwargs):
        attempts.append(True)
        raise AssertionError('Plugin must use stored Dispatcharr data only')

    monkeypatch.setitem(sys.modules, 'core.xtream_codes', NS(Client=forbidden))
    monkeypatch.setitem(sys.modules, 'apps.vod.tasks', NS(
        refresh_series_episodes=forbidden, batch_process_episodes=forbidden))
    return attempts


@pytest.mark.parametrize('action', [
    'generate_movies', 'generate_series', 'rescan_all', 'preview_cleanup',
    'selective_cleanup', 'rebuild_inventory',
])
def test_actions_never_fetch_providers_or_mutate_native_freshness(library, monkeypatch, action):
    attempts = forbid_provider(monkeypatch)
    show = media(10)
    show_relation = relation(show, 'series')
    show_relation.custom_properties = {'episodes_fetched': False, 'detailed_fetched': False}
    show_relation.last_episode_refresh = 'original series timestamp'
    episode = relation(media(1, series=show, season_number=1, episode_number=1), 'episode')
    episode.last_seen = 'original episode timestamp'
    properties = copy.deepcopy(show_relation.custom_properties)
    library.rows['movies'].append(relation(media(2)))
    library.rows['series'].append(show_relation)
    library.rows['episodes'].append(episode)
    result = library.run(action, refresh_existing=True, m3u_cleanup_enabled=True,
                         m3u_cleanup_timing='rescan')
    assert result['status'] == 'ok' and result.get('errors', 0) == 0
    assert not attempts and not library.calls
    assert show_relation.custom_properties == properties
    assert show_relation.last_episode_refresh == 'original series timestamp'
    assert episode.last_seen == 'original episode timestamp'
    assert not any(key.startswith('provider_') for key in result['reconciliation']['timings'])


def test_missing_episode_enrichment_failure_defers_output(library, monkeypatch):
    attempts = forbid_provider(monkeypatch)
    show = relation(media(10), 'series')
    show.custom_properties = {}
    library.rows['series'].append(show)
    result = library.run('generate_series', refresh_existing=True)
    assert result['status'] == 'ok' and result['episodes_created'] == 0
    # This fake account has no provider credentials; preparation fails safely.
    assert result['reconciliation']['enrichment']['deferred'] == 1
    assert not attempts and not list(Path(library.settings['series_root_folder']).rglob('*.strm'))


def test_database_episode_additions_and_stream_changes_are_applied_without_fetching(library, monkeypatch):
    attempts = forbid_provider(monkeypatch)
    show = media(10)
    library.rows['series'].append(relation(show, 'series'))
    first = relation(media(1, series=show, season_number=1, episode_number=1), 'episode')
    library.rows['episodes'].append(first)
    assert library.run('generate_series', refresh_existing=True)['episodes_created'] == 1
    assert library.run('generate_series', refresh_existing=True)['episodes_created'] == 0
    first.stream_id = 'changed database stream'
    result = library.run('generate_series', refresh_existing=True)
    assert result['episodes_refreshed'] == 1
    library.rows['episodes'].append(relation(
        media(2, series=show, season_number=1, episode_number=2), 'episode'))
    assert library.run('generate_series', refresh_existing=True)['episodes_created'] == 1
    assert not attempts


@pytest.mark.parametrize('action', ['generate_series', 'rescan_all'])
@pytest.mark.parametrize('workers,batch', [('1', '1'), ('3', '1'), ('3', '2')])
def test_repeated_refresh_batches_reach_later_series(library, monkeypatch, action, workers, batch):
    attempts = forbid_provider(monkeypatch)
    episodes = []
    for show_id in range(10, 15):
        show = media(show_id)
        library.rows['series'].append(relation(show, 'series'))
        episode = relation(media(show_id * 100, series=show,
                                 season_number=1, episode_number=1), 'episode')
        library.rows['episodes'].append(episode)
        episodes.append(episode)

    def run():
        result = library.run(action, refresh_existing=True, series_workers=workers,
                             series_batch_size=batch)
        assert result['status'] == 'ok'
        result = result['series'] if action == 'rescan_all' else result
        assert result['errors'] == 0
        return result

    # An unchanged prefix must not consume every subsequent run's batch.
    remaining = len(episodes)
    while remaining:
        expected = min(int(batch), remaining)
        assert run()['episodes_created'] == expected
        remaining -= expected
    root = Path(library.settings['series_root_folder'])
    assert len(list(root.rglob('*.strm'))) == len(episodes)
    assert run()['episodes_created'] == 0

    episodes[-1].stream_id = 'changed database stream'
    assert run()['episodes_refreshed'] == 1
    files = list(root.rglob('*.strm'))
    assert sum('changed database stream' in path.read_text() for path in files) == 1
    # Dispatcharr can replace an episode UUID while retaining its provider ID.
    episodes[-1].episode.uuid = 'replacement-episode-uuid'
    assert run()['episodes_refreshed'] == 1
    assert sum('replacement-episode-uuid' in path.read_text() for path in files) == 1
    show = library.rows['series'][-1].series
    library.rows['episodes'].append(relation(
        media(9999, series=show, season_number=1, episode_number=2), 'episode'))
    assert run()['episodes_created'] == 1
    assert not attempts


@pytest.mark.parametrize('scope', ['strm', 'strm_nfo'])
def test_full_rescan_removes_absent_show_outside_generation_batch(library, monkeypatch, scope):
    attempts = forbid_provider(monkeypatch)
    for show_id in (10, 20):
        show = media(show_id)
        library.rows['series'].append(relation(show, 'series'))
        for number in (1, 2):
            library.rows['episodes'].append(relation(media(
                show_id * 100 + number, series=show,
                season_number=1, episode_number=number), 'episode'))
    assert library.run('rescan_all')['series']['episodes_created'] == 4
    root = Path(library.settings['series_root_folder'])
    removed_folder = next(path.parent for path in root.rglob('tvshow.nfo')
                          if 'Title 20' in path.read_text())
    library.rows['series'].pop()
    # Orphan episode rows alone cannot keep a removed show alive.
    result = library.run('rescan_all', m3u_cleanup_enabled=True,
                         series_batch_size='1', deletion_scope=scope)
    assert result['status'] == 'ok'
    assert result['reconciliation']['deleted'] == 2
    assert not list(removed_folder.rglob('*.strm'))
    assert len(list(root.rglob('*.strm'))) == 2
    if scope == 'strm_nfo':
        assert not removed_folder.exists()
    else:
        assert len(list(removed_folder.rglob('*.nfo'))) == 3
    assert not attempts
