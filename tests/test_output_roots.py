from pathlib import Path
import sqlite3

from tests.test_reconciliation import library, media, relation


def test_series_roots_generate_refresh_and_clean_independently(library):
    show = media(10)
    library.rows['series'].append(relation(show, 'series'))
    episode = relation(media(1, series=show, season_number=1, episode_number=1), 'episode')
    library.rows['episodes'].append(episode)
    live = Path(library.settings['series_root_folder'])
    test = live.with_name('series_tmp')
    for root in (test, live):
        result = library.run('generate_series', series_root_folder=str(root), refresh_existing=True)
        assert result['episodes_created'] == 1
    test_strm, live_strm = next(test.rglob('*.strm')), next(live.rglob('*.strm'))
    original = test_strm.read_bytes()
    episode.stream_id = 'updated-stream'
    result = library.run('rescan_all')
    assert result['series']['episodes_refreshed'] == 1
    assert 'updated-stream' in live_strm.read_text()
    assert test_strm.read_bytes() == original
    # Switching back revisits this root's files; it does not inherit the other
    # root's completed generation decisions or migrate any paths.
    result = library.run('generate_series', series_root_folder=str(test), refresh_existing=True)
    assert result['episodes_refreshed'] == 1
    library.rows['series'].clear()
    result = library.run('selective_cleanup', m3u_cleanup_enabled=True, deletion_scope='strm_nfo')
    assert result['reconciliation']['deleted'] == 1
    assert result['reconciliation']['source_presence_requested'] == 2
    assert not live_strm.exists() and test_strm.exists()
    assert list(test.rglob('*.nfo'))
    result = library.run('selective_cleanup', series_root_folder=str(test),
                         m3u_cleanup_enabled=True, deletion_scope='strm_nfo')
    assert result['reconciliation']['deleted'] == 1
    assert not test_strm.exists()


def test_live_cleanup_keeps_missing_test_root_inventory(library):
    library.rows['movies'].append(relation(media(1)))
    test = library.tmp / 'movies_tmp'
    assert library.run(root_folder=str(test))['created_strm'] == 1
    test_strm = next(test.rglob('*.strm'))
    test_strm.unlink()
    result = library.run('selective_cleanup')
    assert result['reconciliation']['missing'] == 0
    with sqlite3.connect(library.tmp / 'state' / 'inventory.sqlite3') as db:
        assert db.execute('SELECT 1 FROM files WHERE path=?', (str(test_strm),)).fetchone()


def test_unrelated_root_sources_cannot_disable_live_cleanup(library):
    library.rows['movies'].append(relation(media(1)))
    test = library.tmp / 'movies_tmp'
    library.run(root_folder=str(test))
    library.run()
    with sqlite3.connect(library.tmp / 'state' / 'inventory.sqlite3') as db:
        db.execute('UPDATE sources SET source=? WHERE path=?',
                   ('unrecognized test source', str(next(test.rglob('*.strm')))))
    library.rows['movies'].clear()
    result = library.run('selective_cleanup', m3u_cleanup_enabled=True)
    assert not result['reconciliation']['warnings']
    assert result['reconciliation']['deleted'] == 1
    assert result['reconciliation']['source_presence_requested'] == 1
    assert next(test.rglob('*.strm')).exists()
