"""Avoid unchanged native imports, while fetching the provider on every refresh."""

import hashlib
import inspect
from importlib import import_module
from contextlib import nullcontext
from functools import lru_cache


# Fall back to the native task when its import semantics change. No monkeypatches
# or changes to Dispatcharr are involved in this optimization.
IMPORTER_SHA256 = '897ffb21cd870f55e6074a24a20afc3c75662349186a482340179a3bbcaafef4'


@lru_cache(maxsize=4)
def compatible(refresher, batch):
    try:
        source = inspect.getsource(refresher) + inspect.getsource(batch)
        return hashlib.sha256(source.encode()).hexdigest() == IMPORTER_SHA256
    except (TypeError, OSError):
        return False


def details_unchanged(series, info, helpers):
    if not isinstance(info, dict):
        return False
    return not (
        helpers.should_update_field(series.description, info.get('plot'))
        or (helpers.normalize_rating(info.get('rating'))
            and (not series.rating or not str(series.rating).strip()))
        or helpers.should_update_field(series.genre, info.get('genre'))
        or (helpers.extract_year_from_data(info) and not series.year)
    )


def episodes_unchanged(relation, payload, rows, helpers):
    """Conservatively compare all fields written by the supported importer."""
    if not isinstance(payload, (dict, list)) or not payload:
        return False
    items = payload.items() if isinstance(payload, dict) else enumerate(payload)
    by_stream = {str(row.stream_id): row for row in rows}
    if len(by_stream) != len(rows):
        return False
    seen = set()
    for season, episodes in items:
        if not isinstance(episodes, list):
            return False
        for data in episodes:
            if not isinstance(data, dict) or not data.get('id'):
                return False
            stream = str(data['id'])
            if stream in seen or stream not in by_stream:
                return False
            seen.add(stream)
            row = by_stream[stream]
            ep = row.episode
            season_number = int(season)
            episode_number = int(data.get('episode_num', 0))
            copied = {**data, '_season_number': season_number}
            if (row.series_relation_id != relation.pk
                    or row.m3u_account_id != relation.m3u_account_id
                    or ep.series_id != relation.series_id
                    or row.container_extension != data.get('container_extension', 'mp4')
                    or row.custom_properties != {'info': copied, 'season_number': season_number}
                    or ep.season_number != season_number
                    or ep.episode_number != episode_number):
                return False
            info = data.get('info', {})
            if not isinstance(info, dict):
                return False
            props = {}
            if info.get('crew'):
                props['crew'] = info['crew']
            image = helpers.extract_string_from_array_or_string(info.get('movie_image'))
            if image:
                props['movie_image'] = image
            backdrop = helpers.extract_string_from_array_or_string(info.get('backdrop_path'))
            if backdrop:
                props['backdrop_path'] = [backdrop]
            expected = {
                'name': data.get('title', 'Unknown Episode'),
                'description': (info.get('plot') or info.get('overview', '')) if info else '',
                'rating': helpers.normalize_rating(info.get('rating')) if info else None,
                'air_date': helpers.extract_date_from_data(info) if info else None,
                'duration_secs': info.get('duration_secs') if info else None,
                'tmdb_id': info.get('tmdb_id') if info else None,
                'imdb_id': info.get('imdb_id') if info else None,
                'custom_properties': props or None,
            }
            if any(getattr(ep, key) != value for key, value in expected.items()):
                return False
    return seen == set(by_stream)


def refresh(relation, refresher, rec=None):
    """Return loaded rows on the fast path; otherwise let generation reload them."""
    tasks = import_module('apps.vod.tasks')

    account, series = relation.m3u_account, relation.series
    kwargs = dict(account=account, series=series,
                  external_series_id=relation.external_series_id)
    batch = getattr(tasks, 'batch_process_episodes', None)
    if not batch or not compatible(refresher, batch):
        refresher(**kwargs)
        return None
    from core.xtream_codes import Client
    from apps.vod.models import M3UEpisodeRelation, M3USeriesRelation
    from django.utils import timezone
    from django.db import transaction

    with rec.measure('provider_fetch', 1, worker=True) if rec else nullcontext():
        with Client(account.server_url, account.username, account.password,
                    account.get_user_agent_string()) as client:
            response = client.get_series_info(relation.external_series_id)
    try:
        if not isinstance(response, dict) or not details_unchanged(series, response.get('info', {}), tasks):
            refresher(**kwargs)
            return None
        payload = response.get('episodes')
        with rec.measure('provider_compare', 1, worker=True) if rec else nullcontext():
            rows = list(M3UEpisodeRelation.objects.filter(
                m3u_account=account, episode__series=series,
            ).select_related('episode').order_by(
                'episode__season_number', 'episode__episode_number', 'id'))
            # Check the importer's additional cleanup scope separately. An OR
            # across these joins prevents PostgreSQL using the episode-series
            # index efficiently on large catalogues.
            misplaced = M3UEpisodeRelation.objects.filter(series_relation=relation).exclude(
                pk__in=[row.pk for row in rows],
            ).exists()
            same = not misplaced and episodes_unchanged(relation, payload, rows, tasks)
    except (ValueError, TypeError, AttributeError):
        # Unrecognized data is handled by Dispatcharr, never declared unchanged.
        refresher(**kwargs)
        return None
    if not same:
        with rec.measure('provider_import', 1, worker=True) if rec else nullcontext():
            if isinstance(payload, (dict, list)) and payload:
                refresher(**kwargs, episodes_data=payload)
            else:
                refresher(**kwargs)
        return None
    # Preserve the native freshness bookkeeping using small UPDATEs instead of
    # rebuilding bulk-update expressions for every relation and JSON payload.
    with (rec.measure('provider_touch', len(rows), worker=True) if rec else nullcontext()), transaction.atomic():
        now = timezone.now()
        M3UEpisodeRelation.objects.filter(pk__in=[row.pk for row in rows]).update(last_seen=now)
        props = {**(relation.custom_properties or {}), 'episodes_fetched': True, 'detailed_fetched': True}
        M3USeriesRelation.objects.filter(pk=relation.pk).update(
            custom_properties=props, last_episode_refresh=now)
    if rec:
        with rec.counter_lock:
            rec.report['provider_imports_skipped'] = rec.report.get('provider_imports_skipped', 0) + 1
    return rows
