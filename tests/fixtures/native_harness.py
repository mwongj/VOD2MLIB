"""Isolated Django database harness for pinned Dispatcharr population functions."""
import ast
import logging
import re
import sys
import types
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from django.conf import settings
settings.configure(INSTALLED_APPS=[], DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}}, USE_TZ=True)
import django
django.setup()
from django.db import models, connection, transaction, IntegrityError
from django.db.models import Q
from django.utils import timezone
from enrichment import NativeAdapter, normalize

class Base(models.Model):
    class Meta:
        abstract = True
        app_label = 'compat'

class Agent(Base):
    user_agent = models.CharField(max_length=100, default='compat')

class Account(Base):
    server_url = models.CharField(max_length=100, default='http://provider.test')
    username = models.CharField(max_length=100, default='private')
    password = models.CharField(max_length=100, default='private')
    user_agent = models.ForeignKey(Agent, on_delete=models.CASCADE)
    def get_user_agent(self): return self.user_agent
    def get_user_agent_string(self): return self.user_agent.user_agent

class Content(Base):
    name = models.CharField(max_length=100, default='Title')
    description = models.TextField(default='')
    year = models.IntegerField(null=True)
    rating = models.CharField(max_length=30, default='0', null=True)
    genre = models.CharField(max_length=100, default='')
    tmdb_id = models.CharField(max_length=50, null=True)
    imdb_id = models.CharField(max_length=50, null=True)
    custom_properties = models.JSONField(default=dict, null=True)
    duration_secs = models.IntegerField(null=True)
    class Meta:
        abstract = True
        app_label = 'compat'

class Movie(Content): pass
class Series(Content): pass
class Episode(Content):
    series = models.ForeignKey(Series, on_delete=models.CASCADE)
    season_number = models.IntegerField(default=0)
    episode_number = models.IntegerField(default=0)
    air_date = models.DateField(null=True)
    class Meta:
        app_label = 'compat'
        unique_together = [('series', 'season_number', 'episode_number')]

class Relation(Base):
    m3u_account = models.ForeignKey(Account, on_delete=models.CASCADE)
    custom_properties = models.JSONField(default=dict, null=True)
    class Meta:
        abstract = True
        app_label = 'compat'

class M3UMovieRelation(Relation):
    movie = models.ForeignKey(Movie, on_delete=models.CASCADE)
    stream_id = models.CharField(max_length=100)
    last_advanced_refresh = models.DateTimeField(null=True)

class M3USeriesRelation(Relation):
    series = models.ForeignKey(Series, on_delete=models.CASCADE)
    external_series_id = models.CharField(max_length=100)
    last_episode_refresh = models.DateTimeField(null=True)

class M3UEpisodeRelation(Relation):
    episode = models.ForeignKey(Episode, on_delete=models.CASCADE)
    series_relation = models.ForeignKey(M3USeriesRelation, on_delete=models.CASCADE, null=True)
    stream_id = models.CharField(max_length=100)
    container_extension = models.CharField(max_length=30, default='mp4')
    last_seen = models.DateTimeField(null=True)
    class Meta:
        app_label = 'compat'
        unique_together = [('m3u_account', 'stream_id')]

model_module = types.ModuleType('apps.vod.models')
for cls in [Movie, Series, Episode, M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation]:
    setattr(model_module, cls.__name__, cls)
for name in ['apps', 'apps.vod', 'core']:
    sys.modules[name] = types.ModuleType(name)
sys.modules['apps.vod.models'] = model_module
xtream = types.ModuleType('core.xtream_codes')
xtream.Client = lambda *a, **k: (_ for _ in ()).throw(AssertionError('Replay must not request provider'))
xtream.logger = logging.getLogger('compat.client')
sys.modules['core.xtream_codes'] = xtream

version = sys.argv[1]
source_path = Path(sys.argv[2])
selected = {'refresh_series_episodes', 'batch_process_episodes', 'refresh_movie_advanced_data',
            'extract_string_from_array_or_string', 'clean_custom_properties', 'should_update_field',
            'normalize_rating', 'extract_year', 'extract_year_from_title', 'extract_year_from_data', 'extract_date_from_data'}
tree = ast.parse(source_path.read_text(encoding='utf8'))
functions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in selected]
for node in functions: node.decorator_list = []
tasks = types.ModuleType('apps.vod.tasks')
tasks.__dict__.update({k: v for k, v in vars(model_module).items() if not k.startswith('__')})
tasks.__dict__.update(timezone=timezone, transaction=transaction, Q=Q, IntegrityError=IntegrityError,
                      datetime=datetime, re=re, logger=logging.getLogger('compat.tasks'), XtreamCodesClient=xtream.Client)
sys.modules['apps.vod.tasks'] = tasks
exec(compile(ast.Module(body=functions, type_ignores=[]), str(source_path), 'exec'), tasks.__dict__)

with connection.schema_editor() as schema:
    for cls in [Agent, Account, Movie, Series, Episode, M3UMovieRelation, M3USeriesRelation, M3UEpisodeRelation]:
        schema.create_model(cls)
account = Account.objects.create(user_agent=Agent.objects.create())
show = Series.objects.create()
rel = M3USeriesRelation.objects.create(m3u_account=account, series=show, external_series_id='100')
movie = Movie.objects.create()
movierel = M3UMovieRelation.objects.create(m3u_account=account, movie=movie, stream_id='101')
adapter = NativeAdapter(None)
payload = normalize({'info': {'rating': '8.1', 'genre': 'Drama', 'releaseDate': '2020-02-03'},
                     'episodes': [[{'id': '200', 'title': 'Pilot', 'episode_num': '1'}]]}, 'series')
current = adapter.populate(rel, 'series', payload)
assert current.series.rating == '8.1' and current.series.year == 2020
assert M3UEpisodeRelation.objects.get(stream_id='200').series_relation_id == rel.id
assert Episode.objects.get().season_number == 0
# Movie helper's 24-hour gate is bypassed only for coordinator-selected missing data.
movierel.last_advanced_refresh = timezone.now()
movierel.save()
current_movie = adapter.populate(movierel, 'movie', normalize({'info': {'rating': '8', 'genre': 'Drama', 'year': 2021}}, 'movie'))
assert current_movie.movie.year == 2021 and float(current_movie.movie.rating) == 8
# Empty provider episode lists are verified, and no synthetic episodes appear.
emptyshow = Series.objects.create()
emptyrel = M3USeriesRelation.objects.create(m3u_account=account, series=emptyshow, external_series_id='empty')
adapter.populate(emptyrel, 'series', normalize({'info': {}, 'episodes': {}}, 'series'))
assert not Episode.objects.filter(series=emptyshow).exists()
# Native importer can silently skip writes/return normally. Readback must roll back flags and metadata.
original = tasks.batch_process_episodes
tasks.batch_process_episodes = lambda *a, **k: None
try:
    adapter.populate(emptyrel, 'series', normalize({'info': {'genre': 'Horror'}, 'episodes': {'1': [{'id': '300', 'episode_num': 1}]}}, 'series'))
except ValueError:
    emptyshow.refresh_from_db()
    assert emptyshow.genre == ''
else: raise AssertionError('Partial import established success')
tasks.batch_process_episodes = original
# A stream already owned by another provider-show must never be reassigned.
try:
    adapter.populate(emptyrel, 'series', payload)
except ValueError: pass
else: raise AssertionError('Source reassignment accepted')
assert M3UEpisodeRelation.objects.get(stream_id='200').episode.series_id == show.id
# Native movie identity merging may replace the model behind a stable provider relation.
target = Movie.objects.create(tmdb_id='shared')
def merge(source, relation, tmdb, imdb):
    relation.movie = target
    relation.save(update_fields=['movie'])
    return target, True
tasks.handle_movie_id_conflicts = merge
merged = adapter.populate(movierel, 'movie', normalize({'info': {'tmdb_id': 'shared', 'year': 2022}}, 'movie'))
assert merged.movie_id == target.id and merged.movie.year == 2022
# A normal helper return without persistence cannot establish a verified omission.
original = tasks.refresh_movie_advanced_data
tasks.refresh_movie_advanced_data = lambda *a, **k: 'Advanced data refreshed.'
try:
    adapter.populate(movierel, 'movie', normalize({'info': {}}, 'movie'))
except ValueError: pass
else: raise AssertionError('Normal return accepted as persistence')
tasks.refresh_movie_advanced_data = original
# Swallowed save errors must roll back native mutation rather than leave completion flags.
original_save = Series.save
def failed_save(self, *a, **k): raise ValueError('Failed native save')
Series.save = failed_save
try:
    adapter.populate(emptyrel, 'series', normalize({'info': {'genre': 'Horror'}, 'episodes': {}}, 'series'))
except ValueError: pass
else: raise AssertionError('Failed save accepted')
Series.save = original_save
emptyshow.refresh_from_db()
assert emptyshow.genre == ''
print(f'{version}: real Django transaction and native helper/importer checks passed')
