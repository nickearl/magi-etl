import os, requests, re, json, bs4, time, ast
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import dash_bootstrap_components as dbc
from datetime import date, datetime
from dotenv import load_dotenv
import biutils as bi
from connectors import QueryAthena, AmazonS3Connector, QueryRedshift, QueryRestApi, CustomAPIException
from dash_app import TaxonomyRelationships
import redis
import google.oauth2.credentials
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.auth.exceptions import RefreshError
from celery import Celery, shared_task
import hashlib
from openai import OpenAI
from pydantic import BaseModel
from slack_sdk import WebClient
from conf import global_config
from flask import session
# Get environment variables
load_dotenv()

REDISCLOUD_URL = os.environ['REDISCLOUD_URL']
OPENAI_API_KEY = os.environ['OPENAI_API_KEY']
GEMINI_API_KEY = os.environ['GEMINI_API_KEY']
SLACK_TOKEN = os.environ['SLACK_TOKEN']
DEPLOY_ENV = os.environ['DEPLOY_ENV']
DRYRUN = True if os.environ['DRYRUN'].lower() == 'true' else False
CACHE_KEY = global_config['app_prefix']
if global_config['enable_google_auth']:
	ENCRYPTION_SECRET_KEY = os.environ['ENCRYPTION_SECRET_KEY']
	GOOGLE_CLIENT_ID = os.environ['GOOGLE_CLIENT_ID']
	GOOGLE_CLIENT_SECRET = os.environ['GOOGLE_CLIENT_SECRET']
print(f'DRYRUN is: {DRYRUN}')

b = bi.BiUtils()

def auto_num_format(raw_number):
	num = float(f'{raw_number:.2g}')
	magnitude = 0
	while abs(num) >= 1000:
		magnitude += 1
		num /= 1000.0
	return '{} {}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), 
		['', 'K', 'M', 'B', 'T'][magnitude])

def extract_creds(creds):
	formatted_creds = Credentials(
		token=creds.get("access_token"),
		refresh_token=creds.get("refresh_token"),
		token_uri="https://oauth2.googleapis.com/token",
		client_id=GOOGLE_CLIENT_ID,
		client_secret=GOOGLE_CLIENT_SECRET,
		scopes=creds.get("scope", [])
	)
	return formatted_creds

def set_last_run(key,isostring):
	rdata = {}
	try:
		r = redis.from_url(REDISCLOUD_URL)
		rdata = json.loads(r.get(key))
	except Exception as e:
		print(f'No existing redis key for: {key}')
		pass

	if rdata == {}:
		try:
			from transformer import MagiScheduler
			magi = MagiScheduler()
			rdata = magi.get_default_schedule()
		except Exception as e:
			print(f'Error falling back to default schedule: {e}')
	r = redis.from_url(REDISCLOUD_URL)
	rdata['last_run'] = isostring
	print(f'saving last run time to redis: {rdata}')	
	r.set(key, json.dumps(rdata))

class AnchorCalendar:
	def __init__(self, anchor_date=datetime.now()):
		from datetime import date, datetime, timedelta
		anchor_date = pd.to_datetime(anchor_date)
		self.current_date = anchor_date.date()
		self.latest_date = (anchor_date + timedelta(days=-1)).date()
		self.current_quarter = ((anchor_date.month-1)//3) + 1
		self.last_quarter = self.current_quarter - 1 if self.current_quarter != 1 else 4
		self.latest_complete_month_start = (anchor_date + pd.DateOffset(months=-1)).replace(day=1).date()
		self.latest_complete_month_end = (self.latest_complete_month_start + pd.DateOffset(months=1) + pd.DateOffset(days=-1)).date()
		self.current_month_start = self.latest_date.replace(day=1)
		self.current_month_end = (self.latest_date.replace(day=1) + pd.DateOffset(months=1) + pd.DateOffset(days=-1)).date()
		self.latest_complete_week_start = (anchor_date - timedelta(days=anchor_date.isoweekday() - 1) - timedelta(days=7)).date()
		self.latest_complete_week_end = (self.latest_complete_week_start + pd.DateOffset(days=6)).date()
		self.current_week_start = (anchor_date - timedelta(days=anchor_date.isoweekday() - 1)).date()
		self.current_week_end = (self.current_week_start + pd.DateOffset(days=6)).date()
		self.mom = (anchor_date + pd.DateOffset(months=-1)).date()
		self.yoy = (anchor_date + pd.DateOffset(years=-1)).date()

class TaxonomyRelationships:
	"""
	Content recommendation engine based on taxonomy relationships from external ML model output and traffic data.
	"""
	def __init__(self):
		r"""
		== Usage ==

		Inputs:
		raw_taxonony_df: "select * from my_database.my_table"
		traffic_df: wiki_id, wiki, vertical, lang, is_kid_wiki, is_monetized, users, pageviews, page_count
		taxonomy_df: raw_taxonomy_df -> build_taxonomy_df()

		== Build optimized taxonomy dataframe from raw data (takes ~30 minutes, do offline once initially then store output df) ==

		tax = TaxonomyRelationships()
		traffic_df = pd.read_csv('traffic.csv')
		raw_taxonomy_df = pd.read_csv('all_taxonomy.csv')
		taxonomy_df = None
		fields = [x for x in raw_taxonomy_df.columns if 'site_all' in x and 'vertical' not in x]
		field_keys = [re.sub(r'.*_all_(\w+)s$', r'\1', field) for field in fields]
		taxonomy_df, df = tax.build_datasets(raw_taxonomy_df=raw_taxonomy_df, traffic_df=traffic_df, save=True)


		== Or load existing taxonomy dataframe  ==
		taxonomy_df = pd.read_csv('all_taxonomy_dict.csv')
		field_keys = taxonomy_df.columns[1:]
		taxonomy_df, df = tax.build_datasets(taxonomy_df=taxonomy_df, traffic_df=traffic_df, save=True)

		== Build a feature matrix from each dictionary-formatted taxonomy field, then calculate a weighted cosine similarity matrix ==
		weights = {
			'franchise': 1/4,
			'genre': 1/4,
			'subgenre':1/4,
			'theme': 1/4,
		}
		similarity_matrix = tax.build_matrices(taxonomy_df=taxonomy_df, weights=weights, save=True)

		== Get similar content for a given wiki ID ==
		related = tax.get_similar_content(df=df, input_wiki_id='147', similarity_matrix=similarity_matrix, n=5, alpha=0.8)
		print(related)

		"""
		from sklearn.metrics.pairwise import cosine_similarity
		from scipy.sparse import lil_matrix,csr_matrix, save_npz, load_npz, hstack
		from scipy.sparse.linalg import norm as sparse_norm
		print('Initializing TaxonomyRelationships...')
		self.calendar = AnchorCalendar()
		self.init_time = datetime.now()
		self.conf = {
			'prefix': 'tr',
			'cache_path': 'tr:cache',
			's3_path': 'taxonomy-relationships/',
			'default_weights': {
				'franchise': 1/4,
				# 'entity_type': 0, # No data
				'genre': 1/4,
				'subgenre':1/4,
				# 'platform': 0, # No data
				# 'studio': 0, # No data
				# 'tv_network': 0, # No data
				'theme': 1/4,
				# 'installment_id': 1/7, # low factor
				# 'installment_type': 1/7, # low factor
				# 'installment_title': 1/7, # low factor
				# 'age_rating': 1, # No data
			}
		}
		self.data = {
			'raw_taxonomy': None,
			'taxonomy': None,
			'traffic': None,
			'dataset': None,
		}
		self.queries = {
			'raw_taxonomy': 'select * from my_database.my_table',
			'traffic': """
				with configs as (
					select
						'$$year$$' as year,
						'$$month$$' as month
				),
				taxonomy_wikis as (
					select
						cast(content_ids['wiki_id'] as varchar) as wiki_id,
						count(*) as count
						from my_database.my_table
						group by 1
						order by 2 desc
					),
				traffic as (
					select
					* from (
						select
							cast(cast(floor(wiki_id) as bigint) as varchar) AS wiki_id,
							count(distinct analytics_id) as users,
							sum(pageviews) as pageviews,
							count(distinct concat(cast(wiki_id as varchar),'-',content_id)) as page_count
						from AwsDataCatalog.analytics."my_analytics_table"
						where brand = 'acme'
						and platform = 'Web'
						and year = (select year from configs) and month = (select month from configs)
						group by 1
						order by 2 desc
					)
					where wiki_id is not null and wiki_id != ''
				),
				combined as (
					select
						t1.wiki_id as wiki_id,
						lower(split_part(split_part(t3.url,'://',2),'/',1)) as wiki,
						vertical_name as vertical,
						lang,
						is_kid_wiki,
						is_monetized,
						t2.users as users,
						t2.pageviews as pageviews,
						t2.page_count as page_count
					from taxonomy_wikis t1
					left join traffic t2
					on cast(t1.wiki_id as varchar) = cast(t2.wiki_id as varchar)
					left join analytics.dimension_table t3
					on cast(t1.wiki_id as varchar) = cast(t3.wiki_id as varchar)
					order by users desc
				)
				select * from combined
			""".replace('$$year$$', f'{self.calendar.latest_complete_month_start:%Y}').replace('$$month$$', f'{self.calendar.latest_complete_month_start:%m}')
		}
		# imported modules
		self.cosine_similarity = cosine_similarity
		self.lil_matrix = lil_matrix
		self.csr_matrix = csr_matrix
		self.save_npz = save_npz
		self.load_npz = load_npz
		self.hstack = hstack
		self.sparse_norm = sparse_norm
		self.refresh_data()
		self.build_datasets()
	
	def refresh_data(self):
		print(f'TR: Fetching data for Recommendations')

		# Querying directly for full taxonomy is cumbersome; precompile the dict_df offline and load from cache or s3
		r = redis.from_url(REDISCLOUD_URL)
		s3 = AmazonS3Connector()
		try:
			self.data['taxonomy'] = pd.DataFrame(json.loads(r.get(f'{self.conf["cache_path"]}:taxonomy')))
			print('Loaded taxonomy df from cache')
		except Exception as e:
			try:
				print(f'No taxonomy df cached, loading from s3')
				self.data['taxonomy'] = s3.get_from_s3(f'{self.conf['s3_path']}all_taxonomy_dict.csv')
				print('caching to redis')
				r.set(f'{self.conf["cache_path"]}:taxonomy', json.dumps(self.data['taxonomy'].to_dict('records')))
			except Exception as e:
				print(f'Unable to get taxonomy df: {e}')
				pass
			pass

		queries = {
			x: {
				'query': self.queries[x],
				'cache_key': f'{self.conf['cache_path']}:{x}',
			} for x in self.data.keys() if x == 'traffic'
		}
		qa = QueryAthena()
		results = qa.run_queries_with_cache(queries, force_refresh=False, poll_interval=5, on_progress=None, prev_steps=0, post_steps=0, cache_ttl=None)
		self.data['traffic'] = results['traffic']
		return results
	
	def get_raw_taxonomy(self):
		queries = {
			'raw_taxonomy': {
				'query': self.queries['raw_taxonomy'],
				'cache_key': f'{self.conf["cache_path"]}:raw_taxonomy',
			}
		}
		qa = QueryAthena()
		results = qa.run_queries_with_cache(queries, force_refresh=False, poll_interval=5, on_progress=None, prev_steps=0, post_steps=0, cache_ttl=None)
		self.data['raw_taxonomy'] = results['raw_taxonomy']
		return results
	
	def build_taxonomy_df(self, df, fields, save=False):
		tax_df = pd.DataFrame()

		def wrap_quotes(value):
			if str(value).lower() == 'nan':
				return f'"{value.strip()}"'
			try:
				float(value)
				return value
			except ValueError:
				return f'"{value.strip()}"'
		
		def taxonomy_field_to_dict_list(x):
			y = str(x)
			y = re.sub(r'(\b\w+\b)=([^\{\},]+)', lambda m: f'"{m.group(1)}":{wrap_quotes(m.group(2))}', y)
			z = [{'confidence': 1.0, 'value': y}] # fallback for strings
			try:
				y = ast.literal_eval(y)
				if type(y) not in [list,dict]:
					y = z
			except (SyntaxError, ValueError):
				try:
					y = json.loads(y)
				except json.JSONDecodeError as e:
					y = z
			
			return y
		# parse content IDs
		df['wiki_id'] = df['content_ids'].apply(lambda x: taxonomy_field_to_dict_list(x)['wiki_id'])
		
		keep_fields = ['wiki_id'] + fields
		df = df.fillna(0).groupby(keep_fields).size().reset_index()

		tax_df['wiki_id'] = df['wiki_id']

		# parse taxonomy fields
		field_keys = [re.sub(r'.*_all_(\w+)s$', r'\1', field) for field in fields]
		for i, field in enumerate(fields):
			tax_df[field_keys[i]] = df[field].apply(lambda x: taxonomy_field_to_dict_list(x))
			tax_df[field_keys[i]] = tax_df[field_keys[i]].apply(lambda x: json.dumps(x))

		tax_df = tax_df.groupby([x for x in tax_df.columns], as_index=False).size()
		for i, field in enumerate(fields):
			tax_df[field_keys[i]] = tax_df[field_keys[i]].apply(lambda x: json.loads(x))
		if save:
			tax_df.to_csv('all_taxonomy_dict.csv', index=False)
		return tax_df

	def build_datasets(self,raw_taxonomy_df=None,taxonomy_df=None,traffic_df=None, save=False):
		"""
		raw_taxonony_df: "select * from my_database.my_table"
		taxonomy_df: raw_taxonomy_df -> build_taxonomy_df()
		traffic_df: wiki_id, wiki, vertical, lang, is_kid_wiki, is_monetized, users, pageviews, page_count
		"""
		if traffic_df is None:
			traffic_df = self.data['traffic']
		if taxonomy_df is None:
			taxonomy_df = self.data['taxonomy']
		if raw_taxonomy_df is None:
			raw_taxonomy_df = self.data['raw_taxonomy']
		if taxonomy_df is not None and not taxonomy_df.empty:
			print(f'Using provided taxonomy_df: {taxonomy_df.shape}')
			field_keys = taxonomy_df.columns[1:]
		elif raw_taxonomy_df is not None and not raw_taxonomy_df.empty:
			print('Building taxonomy_df from raw taxonomy...')
			fields = [x for x in raw_taxonomy_df.columns if 'site_all' in x and 'vertical' not in x]
			field_keys = [re.sub(r'.*_all_(\w+)s$', r'\1', field) for field in fields]
			taxonomy_df = self.build_taxonomy_df(df=raw_taxonomy_df, fields=fields, save=save)
		else:
			raise ValueError('No taxonomy data provided.')

		for x in [taxonomy_df, traffic_df]:
			x['wiki_id'] = x['wiki_id'].astype(str)
		df = taxonomy_df.merge(traffic_df, on='wiki_id', how='left')
		self.data['taxonomy'] = taxonomy_df
		self.data['dataset'] = df

		return taxonomy_df, df

	def build_matrices(self,taxonomy_df=None, weights=None, save=False):
		if taxonomy_df is None:
			taxonomy_df = self.data['taxonomy']
		if weights is None:
			weights = self.conf['default_weights']
		feature_matrices = {}
		for field_key, weight in weights.items():
			print(f'Building feature matrix for {field_key} with weight {weight}...')
			feature_matrix = self.build_feature_matrix(df=taxonomy_df[[field_key]], weight=weight)
			if feature_matrix.shape[1] == 0:  
				print(f'No features found for {field_key}, skipping...')
				continue
			feature_matrices[field_key] = feature_matrix
		print('Calculating similarity matrix...')
		similarity_matrix = self.calculate_similarity(feature_matrices, save=save)
		self.data['similarity_matrix'] = similarity_matrix
		return similarity_matrix

	def build_feature_matrix(self,df, weight=1.0):
		"""
		df: a df that has been converted to a dictionary format using taxonomy_to_dict
		weight: float, the weight assigned to each category.
		"""
		def parse_dicts(x):
			if isinstance(x, (int, float)):
				return []
			if isinstance(x, dict):
				return [x]
			if isinstance(x, list):  
				# Ensure all elements in the list are dictionaries
				if all(isinstance(i, dict) for i in x):
					return x
			try:
				o = ast.literal_eval(x)
				if isinstance(o, (list, dict)):
					return o
				else:
					print(f"⚠️ Warning: Parsed value is not list/dict: {o}")
			except Exception:
				pass

			try:
				o = json.loads(x)
				if isinstance(o, (list, dict)):
					return o
				else:
					print(f"⚠️ Warning: JSON parsed value is not list/dict: {o}")
			except Exception:
				pass

			print(f"⚠️ Warning: Unable to parse value: {x}")
			return []

		print(f'df provided to build_feature_matrix: {df.columns} | {df.shape}')
		for col in df.columns:
			print(f'Parsing column {col}...')
			df.loc[:, col] = df[col].apply(lambda x: parse_dicts(x))
		all_categories = set()
		for idx, row in df.iterrows():
			for col in df.columns:
				for cat_dict in row[col]:
					if not cat_dict:  # empty list
						continue
					cat_name = f"{col}:{cat_dict['value']}"
					all_categories.add(cat_name)
		if not all_categories:
			print(f"No categories found for dataframe columns: {df.columns}")
			return self.csr_matrix((len(df), 0))  
		all_categories = sorted(all_categories)  # ascending order
		cat_index = {c: i for i, c in enumerate(all_categories)}
		print(f"Total categories extracted: {len(all_categories)}")

		feature_matrix = self.lil_matrix((len(df), len(all_categories)), dtype=float)

		for i, row in df.iterrows():
			for col in df.columns:
				for cat_dict in row[col]:
					cat_name = f"{col}:{cat_dict['value']}"
					j = cat_index.get(cat_name, None)
					if j is not None:
						feature_matrix[i, j] = cat_dict['confidence']
		feature_matrix = feature_matrix.tocsr()
		row_norms = self.sparse_norm(feature_matrix, axis=1)
		row_norms[row_norms == 0] = 1 
		feature_matrix = feature_matrix.multiply(1 / row_norms.reshape(-1, 1)) * weight

		return feature_matrix

	def calculate_similarity(self,feature_matrices, save=False):

		weighted_matrices = [matrix for key, matrix in feature_matrices.items()]
		combined_feature_matrix = self.hstack(weighted_matrices).tocsr()
		print(f'Combined feature matrix shape: {combined_feature_matrix.shape}')

		similarity_matrix = self.csr_matrix(self.cosine_similarity(combined_feature_matrix))
		print(f'Similarity matrix shape: {similarity_matrix.shape}')
		if save:
			self.save_npz(f'similarity_matrix', similarity_matrix)
		return similarity_matrix

	def apply_scaler(self, feature_matrix): # If exclusively looking at contextual taxonomy data, confidence values are already normalized between 0 and 1
		from sklearn.preprocessing import StandardScaler
		scaler = StandardScaler(with_mean=False)
		feature_matrix_scaled = scaler.fit_transform(feature_matrix)
		return feature_matrix_scaled

	def create_clusters(self, feature_matrix,num_clusters=5, random_state=42):
		from sklearn.cluster import KMeans
		from sklearn.metrics import silhouette_score
		km = KMeans(n_clusters=num_clusters, random_state=random_state)
		cluster_labels = km.fit_predict(feature_matrix)
		sil_score = silhouette_score(feature_matrix, cluster_labels)
		return cluster_labels, sil_score
	
	def get_similar_content(self, input_wiki_id,  df=None, similarity_matrix=None, n=5, alpha=0.8):
		"""
		df: 
		input_wiki_id: the wiki ID for which we want recommendations.
		n: number of recommendations to return.
		alpha: weighting factor that balances similarity vs. popularity (users).
			- alpha=1.0 means only similarity matters.
			- alpha=0.0 means only popularity matters.
		"""
		if df is None or df.empty:
			df = self.data['dataset']
		if similarity_matrix is None:
			similarity_matrix = self.data['similarity_matrix']
		wiki_ids = df['wiki_id'].values.astype(str)
		input_wiki_id = str(input_wiki_id)
		if input_wiki_id not in wiki_ids:
			raise ValueError(f"Wiki ID '{input_wiki_id}' not found.")
		idx = np.where(wiki_ids == input_wiki_id)[0][0]

		sim_scores = similarity_matrix.getrow(idx)

		# get nonzero similarity scores and indices
		nonzero_indices = sim_scores.indices
		nonzero_values = sim_scores.data

		# normalize popularity and combine with similarity 
		popularity = df['users'].fillna(0).astype(float).values
		pop_min, pop_max = popularity.min(), popularity.max()
		if pop_max > pop_min:  # avoid division by zero
			norm_popularity = (popularity - pop_min) / (pop_max - pop_min)
		else:
			norm_popularity = np.zeros_like(popularity)

		# create weighted score
		similarity_score = np.zeros(df.shape[0])
		popularity_score = np.zeros(df.shape[0])
		combined_score = np.zeros(df.shape[0])

		similarity_score[nonzero_indices] = alpha * nonzero_values
		popularity_score[nonzero_indices] = (1 - alpha) * norm_popularity[nonzero_indices]
		combined_score[nonzero_indices] = similarity_score[nonzero_indices] + popularity_score[nonzero_indices]

		recommendations = [
			(wiki_ids[i], similarity_score[i], popularity_score[i], combined_score[i])
			for i in range(len(wiki_ids))
		]
		# create output df
		recommendations_df = pd.DataFrame(recommendations, columns=['wiki_id', 'similarity_score', 'popularity_score', 'combined_score'])
		# filter out input wiki
		recommendations_df = recommendations_df[recommendations_df['wiki_id'] != input_wiki_id]
		recommendations_df = recommendations_df.merge(df[['wiki_id', 'wiki', 'users', 'vertical','is_kid_wiki','is_monetized']], on='wiki_id')
		recommendations_df = recommendations_df[['wiki_id', 'vertical','wiki', 'similarity_score', 'popularity_score', 'combined_score', 'users', 'is_kid_wiki','is_monetized']]
		# filter out junk
		recommendations_df = recommendations_df.sort_values(['combined_score', 'users'], ascending=[False, False])
		recommendations_df = recommendations_df[recommendations_df['users'] > 0]
		recommendations_df = recommendations_df.rename(columns={'wiki_id': 'Wiki ID', 'vertical':'Vertical','wiki': 'Wiki Name', 'similarity_score':'Similarity Score','popularity_score':'Popularity Score','combined_score': 'Combined Score', 'users': 'Users', 'is_kid_wiki': 'Kid Wiki', 'is_monetized': 'Monetized'})
		# top n
		recommendations_df = recommendations_df.head(n)
		return recommendations_df
	
	def analyze(self,wiki_ids=None,weights=None,n=20, alpha=0.8, save=False):
		print(f'****** Analyzing {wiki_ids} *******')
		raw_taxonomy_df = self.data['raw_taxonomy']
		taxonomy_df = self.data['taxonomy']
		traffic_df = self.data['traffic']
		if wiki_ids is None:
			raise ValueError('No wiki IDs provided.')
		if raw_taxonomy_df is None and taxonomy_df is None:
			raise ValueError('No taxonomy data available.')
		if traffic_df is None:
			raise ValueError('No traffic data available.')
		df = self.data['dataset']
		if df is None:
			raise ValueError('No dataset available.')
		similarity_matrix = self.build_matrices(taxonomy_df=taxonomy_df, weights=weights, save=save)
		results = {}
		for wiki_id in wiki_ids:
			wiki_name = df[df['wiki_id'].astype(str) == str(wiki_id)]['wiki'].iloc[0]
			wiki_lang = df[df['wiki_id'].astype(str) == str(wiki_id)]['lang'].iloc[0]
			wiki_key = f'{wiki_id} | {wiki_name} ({wiki_lang})'
			print(f'Getting similar content for wiki ID {wiki_key}...')
			related = self.get_similar_content(df=df, input_wiki_id=wiki_id, similarity_matrix=similarity_matrix, n=n, alpha=alpha)
			results[wiki_key] = related
		return results

class WikiMetadata:
	def __init__(self, session_id=None):
		print('Initalizing WikiMetadata...')
		self.init_time = datetime.now()
		self.conf = {
			's3_path': 'wiki_metadata/',
			'cache_path': 'wm:cache',
			'prefix': 'wm',
		}
		self.data = {
			'wiki_metadata': None,
			'wiki_traffic_30_days': None,
			'pages_traffic_30_days': None,
		}
		self.queries = {
			'configs': """
				with configs as (
					select
						anchor_date as end_date,
						date_add('day', -29, anchor_date) as start_date,
						.60 as confidence_threshold
					from (
						select
							cast('[_Latest Date_]' as date) as anchor_date
						)
				)
			""",
			'ctes': r"""
				wiki_id_traffic as (
					select
						wiki_id,
						count(distinct analytics_id) as users,
						count(distinct session_id) as sessions,
						sum(pageviews) as pageviews
					from AwsDataCatalog.analytics.example_table
					where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs ) and ( select end_date from configs )
					and lower(brand) = 'acme'
					and lower(platform) = 'web'
					and wiki_id > 0
					and wiki_id is not null
					and cast(wiki_id as varchar) in ('[_Wiki List_]')
					group by 1
					order by pageviews desc
					
				),
				wiki_metadata as (
					select
						wiki_id,
						split_part(domain,'/',1) as wiki_group,
						domain,
						created_at,
						is_kid_wiki,
						is_monetized,
						vertical_name,
						lang,
						founding_user_id,
						user_name as founding_user_name
					from analytics.dimension_table a left join analytics.dimension_users b on a.founding_user_id = b.user_id
					where site='acme'
				),
				wikis_complete as (
					select
						a.wiki_id,
						wiki_group,
						domain,
						created_at,
						is_kid_wiki,
						is_monetized,
						vertical_name,
						lang,
						founding_user_id,
						founding_user_name,
						users as users_30_days,
						sessions as sessions_30_days,
						pageviews as pageviews_30_days
					from (
						(select * from wiki_id_traffic) as a
						left join
						(select * from wiki_metadata) as b
						on a.wiki_id = b.wiki_id
					)
					order by pageviews_30_days desc
				),
				page_breakdown as (
					select
						wiki_id,
						regexp_replace(regexp_replace(page_url, '\?.+=?+$',''), '#*+$', '') as page,
						count(distinct analytics_id) as users_30_days,
						count(distinct session_id) as sessions_30_days,
						sum(pageviews) as pageviews_30_days
					from AwsDataCatalog.analytics.example_table
					where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs ) and ( select end_date from configs )
					and lower(brand) = 'acme'
					and lower(platform) = 'web'
					and wiki_id > 0
					and wiki_id is not null
					and page_url not like '%turbopages.org%'
					and cast(wiki_id as varchar) in ('[_Wiki List_]')
					group by 1,2
					order by pageviews_30_days desc
					
				),
				top_wiki_id as (
					select
						wiki_group,
						wiki_id,
						pageviews
					from (
						select
							wiki_group,
							wiki_id,
							pageviews,
							row_number() over (partition by wiki_group order by pageviews desc) as rn
						from(
							select
								split_part(split_part(page_url,'://',2),'/',1) as wiki_group,
								wiki_id,
								sum(pageviews) as pageviews
							from AwsDataCatalog.analytics.example_table
							where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs ) and ( select end_date from configs )
							and lower(brand) = 'acme'
							and lower(platform) = 'web'
							and split_part(split_part(page_url,'://',2),'/',1) in ('[_Wiki Groups_]')
							group by 1,2
						)
					)
					where rn = 1
					order by pageviews desc
				),
				tax as (
					select
					content_ids['article_id'] as article_id,
					content_ids['wiki_id'] as wiki_id,
					url,
					transform(site_all_verticals, x -> x[2])[1] as main_vertical,
					array_join(transform(site_all_verticals, x -> x[2]),',') as all_verticals,
					site_all_franchises as main_franchise,
					array_join(transform(page_main_entity_type, x -> x[2]),',') as main_entity_type,
					array_join(transform(page_all_installment_ids, x -> x[2]),',') as all_installment_ids,
					array_join(transform(page_all_installment_types, x -> x[2]),',') as all_installment_types,
					array_join(transform(page_all_installment_titles, x -> x[2]),',') as all_installment_titles,
					array_join(transform(site_all_genres, x -> x[1]),',') as all_genres_con,
					array_join(transform(site_all_genres, x -> x[2]),',') as all_genres,
					array_join(transform(site_all_subgenres, x -> x[1]),',') as all_subgenres_con,
					array_join(transform(site_all_subgenres, x -> x[2]),',') as all_subgenres,
					array_join(transform(page_all_platforms, x -> x[2]),',') as all_platforms,
					array_join(transform(site_all_themes, x -> x[1]),',') as all_themes_con,
					array_join(transform(site_all_themes, x -> x[2]),',') as all_themes
					from my_database.my_table
				),
				amp as (
					select
						*
					from AwsDataCatalog.analytics.example_table
					where lower(brand) = 'acme'
					and lower(platform) = 'web'
					and cast(wiki_id as varchar) in ('[_Wiki List_]')
				),
				unity as (
					select 
						t1.wiki_id,
						t1.analytics_id,
						t1.pageviews,
						concat(cast(t1.wiki_id as varchar),'-',t1.content_id) as wiki_article_id,
						t2.all_themes,
						t2.all_themes_con,
						t2.all_genres,
						t2.all_genres_con,
						t2.all_subgenres,
						t2.all_subgenres_con
					from (
					(select * from amp) as t1
					full outer join
					(select * from tax) as t2
					on cast(t1.wiki_id as varchar) = cast(t2.wiki_id as varchar)
					and cast(t1.content_id as varchar) = cast(t2.article_id as varchar)
					)
					where cast(concat(year, '-', month, '-', day) as date) between (select start_date from configs) and (select end_date from configs)
				),
				raw_tags as (
					select
						wiki_id,
						all_themes as all_themes,
						all_themes_con as all_themes_con,
						all_genres as all_genres,
						all_genres_con as all_genres_con,
						all_subgenres as all_subgenres,
						all_subgenres_con as all_subgenres_con,
						analytics_id,
						pageviews,
						wiki_article_id
					from unity
					where all_themes is not null
					and all_genres is not null
					and all_subgenres is not null
				),
				exploded_themes AS (
					select
						wiki_id,
						theme,
						confidence,
						count(distinct analytics_id) as users,
						sum(pageviews) as pageviews,
						count(distinct wiki_article_id) as page_count
					from (
						select
							trim(theme_value) as theme,
							trim(confidence_value) as confidence,
							wiki_id,
							analytics_id,
							pageviews,
							wiki_article_id
						from (
							select
								split(all_themes, ',') as themes,
								split(all_themes_con, ',') as confidence_scores,
								wiki_id,
								analytics_id,
								pageviews,
								wiki_article_id
							from raw_tags
						) cross join unnest(themes,confidence_scores) as t (theme_value,confidence_value)
					)
					where try(cast(confidence as double) >= (select confidence_threshold from configs))
					group by 1,2,3
					order by users desc
				),
				exploded_genres AS (
					select
						wiki_id,
						genre,
						confidence,
						count(distinct analytics_id) as users,
						sum(pageviews) as pageviews,
						count(distinct wiki_article_id) as page_count
					from (
						select
							trim(genre_value) as genre,
							trim(confidence_value) as confidence,
							wiki_id,
							analytics_id,
							pageviews,
							wiki_article_id
						from (
							select
								split(all_genres, ',') as genres,
								split(all_genres_con, ',') as confidence_scores,
								wiki_id,
								analytics_id,
								pageviews,
								wiki_article_id
							from raw_tags
						) cross join unnest(genres,confidence_scores) as t (genre_value,confidence_value)
					)
					where try(cast(confidence as double) >= (select confidence_threshold from configs))
					group by 1,2,3
					order by users desc
				),
				exploded_subgenres AS (
					select
						wiki_id,
						subgenre,
						confidence,
						count(distinct analytics_id) as users,
						sum(pageviews) as pageviews,
						count(distinct wiki_article_id) as page_count
					from (
						select
							trim(subgenre_value) as subgenre,
							trim(confidence_value) as confidence,
							wiki_id,
							analytics_id,
							pageviews,
							wiki_article_id
						from (
							select
								split(all_subgenres, ',') as subgenres,
								split(all_subgenres_con, ',') as confidence_scores,
								wiki_id,
								analytics_id,
								pageviews,
								wiki_article_id
							from raw_tags
						) cross join unnest(subgenres,confidence_scores) as t (subgenre_value,confidence_value)
					)
					where try(cast(confidence as double) >= (select confidence_threshold from configs))
					group by 1,2,3
					order by users desc
				)
				
			""",
			'select_statements': {
				'wiki_traffic_30_days': 'select * from wikis_complete',
				'pages_traffic_30_days': 'select * from page_breakdown',
				'tax_themes': 'select * from exploded_themes',
				'tax_genres': 'select * from exploded_genres',
				'tax_subgenres': 'select * from exploded_subgenres',
			},
			'utility_statements': {
				'top_wiki_ids': 'select * from top_wiki_id',
			},
		}
		self.refresh_data()
	def refresh_data(self):
		print(f'WM: Fetching data for WikiMetadata')

		r = redis.from_url(REDISCLOUD_URL)
		s3 = AmazonS3Connector()
		try:
			self.data['wiki_metadata'] = pd.DataFrame(json.loads(r.get(f'{self.conf["cache_path"]}:wiki_metadata')))
			print('Loaded wiki_metadata from cache')
		except Exception as e:
			try:
				print(f'No wiki_metadata cached, loading from s3')
				self.data['wiki_metadata'] = s3.get_from_s3(f'{self.conf['s3_path']}wiki_metadata.csv')
				print('caching to redis')
				r.set(f'{self.conf["cache_path"]}:wiki_metadata', json.dumps(self.data['wiki_metadata'].to_dict('records')))
				# expire after 30 days
				r.expire(f'{self.conf["cache_path"]}:wiki_metadata', self.conf['cache_ttl'])
			except Exception as e:
				print(f'Unable to get wiki_metadata df: {e}')
				pass
			pass

		return self
	
	def store_result(self,filename, data):
		print('Storing result: ')
		print(f'type {type(data)}')
		print('Cols:')
		print(data.columns)
		fpath = os.path.join('.cache',self.conf['s3_path'],filename)
		s3 = AmazonS3Connector()
		try:
			print(f'Saving: {fpath}')
			data.to_csv(fpath, index=False)
			s3.save_to_s3(fpath, self.conf['s3_path'])
		except Exception as e:
			try:
				d = os.path.join('.cache',self.conf['s3_path'])
				print(f'trying to make dir: {d}')
				os.mkdir(d)
				data.to_csv(fpath, index=False)
				s3.save_to_s3(fpath, self.conf['s3_path'])
			except Exception as ee:
				print(f'Error creating local cache folder: {ee}')
			print(f'Error saving to s3 {fpath}: {e}')

	def refresh_wiki_ids(self, wiki_ids):
		if len(wiki_ids) == 0:
			print('No wiki_ids provided, nothing to return')
			return None
		else:
			print(f'Refreshing metadata for [{len(wiki_ids)}] wiki_ids')
			print(wiki_ids[:5])
			self.refresh_data()
			metadata = self.data['wiki_metadata']
			int_stats = self.get_internal_stats(wiki_ids)

			df = int_stats[0]

			new_records = []
			for i in range(len(df)):
				data = df.iloc[i].to_dict()
				pg_df = int_stats[1][int_stats[1]['wiki_id'] == data['wiki_id']]
				data['page_count'] = len(pg_df) if len(pg_df) > 0 else None
				self.store_result(f'pages_{data['wiki_id']}.csv',pg_df.head(1000))

				try:
					data['poster_url'] = self.get_wiki_poster(f'https://{data['domain']}')
				except Exception as e:
					print(f'Error getting poster URL: {e}')
					data['poster'] = None
					pass
				try:
					ai_data = self.ai_analyze_wiki(f'https://{data['domain']}',pg_df['page'].head(50).to_list())
					for k,v in ai_data.items():
						data[f'ai_{k}'] = ai_data[k]
				except Exception as e:
					print(f'Error getting summary from ChatGPT: {e}')
					pass
				try:
					related_wikis = self.get_related_wikis(data['wiki_id'])
					print(f'got related wikis: {type(related_wikis)} | {related_wikis}')
					data['crossover_wikis'] = ','.join(related_wikis)
					print(f'going to use: {data["crossover_wikis"]}')
				except Exception as e:
					print(f'Error getting crossover data from TaxonomyRelationships: {e}')
					data['crossover_wikis'] = None
					pass

				tax_props = ['taxonomy_themes','taxonomy_genres','taxonomy_subgenres']
				for i in range(len(tax_props)): 
					tax_df = int_stats[i+2][int_stats[i+2]['wiki_id'] == data['wiki_id']]
					col_name = tax_df.columns[1]
					tax_df['json_string'] = tax_df.apply(lambda x: json.dumps({x[col_name]: x['confidence']}),axis=1)
					try:
						data[tax_props[i]] = ','.join(tax_df['json_string'].to_list())
					except Exception as e:
						print(f'Error getting taxonomy {tax_props[i]} data: {e}')
						data[tax_props[i]] = None
						pass
				data['last_refreshed'] = datetime.now().isoformat()
				new_records.append(data)
			new_df = pd.DataFrame(new_records)

			old_df = metadata[metadata['wiki_id'].astype(str).isin(new_df['wiki_id'].astype(str)) == False]
			out_df = pd.concat([new_df,old_df],ignore_index=True)
			self.data['wiki_metadata'] = out_df
			print('Saving to redis cache')
			r = redis.from_url(REDISCLOUD_URL)
			r.set(f'{self.conf["cache_path"]}:wiki_metadata', json.dumps(out_df.to_dict('records')))
			r.expire(f'{self.conf["cache_path"]}:wiki_metadata', 60*60*24*30)
			print('Saving to s3')
			fpath = os.path.join('.cache',self.conf['s3_path'],'wiki_metadata.csv')
			s3 = AmazonS3Connector()
			try:
				print(f'Saving: {fpath}')
				out_df.to_csv(fpath, index=False)
				s3.save_to_s3(fpath, self.conf['s3_path'])
			except Exception as e:
				try:
					d = os.path.join('.cache',self.conf['s3_path'])
					print(f'trying to make dir: {d}')
					os.mkdir(d)
					out_df.to_csv(fpath, index=False)
					s3.save_to_s3(fpath, self.conf['s3_path'])
				except Exception as ee:
					print(f'Error creating local cache folder: {ee}')
				print(f'Error saving to s3 {fpath}: {e}')
			print('out_df:')
			print(out_df)
			return out_df

	def get_wiki_ids(self, wiki_ids):
		if len(wiki_ids) == 0:
			print('No wiki_ids provided, nothing to return')
			return None
		else:
			print(f'Getting metadata for [{len(wiki_ids)}] wiki_ids')
			print(wiki_ids[:5])
			self.refresh_data()
			metadata = self.data['wiki_metadata']
			return metadata[metadata['wiki_id'].astype(str).isin(wiki_ids)]

	def get_wiki_ids_from_groups(self,wiki_groups):
		print(f'Getting top wiki_ids for [{len(wiki_groups)}] groups')
		wiki_group_list = ','.join(f"'{str(x)}'" for x in wiki_groups)

		a = self.queries['utility_statements']['top_wiki_ids']
		q = self.queries['configs'].replace('[_Latest Date_]',b.calendar.latest_date.strftime('%Y-%m-%d'))
		q = ',\n'.join([q,self.queries['ctes'].replace("'[_Wiki Groups_]'",wiki_group_list)])
		q = '\n'.join([q,a])
		qa = QueryAthena('AwsDataCatalog','analytics',q)
		df = qa.run_query()
		df['wiki_id'] = df['wiki_id'].fillna(0).astype(int).apply(str)

		return df

	def get_internal_stats(self,wiki_ids):
		print(f'pulling internal data from DW and analytics for [{len(wiki_ids)}] wiki_ids')
		run_queries = True
		b = bi.BiUtils()
		wiki_list = ','.join(f"'{str(x)}'" for x in wiki_ids)

		results = []
		for k,v in self.queries['select_statements'].items():
			fname = os.path.join('.cache',self.conf['s3_path'],f'{k}.csv')
			q = self.queries['configs'].replace('[_Latest Date_]',b.calendar.latest_date.strftime('%Y-%m-%d'))
			q = ',\n'.join([q,self.queries['ctes'].replace("'[_Wiki List_]'",wiki_list)])
			q = '\n'.join([q,v])
			if run_queries:
				qa = QueryAthena('AwsDataCatalog','analytics',q)
				df = qa.run_query()
			df['wiki_id'] = df['wiki_id'].apply(str)
			try:
				df = df.sort_values(by='pageviews_30_days',ascending=False)
			except KeyError as e:
				df = df.sort_values(by=['confidence','users'],ascending=[False,False])
				pass
			self.data[k] = df
			results.append(df)
			# self.store_result(f'{k}.csv',df)
		return results

	def get_related_wikis(self,wiki_id):
		print(f'wm: getting wikis related to {wiki_id}')
		tax = TaxonomyRelationships()
		related_wikis_dict = tax.analyze(wiki_ids=[wiki_id],n=20)
		print(f'********got related wikis dict: {type(related_wikis_dict)}')
		for k,related_wikis_df in related_wikis_dict.items():
			print(f'related_wikis df: {related_wikis_df.shape} | {related_wikis_df.columns}')
		# ['Wiki ID', 'Vertical', 'Wiki Name', 'Similarity Score', 'Popularity Score', 'Combined Score', 'Users', 'Kid Wiki', 'Monetized']
			related_wikis_df['key'] = related_wikis_df['Wiki Name'].astype(str) + ' [' + related_wikis_df['Wiki ID'].astype(str) + '] | ' + related_wikis_df['Combined Score'].apply(lambda x: f'{x:.0%}')
			# Get top 25 key values as a list of strings
			related_list = related_wikis_df['key'].head(25).to_list()

			
			#related_list = related_wikis_df['key'].head(25).to_list()
			print(f'related list: {type(related_list)} | {len(related_list)}')
			for x in related_list:
				print(f'{type(x)} | {x}')
			break
		return related_list

	def get_wiki_poster(self,wiki_url):
		time.sleep(2)
		with requests.Session() as r:
			headers = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Vivaldi/6.9.3447.37'}
			api_response = r.get(wiki_url,headers=headers)
			s = bs4.BeautifulSoup(api_response.text,'html.parser')
			meta_tags = s.find_all('meta', {'property': 'og:image'})
			image_url = None

			if meta_tags:
				for meta_tag in meta_tags:
					if 'content' in meta_tag.attrs and meta_tag['content'].startswith('https://static.wikia.nocookie.net'):
						image_url = meta_tag['content']
						break
			else:
				content_container = s.find('div', {'class': 'lcs-container'})
				if content_container:
					first_image = content_container.find('img')
					if first_image and 'src' in first_image.attrs:
						image_url = first_image['src']
		return image_url
	
	def ai_analyze_wiki(self, wiki_domain, pg_df):
		time.sleep(2)
		client = OpenAI(api_key=OPENAI_API_KEY)

		class MediaSummary(BaseModel):
			summary: str
			news: str
			franchise: str
			roblox: bool
			illegal: bool
			hate: bool
			violence: bool
			sexual: bool

		# combined_prompt = ''

		# for wiki_domain in wiki_domains:
		# 	page_urls = [url for url in pg_df['page'] if wiki_domain in url]
		# 	input_val = f'https://{wiki_domain}'
		# 	combined_prompt += f'\nDomain: {input_val}\nPages: {page_urls}\n'


		response = client.beta.chat.completions.parse(
			#model="gpt-4o-2024-08-06",
			model='gpt-4o-mini',
			messages=[
				{
					"role": "system",
					"content": """
					You are an expert on pop culture, entertainment, video games, literature, music, and anime.
					You write brief reviews of new media, providing a summary of what the media is and who might be interested in it.
					You also mention any relevant recent news about the media that may be driving interest in the media.
					You also indicate what media franchises (for example Star Wars, or the Marvel Cinematic Universe) the media is associated with.
					You also indicate if the media is a Roblox game or is associated with Roblox.
					You also indicate if the media is or is not associated with any of the following types of content:
					1) illegal, harmful
					2) racist, misogynist, or hateful
					3) violence
					4) sexual
					Your output must be in a JSON format that matches the schema provided.
					"""
				},
				{
					"role": "user",
					"content": f"Please write a review of the media topic related to this domain and page URLs:{wiki_domain} {pg_df}"
				}
			],
			response_format=MediaSummary,
		)

		parsed_response = response.choices[0].message.parsed.dict()

		# Process and structure the results
		flags = {
			'roblox': parsed_response['roblox'],
			'illegal': parsed_response['illegal'],
			'hate': parsed_response['hate'],
			'violence': parsed_response['violence'],
			'sexual': parsed_response['sexual'],
		}

		result = {
			'summary': parsed_response['summary'],
			'news': parsed_response['news'],
			'franchise': parsed_response['franchise'],
			'flags': flags,
		}
		return result
	
	def render_wiki_detail_modal(self,wiki_id):
		import dash_bootstrap_components as dbc
		from dash import html, dcc
		import plotly.io as pio
		import plotly.graph_objects as go
		import plotly.express as px
		import pandas as pd
		import numpy as np
		from datetime import date, datetime
		print(f'WM: rendering wiki detail modal for: {wiki_id}')
		# bgcolor = self.styles['color_sequence'][0]
		bgcolor = '#ffffff'
		b = bi.BiUtils()
		df = None
		r = redis.from_url(REDISCLOUD_URL)
		df = self.data['wiki_metadata']
		
		tp_df = None
		try:
			tp_df = pd.DataFrame(json.loads(r.get(f'{self.conf['cache_path']}:pages:{wiki_id}')))
			print('using cached pages metadata')
		except Exception as e:
			print('no cached pages metadata')
			s3 = AmazonS3Connector()
			tp_df = s3.get_from_s3(f'wiki_metadata/pages_{str(wiki_id)}.csv')
			print(f'caching to redis with key: wiki_metadata:{wiki_id}')
			r.set(f'{self.conf['cache_path']}:pages:{wiki_id}',json.dumps(tp_df.to_dict('records')))
			r.expire(f'{self.conf['cache_path']}:pages:{wiki_id}', 60*60*24*7)
			pass	


		df = df[df['wiki_id'].apply(str) == str(wiki_id)]
		df['pv_per_session_30_days'] = df['pageviews_30_days'] / df['sessions_30_days']
		df['sessions_per_user_30_days'] = df['sessions_30_days'] / df['users_30_days']
		wiki_group = df['wiki_group'].iloc[0]
		lang = df['lang'].iloc[0]
		domain = df['domain'].iloc[0]
		is_kid_wiki = df['is_kid_wiki'].iloc[0]
		is_monetized = df['is_monetized'].iloc[0]
		last_refreshed = df['last_refreshed'].iloc[0]
		poster_url = 'dash_app/assets/images/monetization-monitor.webp'
		poster_url = df['poster_url'].iloc[0]
		poster_url = str(poster_url).split('?')[0] + f'?cb={time.time()}'
		poster = dbc.Stack([
			html.Img(src=poster_url,style={'width':'100%'},referrerPolicy='no-referrer')
		],gap=3,className='align-items-start justify-content-center',style={'flex':'1'})
		
		ai_flags = {
			'roblox': None,
			'illegal':None,
			'hate': None,
			'violence': None,
			'sexual': None,
		}

		try:
			ai_flags = ast.literal_eval(df['ai_flags'].iloc[0])
		except Exception as e:
			print(f'Error getting AI flags: {e}')
			pass

		ai_summary = dbc.Stack([
			dbc.Stack([
				html.H5([html.I(className='bi bi-stars'),' Summary'],className='section-header p-1 px-2'),
				html.Span(df['ai_summary'].iloc[0]),
			],gap=3),
			dbc.Stack([
				html.H5([html.I(className='bi bi-stars'),' News'],className='section-header p-1 px-2'),
				html.Span(df['ai_news']),
			],gap=3),
		],gap=1,className='align-items-center justify-content-center')

	
		body_fields = ['wiki_id','wiki_group','domain','lang','vertical_name','created_at','founding_user_name']
		body_field_defs = [
			'Unique wiki ID from MediaWiki.  Each language version of a wiki has its own unique wiki_id, which is standard across all acme datasets.',
			'Equivalent to "Wiki Name" in analytics; rollup of all language versions of a wiki',
			None,
			'Language the content is written in',
			'Vertical (from MediaWiki via analytics)',
			None,
			None,
		]
		kpi_fields = ['users_30_days','pageviews_30_days','pv_per_session_30_days','sessions_per_user_30_days','page_count']
		kpi_field_titles = ['Avg. Monthly Users','Avg. Monthly Pageviews','Pageviews / Session', 'Sessions / User','Total Pages']
		kpi_field_defs = [
			'Average number of unique users expected over a 30 day period',
			'Average number of pageviews expected over a 30 day period',
			'Average number of pages viewed per session over a 30 day period',
			'Average number of times each user is expected to visit the site over a 30 day period',
			'Total number of distinct URLs, excluding variations based on query strings/anchors ("?","&", "#")',
		]
		vertical_icon_url = f'assets/images/{b.styles['icon_map'].get(df['vertical_name'].apply(str).iloc[0].lower())}'
		is_monetized_badge = dbc.Badge('Monetized',color='success') if is_monetized == 1 else dbc.Badge('Unmonetized',color='warning',style={'color':'black'})

		stats_table = []
		kpi_boxes = []
		for i in range(len(kpi_fields)):
			title = kpi_field_titles[i]
			popover=None
			if kpi_field_defs[i] != None:
				popover = dbc.Popover([
						dbc.PopoverBody([
							dbc.Stack([
								dbc.Stack([
									html.Span(kpi_field_defs[i])						
								],gap=3,style={'flex':'1'}),
							],direction='horizontal',gap=3, className='align-items-center justify-content-center'),
						]),
					],
					target=f'field-def-{kpi_fields[i]}',
					trigger='hover',
					className='help-icon'
				)
			val = df[kpi_fields[i]].iloc[0]
			val = auto_num_format(val)

			box = dbc.Card([
				dbc.CardHeader([
					dbc.Stack([
						html.Span(title,className='side-title-text pl-1',style={'font-size':'1.1rem','font-weight':'bold','text-wrap':'nowrap','overflow':'scroll'}),
						popover,
					],gap=3,className='align-items-center justify-content-center')
				],id=f'field-def-{kpi_fields[i]}',),
				dbc.CardBody([
					dbc.Stack([
						html.Span(val,style={'font-weight':'bold','font-size':'1.5rem'})
					],gap=3,className='align-items-center justify-content-center')
				]),
			],style={'flex':'1','align-self':'stretch'})
			kpi_boxes.append(box)

		traffic_box = dbc.Stack([
			html.H5('Key Stats',className='section-header p-1 px-2',),
			dbc.Stack(kpi_boxes,direction='horizontal',gap=3,className='align-items-center justify-content-between')
			],gap=3,className='align-items-center justify-content-center'
		)


		for i in range(len(body_fields)):
			title = body_fields[i].replace('_',' ').title()
			popover=None
			if body_field_defs[i] != None:
				popover = dbc.Popover([
					dbc.PopoverHeader(title,style={'font-weight':'bold'}),
						dbc.PopoverBody([
							dbc.Stack([
								dbc.Stack([
									html.Span(body_field_defs[i])						
								],gap=3,style={'flex':'1'}),
							],direction='horizontal',gap=3, className='align-items-center justify-content-center'),
						]),
					],
					target=f'field-def-{body_fields[i]}',
					trigger='hover',
					className='help-icon'
				)
			val = df[body_fields[i]].iloc[0]
			if body_fields[i] == 'vertical_name':
				img = html.Img(src=vertical_icon_url,style={'max-height':'5vh'})
				try:
					if df['wiki_group'].iloc[0] in ['memory-alpha.acme.com']:
						img = html.Img(src='assets/images/retro_spock_hand.png')
					elif df['wiki_group'].iloc[0] in ['taylorswift.acme.com']:
						img = html.Img(src='assets/images/retro_taylor.png')
				except Exception as e:
					print(f'error checking special icon cases: {e}')
					pass
				val = dbc.Stack([img,val],direction='horizontal',gap=2,className='align-items-center justify-content-start')
			if body_fields[i] in ['created_at']:
				val = f'{datetime.fromisoformat(val):%b %d %Y %H:%M}'
			# if body_fields[i] in ['page_count','users_30_days','sessions_30_days','pageviews_30_days']:
			# 	b = bi()
			# 	val = auto_num_format(val)

			row = dbc.Card([
				dbc.CardBody([
					dbc.Stack([
						dbc.Stack([
							dbc.Stack([
								html.Span(title,className='side-title-text pl-1',style={'font-size':'1.1rem'}),
								#html.Span(secondary_text[i],className='side-title-text-secondary'),
							],className='d-flex justify-content-center align-items-start'),
							#html.Span(f'Source: {source[i]}',className='side-title-text-source'),
							popover,
						],className='side-title d-flex justify-content-between align-items-center p-1',id=f'field-def-{body_fields[i]}',style={'flex':'1','background-color':bgcolor,'width':'100%'}),
						dbc.Stack(val,className='d-flex justify-content-start align-items-start p-1',style={'flex':'2','font-size':'1.1rem'}),
					],direction='horizontal'),
				],style={'padding':'0'}),
			],className='kpi-list-card')
			stats_table.append(row)


		kid_wiki_box = dbc.Card([
			dbc.CardBody([
				dbc.Stack([
					dbc.Stack([
						dbc.Stack([
							html.Span('Kid Wiki',className='side-title-text'),
						],className='d-flex justify-content-center align-items-start'),
					],className='side-title d-flex justify-content-between align-items-center p-1',style={'flex':'1','background-color':self.styles['color_sequence'][0]}),
					dbc.Stack([
						dbc.Badge('Kid Wiki',color='danger') if is_kid_wiki == 1 else None
					],gap=0,className='align-items-start justify-content-center p-1',style={'flex':'2'}),
				],direction='horizontal'),
			],style={'padding':'0'}),
		],className='kpi-list-card')
		stats_table.append(kid_wiki_box)

		is_monetized_box = dbc.Card([
			dbc.CardBody([
				dbc.Stack([
					dbc.Stack([
						dbc.Stack([
							html.Span('Monetized',className='side-title-text'),
						],className='d-flex justify-content-center align-items-start'),
					],className='side-title d-flex justify-content-between align-items-center p-1',style={'flex':'1','background-color':self.styles['color_sequence'][0]}),
					dbc.Stack([
						is_monetized_badge
					],gap=0,className='align-items-start justify-content-center p-1',style={'flex':'2'}),
				],direction='horizontal'),
			],style={'padding':'0'}),
		],className='kpi-list-card')
		stats_table.append(is_monetized_box)

		types = ['roblox']
		badges = []
		for i in range(len(types)):
			if ai_flags[types[i]] == None:
				badges.append(dbc.Badge('TBD'))
				break
			badge = dbc.Badge(types[i].title(),className=f'ai-badge-{i}') if ai_flags[types[i]] else None
			badges.append(badge)
		flags_box = dbc.Card([
			dbc.CardBody([
				dbc.Stack([
					dbc.Stack([
						dbc.Stack([
							html.Span([html.I(className='bi bi-stars'),' Flags'],className='side-title-text'),
						],className='d-flex justify-content-center align-items-start'),
					],className='side-title d-flex justify-content-between align-items-center p-1',style={'flex':'1','background-color':self.styles['color_sequence'][0]}),
					dbc.Stack(badges,gap=0,className='align-items-start justify-content-center p-1',style={'flex':'2'}),
				],direction='horizontal'),
			],style={'padding':'0'}),
		],className='kpi-list-card')
		stats_table.append(flags_box)

		types = ['illegal','hate','violence','sexual']
		badges = []
		for i in range(len(types)):
			if ai_flags[types[i]] == None:
				badges.append(dbc.Badge('TBD'))
				break
			badge = dbc.Badge(types[i].title(),className=f'ai-badge-{i}') if ai_flags[types[i]] else None
			if badge != None:
				badges.append(badge)
		if badges == []:
			badges.append(dbc.Badge('None Detected'))
		sensitive_content_box = dbc.Card([
			dbc.CardBody([
				dbc.Stack([
					dbc.Stack([
						dbc.Stack([
							html.Span([html.I(className='bi bi-stars'),'Sensitive Content'],className='side-title-text'),
						],className='d-flex justify-content-center align-items-start'),
					],className='side-title d-flex justify-content-between align-items-center p-1',style={'flex':'1','background-color':self.styles['color_sequence'][0]}),
					dbc.Stack(badges,gap=0,className='align-items-start justify-content-center p-1',style={'flex':'2'}),
				],direction='horizontal'),
			],style={'padding':'0'}),
		],className='kpi-list-card')
		stats_table.append(sensitive_content_box)
		


		#### Top Pages ############################################

		tp_df['wiki_id'] = tp_df['wiki_id'].apply(str)
		tp_df = tp_df[['page','users_30_days','pageviews_30_days']].sort_values(by=['pageviews_30_days'], ascending=False)

		stats_table = dbc.Stack(stats_table,gap=0,className='align-items-center justify-content-start',style={'flex':'1'})
		stats_table = dbc.Stack([
			stats_table
		],gap=3,className='align-items-start justify-content-center',style={'flex':'1'})
		
		top_pages = dbc.Stack([
			html.H5('Top Pages',className='section-header p-1 px-2'),
				b.create_display_table(
					tp_df,
					col_sizes=[5 if x.lower() == 'page' else 1 for x in tp_df.columns],
					table_id='top-pages',
					row_ids=tp_df['page'],
					href_vals = tp_df['page'],
					cell_bars=[True if str(x).lower() in ['users_30_days','sessions_30_days','pageviews_30_days'] else False for x in tp_df.columns],
				),
			],gap=3)

		try:
			tax_cards = []
			for tax in ['taxonomy_themes','taxonomy_genres','taxonomy_subgenres']:
				tax_badges = []
				taxonomy_value = df[tax].fillna(0).iloc[0]
				if taxonomy_value != 0:
					print(f'tax not null for {tax}')
					print(f'df[tax].fillna(0).iloc[0]: {taxonomy_value}')
					tax_vals = ast.literal_eval(taxonomy_value)
					print(f'tax_vals after ast.literal_eval: {tax_vals} | {type(tax_vals)}') # as tuples for multiple or string for single item
					if type(tax_vals) != list and type(tax_vals) != tuple:
						tax_vals = [tax_vals]
					for i in range(len(tax_vals)):
						if type(tax_vals[i]) != dict:
							tax_vals[i] = json.loads(tax_vals[i])
						for k,v in tax_vals[i].items():
							#d_val = f'{k} ({v:.0%})'
							d_val = f'{k}'
							badge = dbc.Badge([
								d_val,
								dbc.Popover([
									html.Span(f'Confidence: {v:.0%}',style={'font-weight':'bold'})
									],placement='top',target=f'{tax}-badge-{i}',trigger='hover',className='p-2',style={'border-radius':'.5rem', 'border':'1px darkgray solid'}
								),
								],id=f'{tax}-badge-{i}', className='p-2 bg-danger')
							tax_badges.append(badge)
				else:
					badge = dbc.Badge('TBD',className='p-2')
					tax_badges.append(badge)
				tax_card = dbc.Stack([
					html.H5(tax.replace('_',' ').title(),className='section-header p-1 px-2',),
					dbc.Stack(tax_badges,direction='horizontal',gap=1,className='align-items-center justify-content-start')
				],gap=3,className='align-items-center justify-content-center')
				tax_cards.append(tax_card)
		except Exception as e:
			print(f'Error creating taxonomy cards: {e}')
			raise e

		crossover_badges = []
		has_crossover = False
		cw = None
		try:
			has_crossover = df['crossover_wikis'].fillna(0).iloc[0] != 0
			cw = ast.literal_eval(df['crossover_wikis'].iloc[0])
		except Exception as e:
			print(f'Error checking for crossover wikis: {e}')
			pass
	# def set_google_credentials(self):
	# 	r = redis.from_url(REDISCLOUD_URL)
	# 	creds = json.loads(r.get('google-credentials'))
	# 	self.conf['google_credentials'] = Credentials(**creds)
			cw = ast.literal_eval(df['crossover_wikis'].iloc[0])
			for i in range(len(cw)):
				for k,v in cw[i].items():
					print(f'doing: {k} | {v}')
					# vertical = None
					# try:
					# 	vertical = df[df['wiki_group'] == k]['vertical_name'].iloc[0]
					# 	print(f'vertical found: {vertical}')
					# except Exception as e:
					# 	print('did not find a vertical value')
					# 	pass
					# icon = html.Img(src=f'assets/images/{icon_map.get(vertical,'retro_sunset.png')}',style={'height':'20px'})
					d_val = html.A(f'{k}',href=f'https://{k}',target='_blank',style={'color':'white'})
					badge = dbc.Badge([
						#dbc.Stack([icon,d_val],direction='horizontal',gap='2',className='align-items-center justify-content-start'),
						dbc.Stack([d_val],direction='horizontal',gap='2',className='align-items-center justify-content-start'),
						dbc.Popover([
							html.Span(f'Crossover Score: {v:,.0f}',style={'font-weight':'bold'})
							],placement='top',target=f'crossover-wikis-badge-{i}',trigger='hover',className='p-2',style={'border-radius':'.5rem', 'border':'1px darkgray solid'}
						),
						],id=f'crossover-wikis-badge-{i}', className='p-2 bg-dark',style={'color':'white'})
					crossover_badges.append(badge)
		else:
			badge = dbc.Badge('TBD',className='p-2')
			crossover_badges.append(badge)

		crossover_box = dbc.Stack([
			dbc.Stack([
				html.H5('Related Wikis',style={'font-weight':'bold'}),
				html.Span(['powered by ',html.A(['Recommendations Engine'],href='https://unity.acmebi.com/unity/recommendations')],style={'font-style':'italic'})
			],direction='horizontal',gap=3,className='align-items-start justify-content-start section-header p-1 px-2'),
			dbc.Stack(crossover_badges,direction='horizontal',gap=2,className='align-items-center justify-content-start',style={'flex-wrap':'wrap'})
			],gap=3,className='align-items-center justify-content-center'
		)


		vert_img = html.Img(src=vertical_icon_url,style={'max-height':'75px'})
		card = dbc.Card([
			dbc.CardHeader([
				dbc.Stack([
					vert_img,
					dbc.Stack([
						dbc.Stack([
							html.H4(f'{wiki_group} ({lang})',className='mr-auto',style={'flex':1}),
							dbc.Button(
								dbc.Stack(['Open Wiki',html.I(className='bi bi-box-arrow-up-right')],direction='horizontal',gap=1,className='align-items-center justify-content-center'),
								external_link=True,
								href=f'https://{domain}',
								target='_blank',
								color='dark',
								outline=True,
								className='p-1',
								style={'border':'none'}
							),
						],direction='horizontal',gap=3,className='align-items-start justify-items-between p-1'),
						dbc.Stack([
							html.Span([
								'Wiki ID: ',
								html.Code(wiki_id),

							]),
							is_monetized_badge,
						],direction='horizontal',gap=3,className='align-items-center justify-content-start'),
						#html.Span(f'Last Refreshed: {datetime.strptime(last_refreshed, '%Y-%m-%d %H:%M:%S.%f'):%b %d %H:%M}',style={'font-size':'.9rem'}),
						html.Span(f'Last Refreshed: {datetime.fromisoformat(last_refreshed):%b %d %H:%M}',style={'font-size':'.9rem'}),
					],gap=0,className='align-items-start justify-content-center'),
				],direction='horizontal',gap=3,className='align-items-center justify-content-center')
			]),
			dbc.CardBody([
				dbc.Stack([
					traffic_box,
					html.H5('Wiki Metadata',className='section-header p-1 px-2',),
					dbc.Stack([
						stats_table,
						poster,
					],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
					ai_summary,
					tax_cards[0],
					tax_cards[1],
					tax_cards[2],
					crossover_box,
					top_pages,
				],gap=3,className='align-items-center justify-content-center')
			]),
			dbc.CardFooter([
				dbc.Stack([
					dbc.Button(dbc.Stack([html.I(className='bi bi-download'),'Export to CSV'],direction='horizontal',gap=1),color='success',id={'type':'download-csv','index':'pages_traffic_30_days'},className='p-1'),
					dcc.Download(id={'type':'render-csv','index':'pages_traffic_30_days'})
				],direction='horizontal',gap=3,className='align-items-center justify-content-end')
			]),
		])

		return card

class ForecastTracker:
	def __init__(self, session_id=None):
		from app import AuthCredentials
		auth_manager = AuthCredentials(CACHE_KEY,ENCRYPTION_SECRET_KEY)
		google_creds = auth_manager.get_google_credentials(session_id)['google_oauth_token']
		print('Initializing ForecastTracker')
		self.init_time = datetime.now()
		self.conf = {
			#'google_credentials': None,
			#'google_credentials': Credentials(**google_creds),
			'google_credentials': extract_creds(google_creds),
			's3_path': 'forecast/',
			'forecast': {
				'sheet_id': '1lkoqPCxD24fM2U3F-lHob_CH139ab1Lfup7cd5tzyIQ',
			}
		}
		self.sources = {
			'forecast': None,
		}
		# self.set_google_credentials()
		if 'google_credentials' in self.conf:
			self.get_forecast_doc_data()

	def store_result(self,filename, data):
		print(f'Storing result: {data}')
		s3 = AmazonS3Connector()
		fpath = f'.cache/{filename}'
		try:
			print(f'Saving: {fpath}')
			data.to_csv(fpath, index=False)
			s3.save_to_s3(fpath, self.conf['s3_path'])
		except Exception as e:
			print(e)

	def read_sheet(self, sheet_id, sheet_range):
		credentials = self.conf['google_credentials']

		service = build('sheets', 'v4', credentials=credentials)

		sheet = service.spreadsheets()
		try:
			result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
		except RefreshError:
			print('(read_sheet) No refresh token, redirecting to auth flow')
			raise RefreshError
		except Exception as e:
			print('Unknown error pulling gsheet data')
			print(e)
			raise Exception
		vals = result.get('values', [])
		df = pd.DataFrame(vals)

		return df

	def get_forecast_doc_data(self):
		forecasts = {
			'wiki_breakdown_global': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'Wiki Metric Breakdown!A2:G14'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'Wiki Metric Breakdown!A18:G30'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'Wiki Metric Breakdown!A35:G47'),
			},
			'wiki_breakdown_us': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'Wiki Metric Breakdown!J2:P14'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'Wiki Metric Breakdown!J18:P30'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'Wiki Metric Breakdown!J35:P47'),
			},
			'wiki': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'acme Wiki Forecast!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'acme Wiki Forecast!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'acme Wiki Forecast!A15:R20'),
			},
			'n_r': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'N&R Forecast!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'N&R Forecast!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'N&R Forecast!A15:R20'),
			},
			'gamespot': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'GameSpot Breakout!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'GameSpot Breakout!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'GameSpot Breakout!A15:R20'),
			},
			'gamefaqs': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'GameFAQs Breakout!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'GameFAQs Breakout!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'GameFAQs Breakout!A15:R20'),
			},
			'comicvine': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'CV Breakout!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'CV Breakout!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'CV Breakout!A15:R20'),
			},
			'giant_bomb': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'GB Breakout!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'GB Breakout!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'GB Breakout!A15:R20'),
			},
			'metacritic': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'Metacritic Breakout!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'Metacritic Breakout!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'Metacritic Breakout!A15:R20'),
			},
			'tvguide': {
				'pageviews': self.read_sheet(self.conf['forecast']['sheet_id'], 'TVG Breakout!A1:R6'),
				'users' : self.read_sheet(self.conf['forecast']['sheet_id'], 'TVG Breakout!A8:R13'),
				'sessions' : self.read_sheet(self.conf['forecast']['sheet_id'], 'TVG Breakout!A15:R20'),
			},
			
		}
		self.sources['forecast'] = forecasts
		for k, v in forecasts.items():
			print(f'k is: {k} and v is: {v}')
			for k2,v2 in v.items():
				filename = f'forecast_{k}_{k2}.csv'
				print(f'filename is: {filename}, v2 is: ')
				print(v2)
				self.store_result(filename,v2)
		return self

class TransformTrendingWikis:
	def __init__(self, session_id=None):
		print("Building data for What's Trending app...")
		self.init_time = datetime.now()
		self.dates = {
			'month_start': (b.calendar.latest_date + pd.DateOffset()).replace(day=1).date().strftime('%Y-%m-%d'),
			'latest_date': b.calendar.latest_date.strftime('%Y-%m-%d'),
		}
		self.conf = {
			's3_path': 'trending/',
			'dashboard_url': f'https://insights.nickearl.net/fbi/trending-wikis',
			'slack_channel': {
				'dev': 'qlik_alert_sandbox',
				'prod': 'whats_trending_wiki',
			},
		}
		self.sources = {
			'trending': None,
			'wiki_summary': None,
			'page_total': None,
			'page_percent': None,
			'wiki_daily': None,
		}
		self.queries = {
			'trending_ctes': r"""
				with configs as (
				select
					'acme' as brand,
					'web' as platform,
					date_add('day',-29,anchor_date) as start_date,
					anchor_date as end_date,
					anchor_date as latest_date
					from (
						select cast('[_Latest Date_]' as date) as anchor_date
					)
					
				),
				latest_range as (
					select
					split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
					count(distinct analytics_id) as users,
					count(distinct concat(device_id,'-',session_id)) as sessions,
					count(case when event_type like 'pageview' then 1 end) as pageviews
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) = ( select latest_date from configs )
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					group by 1
				),
				comp_range as (
					select
					split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
					(1.0 * count(distinct analytics_id)) / 4 as users,
					(1.0 * count(distinct concat(device_id,'-',session_id))) / 4 as sessions,
					(1.0 * count(case when event_type like 'pageview' then 1 end)) / 4 as pageviews
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) = date_add('day',-7,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-14,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-21,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-28,( select latest_date from configs ))
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					group by 1
				),
				all_wiki_ids as (
					select
						split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
						wiki_id,
						count(case when event_type = 'pageview' then 1 end) as pageviews
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs )
					and ( select end_date from configs )
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					group by 1,2 order by 3 desc
				),
				top_wiki_id as (
					select
						wiki_name,
						wiki_id,
						vertical_name,
						pageviews
					from (
						select
							wiki_name,
							wiki_id,
							vertical_name,
							pageviews,
							row_number() over (partition by wiki_name order by pageviews desc) as rn
						from(
							select
								split_part(split_part(a.page_url,'://',2),'/',1) as wiki_name,
								a.wiki_id,
								b.vertical_name,
								sum(pageviews) as pageviews
							from AwsDataCatalog.analytics.example_table a left join analytics.dimension_table b on a.wiki_id = b.wiki_id
							where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs ) and ( select end_date from configs )
							and lower(a.brand) = 'acme'
							and lower(a.platform) = 'web'
							group by 1,2,3
						)
					)
					where rn = 1
					order by pageviews desc
				),
				final_wikis as (
					select
						c.wiki_id,
						a.wiki_name,
						c.vertical_name,
						a.users,
						b.users as users_benchmark,
						a.users - b.users as users_total_vs_benchmark,
						(1.0 * a.users / b.users) - 1 as users_percent_vs_benchmark,
						(1.0 * a.pageviews / a.sessions) as pvs_per_session,
						(1.0 * b.pageviews / b.sessions) as pvs_per_session_benchmark,
						(1.0 * a.pageviews / a.sessions) - (1.0 * b.pageviews / b.sessions) as pvs_per_session_total_vs_benchmark,
						( (1.0 * a.pageviews / a.sessions) / (1.0 * b.pageviews / b.sessions) ) -1 as pvs_per_session_percent_vs_benchmark
					from (
						(select * from latest_range) as a
						left join
						(select * from comp_range) as b
						on a.wiki_name = b.wiki_name
						left join
						(select * from top_wiki_id) as c
						on a.wiki_name = c.wiki_name
						)
					where a.wiki_name is not null
					and a.wiki_name not like '%turbopages.org%'
					and a.users >= 1000
					order by users_total_vs_benchmark desc
					),
				top_20_total as (
					select
						wiki_name  
					from final_wikis
					order by users_total_vs_benchmark desc
					limit 20		  
				),
				top_20_percent as (
					select
						wiki_name
					from final_wikis
					where users >= 2000
					order by users_percent_vs_benchmark desc
					limit 20		  
				),
				top_pages_total_latest as (
					select
					split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
					regexp_replace(regexp_replace(page_url, '\?.+=?+$',''), '#.+$', '') as page,
					count(distinct analytics_id) as users,
					count(distinct concat(device_id,'-',session_id)) as sessions,
					count(case when event_type like 'pageview' then 1 end) as pageviews
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) = ( select latest_date from configs )
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					and split_part(split_part(page_url,'://',2),'/',1) in (select wiki_name from top_20_total)
					group by 1,2
				),
				top_pages_total_comp as (
				select
					split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
					regexp_replace(regexp_replace(page_url, '\?.+=?+$',''), '#.+$', '') as page,
					(1.0 * count(distinct analytics_id)) / 4 as users,
					(1.0 * count(distinct concat(device_id,'-',session_id))) / 4 as sessions,
					(1.0 * count(case when event_type like 'pageview' then 1 end)) / 4 as pageviews
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) = date_add('day',-7,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-14,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-21,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-28,( select latest_date from configs ))
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					and split_part(split_part(page_url,'://',2),'/',1) in (select wiki_name from top_20_total)
					group by 1,2
				),
				top_pages_total_final as (
					select
						*,
						rank() over (partition by wiki_name order by users_total_vs_benchmark desc) as rnk
					from (
						select
							a.wiki_name,
							a.page,
							a.users,
							b.users as users_benchmark,
							a.users - b.users as users_total_vs_benchmark,
							(1.0 * a.users / b.users) - 1 as users_percent_vs_benchmark,
							(1.0 * a.pageviews / a.sessions) as pvs_per_session,
							(1.0 * b.pageviews / b.sessions) as pvs_per_session_benchmark,
							(1.0 * a.pageviews / a.sessions) - (1.0 * b.pageviews / b.sessions) as pvs_per_session_total_vs_benchmark,
							( (1.0 * a.pageviews / a.sessions) / (1.0 * b.pageviews / b.sessions) ) -1 as pvs_per_session_percent_vs_benchmark
						from (
							(select * from top_pages_total_latest) as a
							left join
							(select * from top_pages_total_comp) as b
							on a.wiki_name = b.wiki_name
							and a.page = b.page
							)
							where a.users >= 100
						order by users_total_vs_benchmark desc
						)	
				),
				top_pages_percent_latest as (
					select
					split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
					regexp_replace(regexp_replace(page_url, '\?.+=?+$',''), '#.+$', '') as page,
					count(distinct analytics_id) as users,
					count(distinct concat(device_id,'-',session_id)) as sessions,
					count(case when event_type like 'pageview' then 1 end) as pageviews
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) = ( select latest_date from configs )
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					and split_part(split_part(page_url,'://',2),'/',1) in (select wiki_name from top_20_percent)
					group by 1,2
				),
				top_pages_percent_comp as (
					select
					split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
					regexp_replace(regexp_replace(page_url, '\?.+=?+$',''), '#.+$', '') as page,
					(1.0 * count(distinct analytics_id)) / 4 as users,
					(1.0 * count(distinct concat(device_id,'-',session_id))) / 4 as sessions,
					(1.0 * count(case when event_type like 'pageview' then 1 end)) / 4 as pageviews
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) = date_add('day',-7,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-14,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-21,( select latest_date from configs ))
					or cast(concat(year, '-', month, '-', day) as date) = date_add('day',-28,( select latest_date from configs ))
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					and split_part(split_part(page_url,'://',2),'/',1) in (select wiki_name from top_20_percent)
					group by 1,2
					
				),
				top_pages_percent_final as (
					select
						*,
						rank() over (partition by wiki_name order by users_percent_vs_benchmark desc) as rnk
					from (
						select
							a.wiki_name,
							a.page,
							a.users,
							b.users as users_benchmark,
							a.users - b.users as users_total_vs_benchmark,
							(1.0 * a.users / b.users) - 1 as users_percent_vs_benchmark,
							(1.0 * a.pageviews / a.sessions) as pvs_per_session,
							(1.0 * b.pageviews / b.sessions) as pvs_per_session_benchmark,
							(1.0 * a.pageviews / a.sessions) - (1.0 * b.pageviews / b.sessions) as pvs_per_session_total_vs_benchmark,
							( (1.0 * a.pageviews / a.sessions) / (1.0 * b.pageviews / b.sessions) ) -1 as pvs_per_session_percent_vs_benchmark
						from (
							(select * from top_pages_percent_latest) as a
							left join
							(select * from top_pages_percent_comp) as b
							on a.wiki_name = b.wiki_name
							and a.page = b.page
							)
							where a.users >= 100
						order by users_percent_vs_benchmark desc
					)	
				),
				session_details_by_wiki as (
				  select
					concat(device_id,'-',session_id) as device_session_id,
					split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
					greatest(
							date_diff(
								'second',
								min(cast(event_time as timestamp)),
								max(cast(event_time as timestamp))
							),
							0
						) as session_duration_seconds,
					count(*) as hit_depth,
					min(event_time) as min_ts
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs ) and ( select end_date from configs )
					and date_trunc('day',cast(event_time as timestamp)) between ( select start_date from configs ) and ( select end_date from configs )
					and lower(brand) = ( select brand from configs )
					and lower(platform) = (select platform from configs)
					and session_id <> '-1'
					group by 1,2
				),
				traffic_data_daily as(
					select
						date,
						a.wiki_name,
						count (distinct analytics_id ) as users,
						count (distinct a.device_session_id ) as sessions,
						sum ( case when event_type = 'pageview' then 1 end ) as pageviews,
						sum ( session_duration_seconds ) as session_duration_seconds,
						sum ( case when hit_depth = 1 then 1 end ) as bounces
					from (
					(select
						concat(year,'-',month,'-',day) as date,
						split_part(split_part(page_url,'://',2),'/',1) as wiki_name,
						analytics_id,
						concat(device_id,'-',session_id) as device_session_id,
						event_type,
						event_time
					from AwsDataCatalog.analytics.analytics_events_view
					where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs )
					and ( select end_date from configs )
					and lower(brand) = ( select brand from configs ) 
					and lower(platform) = (select platform from configs)
					and (split_part(split_part(page_url,'://',2),'/',1) in (select wiki_name from top_20_total)
					or split_part(split_part(page_url,'://',2),'/',1) in (select wiki_name from top_20_percent) )
					) as a
					left join
					( select * from session_details_by_wiki) as b
					on a.device_session_id = b.device_session_id
					and a.event_time = b.min_ts
					)
					group by 1,2
					order by users desc
				)
		""",
			'trending_selects': {
				'wiki_summary': """
					select * from final_wikis
					where wiki_name is not null
					and wiki_name not like '%turbopages.org%'
					and users >= 1000
				""",
				'page_total': """
					select * from top_pages_total_final
					where wiki_name is not null
					and page is not null
					and rnk <= 20
					order by users_total_vs_benchmark desc  
				""",
				'page_percent': """
					select * from top_pages_percent_final
					where wiki_name is not null
					and page is not null
					and rnk <= 20
					order by users_percent_vs_benchmark desc 
				""",
				'wiki_daily': """
					select * from traffic_data_daily
					where wiki_name is not null
				""",
				# 'wiki_name_to_ids': """
				# 	select * from all_wiki_ids
				# 	where wiki_name is not null
				# 	and wiki_id is not null
				# """,
			}
		}
		self.get_data()
		response = self.send_slack_message()
		print(response)

	def store_result(self,filename, data):
		print('Storing result: ')
		print(f'type {type(data)}')
		print(f'{data}')
		try:
			print(f'Cols: {data.columns}')
		except Exception as e:
			print(f'no columns: {e}')
			pass
		fpath = os.path.join('.cache',self.conf['s3_path'],filename)
		s3 = AmazonS3Connector()
		try:
			print(f'Saving: {fpath}')
			data.to_csv(fpath, index=False)
			s3.save_to_s3(fpath, self.conf['s3_path'])
		except Exception as e:
			try:
				d = os.path.join('.cache',self.conf['s3_path'])
				print(f'trying to make dir: {d}')
				os.mkdir(d)
				data.to_csv(fpath, index=False)
				s3.save_to_s3(fpath, self.conf['s3_path'])
			except Exception as ee:
				print(f'Error creating local cache folder: {ee}')
			print(f'Error saving to s3 {fpath}: {e}')

	def get_data(self):
		print('Getting trending data')
		run_queries = False if DRYRUN else True
		b = bi.BiUtils()

		for k,v in self.queries['trending_selects'].items():
			fname = os.path.join('.cache',self.conf['s3_path'],f'{k}.csv')
			q = self.queries['trending_ctes'].replace('[_Latest Date_]',b.calendar.latest_date.strftime('%Y-%m-%d'))
			q = q + '\n' + v
			print('query will be: ')
			print(q)

			if DRYRUN:
				try:
					df = pd.read_csv(fname, sep=',',header=0)
					run_queries = False
				except Exception as e:
					print(f'no cached file, repulling: {k} | {e}')
					run_queries = True
			if run_queries == True:
				qa = QueryAthena('AwsDataCatalog','analytics',q)
				df = qa.run_query()

			self.sources[k] = df
			print('Saving to S3')
			self.store_result(f'{k}.csv',df)

		# Create metadata for trending wikis
		df = self.sources['wiki_summary']
		df['wiki_id'] = df['wiki_id'].fillna(0).astype(int).apply(str)
		tot = df.sort_values(by=['users_total_vs_benchmark'], ascending=False).head(50)
		spd = df.sort_values(by=['users_percent_vs_benchmark'], ascending=False).head(50)
		wikis = tot[['wiki_id','wiki_name']].merge(spd[['wiki_id','wiki_name']], how='outer', on=['wiki_id','wiki_name'])
		wikis['wiki_group'] = wikis['wiki_name']
		print('wikis is: ')
		print(wikis)

		wm = WikiMetadata()
		all_meta_df = wm.data['wiki_metadata']
		# w_ids = wm.get_wiki_ids_from_groups(wikis['wiki_group'])
		# self.sources['wiki_names_to_ids'] = w_ids
		# print('Saving to S3')
		# self.store_result(f'wiki_names_to_ids.csv',w_ids)

		print('tw got this metadata from wm:')
		print(all_meta_df)
		all_meta_df['wiki_id'] = all_meta_df['wiki_id'].apply(str)
		all_meta_df['last_refreshed'] = all_meta_df['last_refreshed'].apply(lambda x: datetime.fromisoformat(str(x)) if str(x) != '00:25.8' else '2024-09-19 22:02:32.888551' ) # remove once db cleaned up
		all_meta_df['last_refreshed'] = all_meta_df['last_refreshed'].apply(lambda x: datetime.fromisoformat(x) if isinstance(x, str) else x)
		# wiki_id	wiki_group	domain	created_at	is_kid_wiki	vertical_name	lang	founding_user_id	founding_user_name	users_30_days	sessions_30_days	pageviews_30_days	is_monetized	poster_url	ai_summary	ai_news	ai_franchise	ai_flags	last_refreshed
		all_meta_df = all_meta_df.merge(wikis['wiki_id'], how='outer',on=['wiki_id'], suffixes=(None,'_y'))

		need_meta_df = all_meta_df[all_meta_df['wiki_id'].astype(str).isin(wikis['wiki_id'])]
		refresh_meta_df = need_meta_df[(pd.to_datetime(need_meta_df['last_refreshed']) <= datetime.now() + pd.DateOffset(days=-6)) | (pd.isnull(need_meta_df['last_refreshed'])) | (pd.isnull(need_meta_df['ai_summary']))]
		refresh_meta_df = refresh_meta_df[refresh_meta_df['wiki_id'] != '0']
		print('refresh_meta_df starts as this')
		print(refresh_meta_df)
		if refresh_meta_df.empty:
			print('No new wiki_ids to refresh')
		else:
			print(f'Submitting refresh for {len(refresh_meta_df)} wiki_ids')
			all_meta_df = wm.refresh_wiki_ids(refresh_meta_df['wiki_id'])

		self.sources['wiki_metadata'] = all_meta_df

		return self

	def send_slack_message(self):
		print('Sending summary to Slack channel...')

		client = WebClient(token=SLACK_TOKEN)
		channel = self.conf['slack_channel'][DEPLOY_ENV.lower()]

		b = bi.BiUtils()

		_df = self.sources['wiki_summary']
		_df['vertical_name'] = _df['vertical_name'].apply(lambda x: str(x).lower() )
		print('initial _df:')
		print(_df)

		text_val = f"Trending Wikis Report for {b.calendar.latest_date.strftime('%b %d, %Y')}"

		message_content = {
			'users_total_vs_benchmark': [],
			'users_percent_vs_benchmark': [],
		}
		for k,v in message_content.items():
			df = _df.sort_values(by=k,ascending=False)
			df_all = df.head(5)
			df_games = _df[df['vertical_name'].isin(['games']) ].head(5)
			df_ent = df[df['vertical_name'].isin(['tv','movies']) ].head(5)
			df_other = df[~df['vertical_name'].isin(['games','tv','movies']) ].head(5)
			for table in [df_all,df_games,df_ent,df_other]:
				t = []
				for i in range(len(table)):
					icon = b.styles['icon_map_slack'].get(table['vertical_name'].iloc[i].lower(), ':retro_sunset:')
					domain = table['wiki_name'].iloc[i]
					v = table[k].iloc[i]
					value = f'{v:+,.0f}'
					if k == 'users_percent_vs_benchmark':
						value = f'{v:+,.0%}'
					o = {
							"type": "mrkdwn",
							"text": f"*{i + 1}.  {icon} | <https://{domain}|{domain}>*",
							"verbatim": False
						}
					t.append(o)
					o = {
							"type": "plain_text",
							"text": value,
							"emoji": True
						}
					t.append(o)
				message_content[k].append(t)

		blocks_val = [
			  {
				"type": "section",
				"block_id": "xbvHK",
				"accessory": {
				  "type": "button",
				  "action_id": "actionId-0",
				  "text": {
					"type": "plain_text",
					"text": "View Full Report",
					"emoji": True
				  },
				  "value": "view_full_report",
				  "url": self.conf['dashboard_url'],
				},
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*Trending Wikis*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": f"*{b.calendar.latest_date.strftime('%b %d, %Y')}*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "header",
				"block_id": "uLjko",
				"text": {
				  "type": "plain_text",
				  "text": "Which Wikis Grew by the Most Overall Users Yesterday?",
				  "emoji": True
				}
			  },
			  {
				"type": "section",
				"block_id": "aKIr4",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*All Verticals*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "e85BK",
				"fields": message_content['users_total_vs_benchmark'][0],
			  },
			  {
				"type": "divider",
				"block_id": "G0ZIw"
			  },
			  {
				"type": "section",
				"block_id": "JmAVx",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*Games*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "i5s/B",
				"fields": message_content['users_total_vs_benchmark'][1],
			  },
			  {
				"type": "divider",
				"block_id": "izV4f"
			  },
			  {
				"type": "section",
				"block_id": "l3keA",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*Film &amp; TV*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "WJt2s",
				"fields": message_content['users_total_vs_benchmark'][2],
			  },
			  {
				"type": "divider",
				"block_id": "RoA7D"
			  },
			  {
				"type": "section",
				"block_id": "dl00V",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*Other Verticals*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "up3RD",
				"fields": message_content['users_total_vs_benchmark'][3],
			  },
			  {
				"type": "actions",
				"block_id": "xWuzJ",
				"elements": [
				  {
					"type": "button",
					"action_id": "actionId-0",
					"text": {
					  "type": "plain_text",
					  "text": "View Full Report",
					  "emoji": True
					},
					"value": "view_full_report",
					"url": self.conf['dashboard_url']
				  },
				  {
					"type": "button",
					"action_id": "actionId-1",
					"text": {
					  "type": "plain_text",
					  "text": "Discuss in #data-is-fun",
					  "emoji": True
					},
					"value": "discuss",
					"url": "https://acme.slack.com/archives/C011DD5LSUC"
				  }
				]
			  },
			  {
				"type": "divider",
				"block_id": "GzmFc"
			  },
			  {
				"type": "header",
				"block_id": "r4qpW",
				"text": {
				  "type": "plain_text",
				  "text": "Which Wikis Grew the Fastest Yesterday?",
				  "emoji": True
				}
			  },
			  {
				"type": "section",
				"block_id": "yc8je",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*All Verticals*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users % vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "2IoGJ",
				"fields": message_content['users_percent_vs_benchmark'][0]
			  },
			  {
				"type": "divider",
				"block_id": "dZ52N"
			  },
			  {
				"type": "section",
				"block_id": "WQT4x",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*Games*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users % vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "GOBls",
				"fields": message_content['users_percent_vs_benchmark'][1]
			  },
			  {
				"type": "divider",
				"block_id": "FOepC"
			  },
			  {
				"type": "section",
				"block_id": "vUjn7",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*Film &amp; TV*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users % vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "CckDC",
				"fields": message_content['users_percent_vs_benchmark'][2]
			  },
			  {
				"type": "divider",
				"block_id": "lTr5y"
			  },
			  {
				"type": "section",
				"block_id": "Dtpq1",
				"fields": [
				  {
					"type": "mrkdwn",
					"text": "*Other Verticals*",
					"verbatim": False
				  },
				  {
					"type": "mrkdwn",
					"text": "*Users % vs Benchmark*",
					"verbatim": False
				  }
				]
			  },
			  {
				"type": "section",
				"block_id": "uCMiw",
				"fields": message_content['users_percent_vs_benchmark'][3]
			  },
			  {
				"type": "actions",
				"block_id": "fmHhg",
				"elements": [
				  {
					"type": "button",
					"action_id": "actionId-0",
					"text": {
					  "type": "plain_text",
					  "text": "View Full Report",
					  "emoji": True
					},
					"value": "view_full_report",
					"url": self.conf['dashboard_url']
				  },
				  {
					"type": "button",
					"action_id": "actionId-1",
					"text": {
					  "type": "plain_text",
					  "text": "Discuss in #data-is-fun",
					  "emoji": True
					},
					"value": "discuss",
					"url": "https://acme.slack.com/archives/C012DD5LSUC"
				  }
				]
			  },
			  {
				"type": "divider",
				"block_id": "zcrnA"
			  },
			  {
				"type": "context",
				"block_id": "hwqz+",
				"elements": [
				  {
					"type": "mrkdwn",
					"text": "Source: <https://app.amplitude.com/analytics/acme|Amplitude>. Daily comparison of total unique user counts for each wiki vs expected users based on a rolling 4 week benchmark indexed by day of the week, UTC timezone. Excludes wikis with less than 1K users on reporting date. Questions? Join us in <#C011DD5LSUC> or reach out to @nearl.",
					"verbatim": False
				  }
				]
			  }
			]
		response = client.chat_postMessage (
			channel=channel,
			text=text_val,
			blocks=json.dumps(blocks_val)
		)
		return response

class TransformMonetizationMonitor:
	def __init__(self, session_id=None):
		print(f'Building data for Monetization Monitor | session_id: {session_id}...')
		from app import AuthCredentials
		auth_manager = AuthCredentials(CACHE_KEY,ENCRYPTION_SECRET_KEY)
		google_creds = auth_manager.get_google_credentials(session_id)['google_oauth_token']
		print(f'got google creds: {google_creds}')
		self.init_time = datetime.now()
		self.dates = {
			#'month_start': (b.calendar.latest_date + pd.DateOffset()).replace(day=1).date().strftime('%Y-%m-%d'),
			'latest_date': b.calendar.latest_date.strftime('%Y-%m-%d'),
		}
		self.conf = {
			# 'google_credentials': None,
			# 'google_credentials': Credentials(**google_creds),
			'google_credentials': extract_creds(google_creds),
			's3_path': 'monetization_monitor/',
			'ignore_list': '1u687587gihoihJHoIPxJ6MzlC9JhCgdjhgdgfdo9vfUPMw',
			'wiki_list': [],
			'dashboard_url': f'https://insights.acmebi.com/fbi/monetization-monitor',
			'slack_channel': {
				'dev': 'qlik_alert_sandbox',
				'prod': 'wiki_monetization_review',
			},
		}
		self.sources = {
			'wiki_traffic_30_days': None,
			'ignore_list': None,
		}
		self.queries = {
			'configs': """
				with configs as (
					select
						anchor_date as end_date,
						date_add('day', -29, anchor_date) as start_date,
						5000 as threshold
					from (
						select
							cast('[_Latest Date_]' as date) as anchor_date
						)
				)
			""",
			'ctes': r"""
				unmonetized_wiki_ids_above_min_traffic_threshold as (
					select
						wiki_id,
						is_monetized,
						users,
						sessions,
						pageviews
						from (
							select
								a.wiki_id,
								is_monetized,
								count(distinct analytics_id) as users,
								count(distinct session_id) as sessions,
								sum(pageviews) as pageviews
							from AwsDataCatalog.analytics.example_table a
							left join AwsDataCatalog.analytics.dimension_table b
							on a.wiki_id = b.wiki_id
							where cast(concat(year, '-', month, '-', day) as date) between ( select start_date from configs ) and ( select end_date from configs )
							and lower(brand) = 'acme'
							and lower(platform) = 'web'
							and not (is_monetized = 1)
							group by 1,2
							order by pageviews desc
						)	
						where pageviews >= (select threshold from configs)
						order by pageviews desc
				)
				
			""",
			'select_statements': {
				'wiki_traffic_30_days': 'select * from unmonetized_wiki_ids_above_min_traffic_threshold'
			}
		}
		# self.set_google_credentials()
		if 'google_credentials' in self.conf:
			self.get_data()
			response = self.send_slack_message()
			print(response)


	def store_result(self,filename, data):
		print('Storing result: ')
		print(f'type {type(data)}')
		print(f'{data}')
		print('Cols:')
		print(data.columns)
		fpath = os.path.join('.cache',self.conf['s3_path'],filename)
		s3 = AmazonS3Connector()
		try:
			print(f'Saving: {fpath}')
			data.to_csv(fpath, index=False)
			s3.save_to_s3(fpath, self.conf['s3_path'])
		except Exception as e:
			try:
				d = os.path.join('.cache',self.conf['s3_path'])
				print(f'trying to make dir: {d}')
				os.mkdir(d)
				data.to_csv(fpath, index=False)
				s3.save_to_s3(fpath, self.conf['s3_path'])
			except Exception as ee:
				print(f'Error creating local cache folder: {ee}')
			print(f'Error saving to s3 {fpath}: {e}')


	def read_sheet(self, sheet_id, sheet_range):
		credentials = self.conf['google_credentials']

		service = build('sheets', 'v4', credentials=credentials)

		sheet = service.spreadsheets()
		try:
			result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute()
		except RefreshError:
			print('(read_sheet) No refresh token, redirecting to auth flow')
			raise RefreshError
		except Exception as e:
			print('Unknown error pulling gsheet data')
			print(e)
			raise Exception
		vals = result.get('values', [])
		df = pd.DataFrame(vals)

		return df

	def get_data(self):
		run_queries = False if DRYRUN else True
		b = bi.BiUtils()

		# Get wikis above traffic threshold
		fname = os.path.join('.cache',self.conf['s3_path'],f'wiki_traffic_30_days.csv')
		q = self.queries['configs'].replace('[_Latest Date_]',b.calendar.latest_date.strftime('%Y-%m-%d'))
		q = ',\n'.join([q,self.queries['ctes']])
		q = '\n'.join([q,self.queries['select_statements']['wiki_traffic_30_days']])

		if DRYRUN:
			try:
				df = pd.read_csv(fname, sep=',',header=0)
				run_queries = False
			except Exception as e:
				print(f'no cached file, repulling: wiki_traffic_30_days | {e}')
				run_queries = True

		if run_queries:
			qa = QueryAthena('AwsDataCatalog','analytics',q)
			df = qa.run_query()
		df['wiki_id'] = df['wiki_id'].fillna(0).astype(int).apply(str)
		print(f'Results: wiki_traffic_30_days')
		print(df)
		self.sources['wiki_traffic_30_days'] = df
		self.store_result(f'wiki_traffic_30_days.csv',df)

		# Get list of wikis to ignore from Google Sheet
		df = self.read_sheet(self.conf['ignore_list'], 'Ignore!A:B')
		df.columns = df.iloc[0]
		df = df[1:]
		df.reset_index(drop=True, inplace=True)
		df['wiki_id'] = df['wiki_id'].apply(str)
		self.sources['ignore_list'] = df

		# Filter for unmonetized, threshold-meeting wikis
		t_df = self.sources['wiki_traffic_30_days']
		t_df['wiki_id'] = t_df['wiki_id'].apply(str)
		ignore_df = self.sources['ignore_list']

		df = t_df[~t_df['wiki_id'].isin(ignore_df['wiki_id'])]
		print('after ignores, df is:')
		print(df)

		wm = WikiMetadata()
		all_meta_df = wm.data['wiki_metadata']

		print('mm got this metadata from wm:')
		print(all_meta_df)
		all_meta_df['wiki_id'] = all_meta_df['wiki_id'].apply(str)
		all_meta_df['last_refreshed'] = all_meta_df['last_refreshed'].apply(lambda x: datetime.fromisoformat(str(x)) if str(x) != '00:25.8' else '2024-09-19 22:02:32.888551' ) # remove once db cleaned up
		all_meta_df['last_refreshed'] = all_meta_df['last_refreshed'].apply(lambda x: datetime.fromisoformat(x) if isinstance(x, str) else x)
		# wiki_id	wiki_group	domain	created_at	is_kid_wiki	vertical_name	lang	founding_user_id	founding_user_name	users_30_days	sessions_30_days	pageviews_30_days	is_monetized	poster_url	ai_summary	ai_news	ai_franchise	ai_flags	last_refreshed
		all_meta_df = all_meta_df.merge(df[['wiki_id']], how='outer',on=['wiki_id'], suffixes=(None,'_y'))

		need_meta_df = all_meta_df[all_meta_df['wiki_id'].astype(str).isin(df['wiki_id'].astype(str))]

		need_meta_df['crossover_wikis'] = need_meta_df.get('crossover_wikis', None) 
		refresh_meta_df = need_meta_df[(pd.to_datetime(need_meta_df['last_refreshed']) <= datetime.now() + pd.DateOffset(days=-6)) | (pd.isnull(need_meta_df['last_refreshed'])) | (pd.isnull(need_meta_df['crossover_wikis'])) | (pd.isnull(need_meta_df['ai_summary']))]
		refresh_meta_df = refresh_meta_df[refresh_meta_df['wiki_id'] != '0']
		print('refresh_meta_df starts as this')
		print(refresh_meta_df)

		if refresh_meta_df.empty:
			print('No new wiki_ids to refresh')
		else:
			print(f'Submitting refresh for {len(refresh_meta_df)} wiki_ids')
			if not DRYRUN:
				all_meta_df = wm.refresh_wiki_ids(refresh_meta_df['wiki_id'])

		self.sources['wiki_metadata'] = all_meta_df

		unmonetized_wikis_df = all_meta_df[all_meta_df['wiki_id'].isin(df['wiki_id'])]
		print('ended up with merged df (now saving as unmonetized_wikis: ')
		print(unmonetized_wikis_df)
		print(unmonetized_wikis_df.columns)
		self.sources['unmonetized_wikis'] = unmonetized_wikis_df
		self.store_result('unmonetized_wikis.csv',unmonetized_wikis_df)

		return self

	def send_slack_message(self):
		print('Sending summary to Slack channel...')

		client = WebClient(token=SLACK_TOKEN)
		channel = self.conf['slack_channel'][DEPLOY_ENV.lower()]
		b = bi.BiUtils()
		_df = self.sources['unmonetized_wikis']
		print('unmonetized_wikis df is: ')
		print(_df)
		_df['vertical_name'] = _df['vertical_name'].apply(lambda x: str(x).lower() )
		_df['ai_summary'] = _df['ai_summary'].fillna('TBD')
		_df['ai_flags'] = _df['ai_flags'].fillna('0').apply(lambda x: ast.literal_eval(str(x)) if x != 0 else {})
		_df = _df.sort_values(by='pageviews_30_days',ascending=False)
		df = _df
		report_date = (datetime.now() + pd.DateOffset(days=-1)).strftime('%b %d, %Y')
		client = WebClient(token=SLACK_TOKEN)

		text_val = f"Unmonetized Wikis Report for {report_date}"
		content = []
		for i in range(min(len(df),5)):
			data = df.iloc[i]
			vertical_icon = b.styles['icon_map_slack'].get(data['vertical_name'].lower(), ':retro_sunset:')
			summary = data['ai_summary'] if len(data['ai_summary']) <= 200 else f"{data['ai_summary'][:200]}..."
			sensitive_flags = []
			for k,v in data['ai_flags'].items():
				if k in ['illegal','hate','violence','sexual']:
					if v:
						sensitive_flags.append(k)
			if len(sensitive_flags) == 0:
				sensitive_flags = 'TBD'
			else:
				sensitive_flags = ', '.join(sensitive_flags)
			wiki_block = [{
				"type": "section",
				"text": {
					"type": "mrkdwn",
					"text": f" {vertical_icon} *<https://{data['domain']}|{data['domain']} ({data['lang']})> [id: {data['wiki_id']}]*"},
					},
					{
						"type": "section",
						"text": {
							"type": "mrkdwn",
							"text": f"*:sparkles: Summary:* {summary}"
						}
					},
					{
						"type": "section",
						"fields": [
							{
								"type": "mrkdwn",
								"text": "*30 Day Users:*"
							},
							{
								"type": "plain_text",
								"text": b.auto_num_format(data['users_30_days']),
								"emoji": True
							},
							{
								"type": "mrkdwn",
								"text": "*30 Day Pageviews:*"
							},
							{
								"type": "plain_text",
								"text": b.auto_num_format(data['pageviews_30_days']),
								"emoji": True
							},
							{
								"type": "mrkdwn",
								"text": "*Avg. PV/S*"
							},
							{
								"type": "plain_text",
								"text": f"{data['pageviews_30_days'] / data['sessions_30_days']:.1f}",
								"emoji": True
							}
						]
					},
					{
						"type": "context",
						"elements": [
							{
								"text": f"*:sparkles: Sensitive Content:* {sensitive_flags}",
								"type": "mrkdwn"
							}
						]
					},
					{
						"type": "divider"
					},]
			content.extend(wiki_block)

		blocks_val = [
				{
					"type": "header",
					"text": {
						"type": "plain_text",
						"text": ":flame-heart: Unmonetized Wikis Review"
					}
				},
				{
					"type": "context",
					"elements": [
						{
							"type": "mrkdwn",
							"text": f"Latest Unmonetized Wikis for Review | {b.calendar.latest_date:%b %d %Y}"
						}
					]
				},
				{
					"type": "actions",
					"elements": [
						{
							"type": "button",
							"text": {
								"type": "plain_text",
								"text": "Edit Ignore List",
								"emoji": True
							},
							"value": "edit_ignore_list",
							"action_id": "edit_ignore_list",
							"url": "https://docs.google.com/spreadsheets/d/gkiuyg78tg887yPMw/edit?gid=0#gid=0"
						},
						{
							"type": "button",
							"text": {
								"type": "plain_text",
								"text": "View Full Report",
								"emoji": True
							},
							"value": "view_full_report",
							"action_id": "view_full_report",
							"url": self.conf['dashboard_url']
						}
					]
				},
				{
					"type": "divider"
				},
		]
			#########
		blocks_val.extend(content)
			#####
		blocks_val.extend([
				{
					"type": "context",
					"elements": [
						{
							"type": "mrkdwn",
							"text": "Source: Amplitude. Daily comparison of rolling prior 30 day total traffic for each wiki that 1) has at least 5K pageviews in the past 30 days and 2) is unmonetized."
						},
						{
							"type": "mrkdwn",
							"text": ":sparkles: = via AI (so please verify accuracy!)"
						}
					]
				}
			])

		response = client.chat_postMessage (
			channel=channel,
			text=text_val,
			unfurl_links=False,
			unfurl_media=False,
			blocks=json.dumps(blocks_val)
		)
		return response


@shared_task
def run_transform_forecast(client_name,session_id=None):
	forecast = ForecastTracker(session_id=session_id)
	set_last_run(f'tf_schedule:{hashlib.md5(client_name.encode('utf-8')).hexdigest()}',datetime.now().isoformat())
	return forecast

@shared_task
def run_transform_trending(client_name,session_id=None):
	trending = TransformTrendingWikis(session_id=session_id)
	set_last_run(f'tf_schedule:{hashlib.md5(client_name.encode('utf-8')).hexdigest()}',datetime.now().isoformat())
	return trending

# @shared_task
# def run_transform_kpi_summary(client_name):
# 	eda = TransformKpiSummary()
# 	set_last_run(f'tf_schedule:{hashlib.md5(client_name.encode('utf-8')).hexdigest()}',datetime.now().isoformat())
# 	return eda

@shared_task
def run_transform_mon(client_name,session_id=None):
	mon = TransformMonetizationMonitor(session_id=session_id)
	set_last_run(f'tf_schedule:{hashlib.md5(client_name.encode('utf-8')).hexdigest()}',datetime.now().isoformat())
	return mon

@shared_task
def run_wiki_metadata(client_name,session_id=None):
	wm = WikiMetadata(session_id=session_id)
	set_last_run(f'tf_schedule:{hashlib.md5(client_name.encode('utf-8')).hexdigest()}',datetime.now().isoformat())
	return wm
