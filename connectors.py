import pandas as pd
import os, json, requests, boto3, time, io, redshift_connector
from typing import Dict
from google.oauth2 import service_account
from googleapiclient.discovery import build, Resource
from google.cloud import bigquery
from dotenv import load_dotenv
import redis

# Connections to various major data sources
# Nick Earl, 2024

load_dotenv()


# AMPLITUDE_KEY = os.environ['AMPLITUDE_KEY']
# AMPLITUDE_SECRET = os.environ['AMPLITUDE_SECRET']
AWS_KEY = os.environ['AWS_KEY']
AWS_SECRET = os.environ['AWS_SECRET']
REDSHIFT_KEY = os.environ['REDSHIFT_KEY']
REDSHIFT_SECRET = os.environ['REDSHIFT_SECRET']
REST_API_USER = os.environ['REST_API_USER']
REST_API_PASS = os.environ['REST_API_PASS']
REDISCLOUD_URL = os.environ['REDISCLOUD_URL']


BQ_FILE = 'key_bq.json'
OAUTH_FILE = 'key_oauth.json'

conf = {
	'aws': {
		'key': AWS_KEY,
		'secret': AWS_SECRET,
		'bucket': 'de-qlik',
		'folder': 'transform/',
		'region': 'us-east-1'
	},
	'redshift': {
		'key': REDSHIFT_KEY,
		'secret': REDSHIFT_SECRET,
		'host': 'xx-redshift.xxxxxxxxxxxx.us-east-1.redshift.amazonaws.com',
		'port': 5439,
		'database': 'db',
	},
	'biq_query': {
		'credentials': service_account.Credentials.from_service_account_file(BQ_FILE)
	},
}

class QueryBigQuery:

	def __init__(self,creds:str='biq_query'):
		print('Initializing BQ connector')
		self.credentials=conf[creds]['credentials']
		print(f'using:{str(self.credentials)}')
		self.client = bigquery.Client(credentials = self.credentials)

	def run_query(self,query: str) -> pd.DataFrame:
		print('running BQ query')
		result = self.client.query(query)
		df = result.to_dataframe()
		return df

class QueryRedshift:

	def __init__(self):
		print('Initializing Redshift connector')
		self.conn = redshift_connector.connect(
			host = conf['redshift']['host'],
			database = conf['redshift']['database'],
			user = conf['redshift']['key'],
			password = conf['redshift']['secret'],
			# access_key_id = conf['redshift']['key'],
			# secret_access_key = conf['redshift']['secret'],
			)
		print('connected to redshift')

	def run_query(self,query:str) -> pd.DataFrame:
		print('running redshift query')
		cursor = self.conn.cursor()
		cursor.execute(query)
		#result = cursor.fetchall()
		result: pd.DataFrame = cursor.fetch_dataframe()
		return result

class AmazonS3Connector:

	def __init__(self):
		self.folder = conf['aws']['folder']
		self.bucket = conf['aws']['bucket']
		self.s3_path =  's3://' + self.bucket + '/' + self.folder
		self.region_name = conf['aws']['region']
		self.aws_access_key_id = conf['aws']['key']
		self.aws_secret_access_key = conf['aws']['secret']

	def save_to_s3(self,filepath:str,folder:str=None):
		filename = os.path.basename(filepath).split('/')[-1]
		s3_path = self.folder + filename
		if folder != None:
			s3_path = self.folder + folder + filename
		try:
			self.resource = boto3.resource('s3', 
								  region_name = self.region_name, 
								  aws_access_key_id = self.aws_access_key_id,
								  aws_secret_access_key= self.aws_secret_access_key)

			response = self.resource \
			.Bucket(self.bucket) \
			.upload_file(filepath, s3_path)
		except Exception as e:
			print(f'save_to_s3 Exception: {e}')
			raise Exception

	def get_from_s3(self,filepath:str)->pd.DataFrame:
		try:
			self.resource = boto3.resource('s3', 
			  region_name = self.region_name, 
			  aws_access_key_id = self.aws_access_key_id,
			  aws_secret_access_key= self.aws_secret_access_key)

			response = self.resource \
			.Bucket(self.bucket) \
			.Object(key= self.folder + filepath) \
			.get()

			return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')   
		except Exception as e:
			print(f'get_from_s3 Exception: {e}')
			raise Exception

	def file_exists_in_s3(self, filepath:str)->bool:
		try:
			self.resource = boto3.resource('s3', 
			  region_name = self.region_name, 
			  aws_access_key_id = self.aws_access_key_id,
			  aws_secret_access_key= self.aws_secret_access_key)

			response = self.resource \
			.Bucket(self.bucket) \
			.Object(key= self.folder + filepath) \
			.load()
			return True
		except Exception as e:
			print(f'file_exists_in_s3 Exception: {e}')
			print(self.folder + filepath)
			return False


class CustomAPIException(Exception):
	pass

class QueryRestApi:
	def __init__(self):
		self.endpoint = 'https://nickearl.net/api/v1.0/not-a-real-endpoint'
		self.auth = (REST_API_USER, REST_API_PASS)

	def run_query(self, community_names:list, start_date:str, end_date:str)->pd.DataFrame:
		print('rq: running va query')
		headers = {'Content-type': 'application/json'}
		payload = { 'community_names': community_names, 'start_date': start_date,'end_date': end_date}
		with requests.Session() as r:
			r.auth = self.auth
			success = False
			retries = 1
			i = 0
			while success == False and i <= retries:
				api_response = r.post(self.endpoint, json=payload, headers=headers)
				if api_response.status_code != 200:
					print('rq: query failed, retrying')
					i = i + 1
				else:
					print('rq: success')
					success = True
		try:
			result = json.loads(api_response.content)
			df = pd.DataFrame(result['overlaps'])
			return df
		except Exception as e:
			raise CustomAPIException(f'failed to retrieve data from the API: {result} | {e}')


class QueryAthena:

	def __init__(self, catalog:str='AwsDataCatalog', database:str='analytics', query=None):
		self.catalog = catalog
		self.database = database
		self.folder = conf['aws']['folder']
		self.bucket = conf['aws']['bucket']
		self.s3_output =  's3://' + self.bucket + '/' + self.folder
		self.region_name = conf['aws']['region']
		self.aws_access_key_id = conf['aws']['key']
		self.aws_secret_access_key = conf['aws']['secret']
		self.query = query

	def load_conf(self, q:str):
		print('QA: loading conf')
		try:
			self.client = boto3.client('athena', 
							  region_name = self.region_name, 
							  aws_access_key_id = self.aws_access_key_id,
							  aws_secret_access_key= self.aws_secret_access_key)
			response = self.client.start_query_execution(
				QueryString = q,
					QueryExecutionContext={
					'Catalog': self.catalog,
					'Database': self.database
					},
					ResultConfiguration={
					'OutputLocation': self.s3_output,
					}
			)
			self.filename = response['QueryExecutionId']
			print('Execution ID: ' + response['QueryExecutionId'])
			return response  
		except Exception as e:
			raise e
			print(e)         

	def run_query(self):
		queries = [self.query]
		for q in queries:
			res = self.load_conf(q)
		try:              
			query_status = None
			while query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
				query_status = self.client.get_query_execution(QueryExecutionId=res["QueryExecutionId"])['QueryExecution']['Status']['State']
				print(query_status)
				if query_status == 'FAILED' or query_status == 'CANCELLED':
					raise Exception(f'Athena query with the string "{self.query[:100]}" failed or was cancelled')
				time.sleep(10)
			print(f'Query "{self.query}" finished.')

			df = self.obtain_data()
			return df

		except Exception as e:
			print(e)      

	def obtain_data(self):
		try:
			self.resource = boto3.resource('s3', 
								  region_name = self.region_name, 
								  aws_access_key_id = self.aws_access_key_id,
								  aws_secret_access_key= self.aws_secret_access_key)

			response = self.resource \
			.Bucket(self.bucket) \
			.Object(key= self.folder + self.filename + '.csv') \
			.get()

			return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')   
		except Exception as e:
			print(e)  
			
	def run_query_async(self):
		print('QA: running query async')
		queries = [self.query]
		o = []
		for q in queries:
			o.append(self.load_conf(q))
		return o

	def poll_status(self,ex_id:str)->str:
		self.client = boto3.client('athena', 
			region_name = self.region_name, 
			aws_access_key_id = self.aws_access_key_id,
			aws_secret_access_key= self.aws_secret_access_key)
		query_status = None
		try:       
			print('QA: trying to get get status')       
			query_status = self.client.get_query_execution(QueryExecutionId=ex_id)['QueryExecution']['Status']['State']
			print(f'QUERY {ex_id} | {query_status}')
			if query_status == 'FAILED' or query_status == 'CANCELLED':
				print(f'Athena query "{ex_id}" failed or was cancelled')
				return query_status
			elif query_status == 'QUEUED' or query_status == 'RUNNING' or query_status is None:
				return query_status
			else:
				return query_status

		except Exception as e:
			print(e)           

	def obtain_data_async(self,execution_id,metadata=False):
		try:
			self.resource = boto3.resource('s3', 
								  region_name = self.region_name, 
								  aws_access_key_id = self.aws_access_key_id,
								  aws_secret_access_key= self.aws_secret_access_key)
			if metadata == False:
				filekey = self.folder + str(execution_id) + '.csv'
			else:
				filekey = self.folder + str(execution_id) + '.csv.metadata'
			response = self.resource \
			.Bucket(self.bucket) \
			.Object(key= filekey) \
			.get()
			return pd.read_csv(io.BytesIO(response['Body'].read()), encoding='utf8')   
		except Exception as e:
			print(e)  

	def run_multiple_queries(self, query_map:dict, poll_interval:int=5, on_progress=None, prev_steps:int=0,post_steps:int=0):
		"""
		Query map looks like:
		{
			'query_name': 'SELECT * FROM table',
			...
		}
		"""
		print('QA: Running multiple queries')
		for name, query_or_id in query_map.items():
			print(name)
			print(query_or_id)
		results = {}
		submissions = {}
		total = len(query_map)

		for name, query_or_id in query_map.items():
			if query_or_id and isinstance(query_or_id, str) and query_or_id.startswith("existing:"):
				# Poll an existing query
				execution_id = query_or_id.split("existing:")[1]
				submissions[name] = {"qa": QueryAthena(), "execution_id": execution_id}
			elif query_or_id:  # Submit a new query
				qa = QueryAthena(query=query_or_id)
				resp_list = qa.run_query_async()
				if resp_list and len(resp_list) > 0:
					execution_id = resp_list[0]['QueryExecutionId']
					submissions[name] = {"qa": qa, "execution_id": execution_id}
				else:
					results[name] = {"status": "FAILED", "df": None}

		# Poll statuses
		pending_names = list(submissions.keys())
		while pending_names:
			time.sleep(poll_interval)
			for name in pending_names[:]:
				info = submissions[name]
				status = info['qa'].poll_status(info['execution_id'])

				if status in ["FAILED", "CANCELLED"]:
					results[name] = {"status": status, "df": None}
					pending_names.remove(name)
				elif status == "SUCCEEDED":
					results[name] = {"status": "SUCCEEDED", "df": None}
					pending_names.remove(name)
			# If anything finished in this pass, call on_progress
			done = sum(
				1 for k,v in results.items() if v['status'] in ["SUCCEEDED","FAILED","CANCELLED"]
			)
			if on_progress:
				on_progress(done, total, prev_steps=prev_steps, post_steps=post_steps)

		# Fetch data for successful queries
		for name, info in submissions.items():
			if results[name]["status"] == "SUCCEEDED":
				ex_id = info["execution_id"]
				qa = info["qa"]
				try:
					df = qa.obtain_data_async(ex_id)
					results[name]["df"] = df
				except Exception as e:
					print(f"Error fetching data for query '{name}': {e}")
					results[name]["status"] = "FAILED"

		return results

	def run_queries_with_cache(self, queries:dict, force_refresh:bool=False, poll_interval:int=5, on_progress=None, prev_steps:int=0, post_steps:int=0, cache_ttl:int=None)->dict:
		"""
		Queries is a dictionary of query configurations with the following structure:
			{
		 		'query_name': {
					'query': 'SELECT * FROM table',
					'cache_key': 'cache_key_here'
		 		}
			}
		Returns a dictionary of query results with the same keys as the input queries.
		Checks cache first and only runs queries that are not cached.
		Implements a Redis lock to prevent multiple gunicorn workers from trying to run the same query 
		"""
		r = redis.from_url(REDISCLOUD_URL)
		cache_ttl = cache_ttl or 60*60*24*30  # 30 days
		missing_queries = {}
		cached_results = {}
		
		for name,query_configs in queries.items():			
			if not force_refresh:
				cache_key = query_configs['cache_key']

				try:
					print(f'QA: Checking cache for: {cache_key}')
					# Check if data is already cached
					c_data = r.get(cache_key)
					if c_data:
						cached_results[name] = pd.DataFrame(json.loads(c_data))
						print(f'Loaded cached data for {name}')
						continue
				
					# Check for existing lock
					print(f'Checking for existing lock: {f'_{cache_key}'}')
					lock_status = r.get(f'_{cache_key}')
					print(f'Lock status: {lock_status}')
					if lock_status:
						execution_id = lock_status.decode('utf-8')
						print(f"Query '{name}' is already in progress with execution ID: {execution_id}. Polling...")
						missing_queries[name] = f"existing:{execution_id}"
						continue

					# Acquire lock and submit the query
					print(f"Submitting query for '{name}'...")
					self.catalog = "AwsDataCatalog"
					self.database = "analytics"
					self.query = query_configs['query']
					resp_list = self.run_query_async()
					if resp_list and len(resp_list) > 0:
						execution_id = resp_list[0]['QueryExecutionId']
						r.set(f'_{cache_key}', execution_id, nx=True, ex=3600)  # Lock for 1 hour
						print(f"Execution ID for '{name}': {execution_id}")
						missing_queries[name] = f"existing:{execution_id}"  # Add to missing_queries for polling
					else:
						print(f"Failed to submit query for '{name}'")

				except Exception as e:
					print(f"Error processing lock for '{name}': {e}")
					cached_results[name] = pd.DataFrame()  # Fallback to an empty DataFrame

		# Run or poll queries
		
		results = self.run_multiple_queries(missing_queries, poll_interval=poll_interval, on_progress=on_progress, prev_steps=prev_steps, post_steps=post_steps)
		
		# Cache results and release locks
		for name, result in results.items():
			if result['status'] == 'SUCCEEDED' and result['df'] is not None:
				cache_key = queries[name]['cache_key']
				try:
					print(f'attempting to cache result for {name}: {cache_key}, num rows: {result['df'].shape[0]}')
					r.set(cache_key, json.dumps(result['df'].to_dict(orient='records')))
					r.expire(cache_key, cache_ttl)
					print(f"Cached result for '{name}'")
				except Exception as e:
					print(f"Error caching result for '{name}': {e}")
			# Release lock after query is done
			r.delete(f'_{cache_key}')

		# Combine results
		final_results = {}
		for name in queries.keys():
			if name in results and results[name]['status'] == 'SUCCEEDED':
				final_results[name] = results[name]['df']
			else:
				final_results[name] = cached_results.get(name, None)

		return final_results



# Google Search Console
"""
Returns a dataframe containing the results of a Google Search Console API query

Example Usage

####
Initialize object:
####

'key_file' = './fandom-bi-0d6bd93658ba.json'

a = SearchData(key_file)

#####
Get lists of sites we have permission to access
#####
a.permissions

#####
Pass an object to set GSC query parameters and return response to dataframe:
#####
conf = {
	 'domain': 'sc-domain:fandom.com', #ie, from a.permissions
	 'startDate': '2024-03-31',
	 'endDate': '2024-03-31',
	 'dimensions': ['page', 'date','query'], # all available: ['date','country','device','page','query']
	 'rowLimit': 25_000 # Max 25000, formatted with underscore as thousands sep
}

df = a.get_search_data(conf)
"""

class SearchData:
	def __init__(self):
		print('Initializing SearchData object')
		self.key_file = conf['gsc']['key_file']
		self.api_service_name = 'webmasters'
		self.api_version = 'v3'
		self.scope = ['https://www.googleapis.com/auth/webmasters.readonly']
	
		 # Auth on initiatilization
		credentials = service_account.Credentials.from_service_account_file(self.key_file, scopes=self.scope)
		self.service = build(self.api_service_name, self.api_version, credentials=credentials)
		self.permissions = self.service.sites().list().execute()
	
	def gsc_query(self, client: Resource, domain, payload: Dict[str, str]) -> Dict[str, any]:
		response = client.searchanalytics().query(siteUrl=domain, body=payload).execute()
		return response
		
	def get_search_data(self, queryConfigObject):
		print('Pulling GSC data...')
		response_rows = []
		i=0	
		while True:
			payload = {
				 'startDate': queryConfigObject['startDate'],
				 'endDate': queryConfigObject['endDate'],
				 'dimensions': queryConfigObject['dimensions'],
				 'rowLimit': queryConfigObject['rowLimit'],
				 'startRow': i * queryConfigObject['rowLimit'],
			}
			response = self.gsc_query(self.service, queryConfigObject['domain'], payload)
		
			if response.get("rows"):
			#print('got some rows')
				response_rows.extend(response["rows"])
				i += 1
			else:
				#print('no more rows')
				break
		
			print(f"Collected {len(response_rows):,} rows")
			odf = pd.DataFrame(response_rows)
			# Clean up the data
			odf[queryConfigObject['dimensions']] = pd.DataFrame(odf["keys"].tolist(), index=odf.index)
			odf['date'] = pd.to_datetime(odf['date'])
			odf.drop('keys', axis=1, inplace=True)
		return odf


# Amplitude lookup tables
class AmplitudeTables:
	"""
	Simple CRUD operations for Amplitude Lookup Tables API
	Mostly determined via trial and error as I think I'm the only one asking Amplitude for API documentation for this!
	"""

	def __init__(self,brand:str):
		print(f'>> Connecting to Amplitude Lookup Tables for {str(brand.lower())}...')
		self.brand = brand.lower()
		self.auth = (conf['amp'][self.brand]['key'], conf['amp'][self.brand]['secret'])
		self.base_endpoint = conf['amp']['base_endpoint']
		self.tables = []
		self.get_table_status()
		#self.refresh_job_schedule()

	def get_table_status(self):
		with requests.Session() as r:
			r.auth = self.auth
			api_response = r.get(self.base_endpoint)
			for table in json.loads(api_response.text)['data']:
				t = {}
				for key,val in table.items():
					t[key] = val
				self.tables.append(t)
			 #print(self.tables)
		return self

	def create_table(self,file_path:str,table_name:str=''):
		if not table_name:
			table_name = os.path.basename(file_path).split('/')[-1]
		with open(file_path) as g, open(file_path) as h:
			files = {'file': ('new_csv.csv', g, 'text/csv')}
			source_property = pd.read_csv(h, sep=",", header=0).columns[0]
			payload = {'property_group_type':'User', 'property_type':'event', 'property_name':source_property}
			with requests.Session() as r:
				r.auth = self.auth
				api_response = r.post(self.base_endpoint+'/'+ table_name, payload, files=files) 
		return api_response

	def update_table(self,filepath,table_name=''):
		if not table_name:
			table_name = os.path.basename(filepath).split('/')[-1]
		with open(filepath) as g, open(filepath) as h:
			files = {'file': ('new_csv.csv', g, 'text/csv')}
			source_property = pd.read_csv(h, sep=",", header=0).columns[0]
			payload = {'property_group_type':'User', 'property_type':'event', 'property_name':source_property}
			with requests.Session() as r:
				r.auth = self.auth
				api_response = r.patch(self.base_endpoint+'/'+ table_name, payload, files=files)		
		return(api_response)


	def delete_table(self,table_name):
		with requests.Session() as r:
			r.auth = self.auth
			api_response = r.delete(self.base_endpoint + '/' + table_name + '?force=True')
		return api_response

	def check_if_table_exists(self,table_name):
		with requests.Session() as r:
			r.auth = self.auth
			api_response = r.get(self.base_endpoint)
			rs = pd.DataFrame(json.loads(api_response.text)['data'])
			k = list(rs['name'])
			if table_name in k:
				print('Lookup table already exists')
				return True
			else:
				print('No existing lookup table found')
				return False
			return False
