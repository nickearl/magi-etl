import os, json, uuid, requests, re, time
from datetime import date, datetime, timedelta
import flask
from flask import Flask, request, make_response, session
import dash
from dash import CeleryManager
from dotenv import load_dotenv
import redis
from flask.helpers import get_root_path
from celery import Celery, Task
import biutils as bi
from conf import global_config


"""
Core dependencies:
	pip install flask gunicorn requests dash dash-bootstrap-components python-dotenv redis celery pandas numpy plotly pytz bs4

LLMs (Optional):
	pip install google-generativeai openai

Google Auth (Optional):
	pip install flask-dance google-auth google-auth-oauthlib oauthlib cryptography

Slack (Optional):
	pip install slack-sdk

Taxonomy Relationships (Optional):
	pip install scikit-learn scipy

Running the app:
	Open two terminals and run the following:
	flask --app app run -p 1701    # Run the app on port 1701
	celery -A app:celery_app worker --loglevel=INFO --concurrency=2 -Q {queue id}    # Run the celery worker, replace {queue id} with value like 'prefix-queue' where 'prefix' is your app prefix
"""

load_dotenv()

DEPLOY_ENV = os.environ['DEPLOY_ENV']
if DEPLOY_ENV.lower() == 'dev':
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
REDISCLOUD_URL = os.environ['REDISCLOUD_URL']
PORT = os.environ['PORT']
APP_NAME = global_config['app_name']
DISPLAY_NAME = global_config['display_name']
CACHE_KEY = global_config['app_prefix']
REDIRECT_PATH = f'/{re.sub(r'[_\s]+', '-', APP_NAME).lower()}'
DEFAULT_PORT = 1701
FLASK_SECRET_KEY = os.environ['FLASK_SECRET_KEY']
redis_client = redis.from_url(REDISCLOUD_URL)
cache_uuid = uuid.uuid4().hex

if global_config['enable_google_auth']:
	print('* Google Auth enabled * ')
	from flask_dance.contrib.google import make_google_blueprint, google
	from google.oauth2.credentials import Credentials
	from google.auth.transport.requests import Request
	from oauthlib.oauth2.rfc6749.errors import TokenExpiredError
	from cryptography.fernet import Fernet
	# Google auth
	CLIENT_SECRETS_FILE = 'key_oauth.json'
	GOOGLE_SCOPES = [
				"https://www.googleapis.com/auth/userinfo.email",
				"https://www.googleapis.com/auth/userinfo.profile",
				"https://www.googleapis.com/auth/spreadsheets.readonly",
				"https://www.googleapis.com/auth/drive.metadata.readonly",
				"openid",
			]
	GOOGLE_CLIENT_ID = os.environ['GOOGLE_CLIENT_ID']
	GOOGLE_CLIENT_SECRET = os.environ['GOOGLE_CLIENT_SECRET']
	GOOGLE_REDIRECT_URL = os.environ['GOOGLE_REDIRECT_URL']
	AUTHORIZED_DOMAINS = []
	try:
		AUTHORIZED_DOMAINS = json.loads(os.environ['AUTHORIZED_DOMAINS']) # List of authorized email domains to allow access via Google OAuth SSO
	except Exception as e:
		pass
	ENCRYPTION_SECRET_KEY = os.environ['ENCRYPTION_SECRET_KEY']


class AuthCredentials:
    def __init__(self, cache_key:str, encryption_secret_key:str):
        self.cache_key = cache_key
        self.cipher = Fernet(encryption_secret_key)

    def encrypt_data(self, data):
        return self.cipher.encrypt(data.encode()).decode()

    def decrypt_data(self, data):
        return self.cipher.decrypt(data.encode()).decode()

    def save_google_credentials(self, session_id, credentials):
        redis_client = redis.from_url(REDISCLOUD_URL)
        key = f"{self.cache_key}:session:{session_id}"
        encrypted_credentials = self.encrypt_data(json.dumps(credentials))
        redis_client.setex(
            key,
            timedelta(days=30),
            encrypted_credentials
        )

    def get_google_credentials(self, session_id):
        r = redis.from_url(REDISCLOUD_URL)
        key = f"{self.cache_key}:session:{session_id}"
        encrypted_credentials = r.get(key)

        if encrypted_credentials:
            credentials = json.loads(self.decrypt_data(encrypted_credentials.decode()))

            if 'google_oauth_token' in credentials:
                oauth_token = credentials['google_oauth_token']
                if time.time() >= oauth_token.get('expires_at', 0):
                    refreshed_token = self.refresh_google_token(oauth_token['refresh_token'])
                    if refreshed_token:
                        oauth_token.update(refreshed_token)
                        self.save_google_credentials(session_id, credentials)

            return credentials
        
        return None

    def refresh_google_token(self, refresh_token):
        data = {
            'client_id': GOOGLE_CLIENT_ID,
            'client_secret': GOOGLE_CLIENT_SECRET,
            'refresh_token': refresh_token,
            'grant_type': 'refresh_token'
        }

        response = requests.post('https://oauth2.googleapis.com/token', data=data)

        if response.status_code == 200:
            new_token = response.json()
            return {
                'access_token': new_token['access_token'],
                'expires_at': time.time() + new_token['expires_in']
            }
        else:
            print(f"Failed to refresh token: {response.text}")
            return None

if global_config['enable_slack']:
	print('* Slack enabled * ')
	from slack_sdk.signature import SignatureVerifier
	signature_verifier = None
	try:
		signature_verifier = SignatureVerifier(os.environ["SLACK_SIGNING_SECRET"])
	except Exception as e:
		print(f"Failed to create Slack SignatureVerifier: {str(e)}")
		pass

def create_celery_server(server):

	class FlaskTask(Task):
		def __call__(self, *args: object, **kwargs: object) -> object:
			with server.app_context():
				return self.run(*args, **kwargs)

	celery_app = Celery(
		server.name,
		imports = ('tasks',), # uncomment and add files like 'tasks.py' that scheduled tasks need access to as 'tasks'
		broker=REDISCLOUD_URL,
		backend=REDISCLOUD_URL,
		task_ignore_result=True,
		task_cls=FlaskTask,
		include=['callbacks'],
	)

	# Set a unique queue name per app to avoid issues when multiple apps share the same Redis server
	# Make sure to update your Procfile with the correct queue name ie "celery worker -A app:celery_app --loglevel=INFO --concurrency=2 -Q app-queue"
	queue_name = f'{global_config['app_prefix']}-queue'
	celery_app.conf.task_routes = {
		'app.*': {'queue': queue_name},
		'long_callback_*': {'queue': queue_name},
	}
	celery_app.conf.update(
		# event_serializer='json',
		# task_serializer='json',
		# result_serializer='json',
		# accept_content=['json'],
		beat_scheduler = 'redbeat.RedBeatScheduler',
		event_serializer = 'pickle',
		task_serializer = 'pickle',
		result_serializer = 'pickle',
		accept_content = ['pickle'],
		)
	celery_app.set_default()
	server.extensions["celery"] = celery_app
	return celery_app

def create_server():
	server = create_flask_server(port=PORT)
	return server

def create_dash_app(server):

	from dash_app import create_dash_app
	from callbacks import register_callbacks as register_callbacks
	print(f'Creating Dash app APP_NAME: {APP_NAME} at /{APP_NAME.replace(' ','-').lower()}/')
	register_dash_app(server, 'dash_app', APP_NAME, APP_NAME.replace(' ','-').lower(), create_dash_app, register_callbacks)

	return server


def register_dash_app(app, app_dir, title, base_pathname, create_dash_fun, register_callbacks_fun):
	import importlib
	meta_viewport = {"name": "viewport", "content": "width=device-width, initial-scale=1, shrink-to-fit=no"}
	with app.app_context():
		new_dash_app = create_dash_fun(
			server=app,
			url_base_pathname=f'/{base_pathname}/',
			assets_folder=get_root_path(__name__) + f'/{app_dir}/assets/',
			meta_tags=[meta_viewport],
			use_pages=True,
			pages_folder=get_root_path(__name__) + f'/{app_dir}/pages/',
		)
		new_dash_app.title = title

		# Register all pages configured in conf.py
		for page_name, page_config in global_config['pages'].items():
			if page_config['enabled']:
				print(f'Registering page: {page_name}')
				module = importlib.import_module(page_name)
				page_ui = getattr(module, 'UInterface')
				page_layout = getattr(module, 'create_app_layout')
				dash.register_page(
					page_config['display_name'],
					title=f'{DISPLAY_NAME} | {page_config["display_name"]}',
					path=page_config['path'],
					layout=page_layout(page_ui()))

		register_callbacks_fun(new_dash_app)


def create_flask_server(port=DEFAULT_PORT):
	server = flask.Flask(__name__)
	server.secret_key = FLASK_SECRET_KEY

	if global_config['enable_google_auth']:
		google_bp = make_google_blueprint(
			client_id=GOOGLE_CLIENT_ID,
			client_secret=GOOGLE_CLIENT_SECRET,
			redirect_url=GOOGLE_REDIRECT_URL,
			scope=GOOGLE_SCOPES,
		)
		google_bp.authorization_url_params["access_type"] = "offline"
		google_bp.authorization_url_params["prompt"] = "consent"  # Force re-consent to get a refresh_token

		server.register_blueprint(google_bp, url_prefix="/login")

	with server.app_context():

		@server.before_request
		def make_session_permanent():	
			flask.session.permanent = True
			server.permanent_session_lifetime = timedelta(days = 90)

		if global_config['enable_google_auth']:
			@server.route('/')
			def index():
				if "google_oauth_token" not in flask.session:
					return flask.redirect(flask.url_for("google.login"))
				try:
					return flask.redirect(REDIRECT_PATH)
				except TokenExpiredError:
					flask.flash("Session expired. Please log in again.", "warning")
					return flask.redirect(flask.url_for("google.login"))

			@server.route('/login')
			def callback():
				if not google.authorized:
					return flask.redirect(flask.url_for("google.login"))
				creds = AuthCredentials(CACHE_KEY, ENCRYPTION_SECRET_KEY)
				try:
					user_info = google.get("/oauth2/v1/userinfo").json()
					user_email = user_info.get("email", "").lower()
					user_domain = user_email.split('@')[-1]
					# Check if user email domain is authorized
					if len(AUTHORIZED_DOMAINS) > 0 and not user_domain in AUTHORIZED_DOMAINS:
						flask.abort(403, description="Access denied: Unauthorized email domain")
					
					session_id = str(uuid.uuid4())
					creds.save_google_credentials(session_id, {
						"user_info": user_info,
						"google_oauth_token": flask.session.get("google_oauth_token")
					})
					
					flask.session['session_id'] = session_id
					
					return flask.redirect(REDIRECT_PATH)
				except TokenExpiredError:
					session_id = flask.session.get("session_id")
					if not session_id:
						return flask.redirect(flask.url_for("google.login"))
					
					cred_data = creds.get_google_credentials(session_id)
					if not cred_data or "refresh_token" not in cred_data["google_oauth_token"]:
						return flask.redirect(flask.url_for("google.login"))
					
					refresh_token = cred_data["google_oauth_token"]["refresh_token"]
					refresh_response = requests.post(
						"https://oauth2.googleapis.com/token",
						data={
							"client_id": GOOGLE_CLIENT_ID,
							"client_secret": GOOGLE_CLIENT_SECRET,
							"refresh_token": refresh_token,
							"grant_type": "refresh_token"
						}
					)
					if refresh_response.ok:
						new_token = refresh_response.json()
						cred_data["google_oauth_token"]["access_token"] = new_token["access_token"]
						creds.save_google_credentials(session_id, cred_data)
						return flask.redirect(flask.url_for("callback"))
					else:
						return "Failed to refresh token. Please log in again.", 401
					
			@server.route('/login/revoke')
			def revoke():
				session_id = flask.session.get("session_id")
				if not session_id:
					return "No active session to revoke.", 400
				else:
					flask.session.clear()

				# Retrieve user credentials from Redis
				# b = bi.BiUtils()
				# cred_data = b.get_google_credentials(session_id)
				creds = AuthCredentials(CACHE_KEY, ENCRYPTION_SECRET_KEY)
				cred_data = creds.get_google_credentials(session_id)
				if not cred_data:
					print("DEBUG: No credentials found in Redis for session.")
					return "No credentials found in Redis.", 400

				print(f"DEBUG: Retrieved credentials: {cred_data}")
				try:
					if not cred_data:
						raise Exception("Google credentials not found for this session.")
					if 'google_oauth_token' in cred_data:
						token = cred_data.get("google_oauth_token", {}).get("access_token")
						refresh_token = cred_data.get("google_oauth_token", {}).get("refresh_token")
						token_uri = "https://oauth2.googleapis.com/token"  # Default Google OAuth token URI
						client_id = GOOGLE_CLIENT_ID
						client_secret = GOOGLE_CLIENT_SECRET
						raw_scopes = cred_data.get("google_oauth_token", {}).get("scope", "")
						if isinstance(raw_scopes, str):
							scopes = raw_scopes.split()  # Space-separated string
						elif isinstance(raw_scopes, list):
							scopes = raw_scopes
						else:
							raise ValueError("Invalid scope format. Expected string or list.")
						if not token:
							raise ValueError("Missing required 'access_token' in credentials")
						if not refresh_token:
							raise ValueError("Missing required 'refresh_token' in credentials")
						credentials = Credentials(
							token=token,
							refresh_token=refresh_token,
							token_uri=token_uri,
							client_id=client_id,
							client_secret=client_secret,
							scopes=scopes,
						)
						if credentials.expired and credentials.refresh_token:
							credentials.refresh(Request())
							creds.save_google_credentials(session_id, {
								'token': credentials.token,
								'refresh_token': credentials.refresh_token,
								'token_uri': credentials.token_uri,
								'client_id': credentials.client_id,
								'client_secret': credentials.client_secret,
								'scopes': credentials.scopes
							})
					else:
						raise Exception('No oauth token found in session data, please log in again')
					token = credentials.token
					if not token:
						print(f"DEBUG: Missing token in credentials: {credentials}")
						return "No valid token found in credentials.", 400

					# Revoke the token
					revoke_response = requests.post(
						"https://oauth2.googleapis.com/revoke",
						params={"token": token},
						headers={"content-type": "application/x-www-form-urlencoded"}
					)

					if revoke_response.status_code == 200:
						# Clear Redis and Flask session
						redis_client.delete(f"session:{session_id}")
						flask.session.clear()
						return "Credentials successfully revoked and cleared.", 200
					else:
						error_details = revoke_response.json()
						print(f"DEBUG: Failed to revoke credentials: {error_details}")
						return f"Failed to revoke credentials: {error_details.get('error', 'Unknown error')}", 400
				except Exception as e:
					print(f"DEBUG: Exception during revocation: {str(e)}")
					return f"An error occurred during revocation: {str(e)}", 500
		else:
			@server.route('/')
			def index():
				return flask.redirect(REDIRECT_PATH)
			
		if global_config['enable_slack']:
			@server.route('/slack/events', methods=['POST'])
			def slack_app():
				return make_response('ok', 200)

		return server
	
print('app.py successfuly initialized')
server = create_server()
print('Flask server successfuly initialized')
celery_app = create_celery_server(server)
print('Celery succesfully initialized')
BACKGROUND_CALLBACK_MANAGER = CeleryManager(celery_app)
print('Celery/Dash integration complete')
server = create_dash_app(server)
print('Dash app successfuly initialized')