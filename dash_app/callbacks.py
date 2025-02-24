import os, time, json, hashlib, io, re, zipfile, pytz
from datetime import date, datetime, timedelta
import dash
from dash import html, dcc, Input, Output, State, ALL, MATCH
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_app import GlobalUInterface, HelixQueryBuilder, MagiScheduler
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
import redis
import biutils as bi
from dotenv import load_dotenv
from connectors import QueryAthena
from conf import global_config
from transformer import UInterface as tf_ui
from flask import session

load_dotenv()
REDISCLOUD_URL = os.environ['REDISCLOUD_URL']
CACHE_KEY = global_config['app_prefix']
if global_config['enable_google_auth']:
	ENCRYPTION_SECRET_KEY = os.environ['ENCRYPTION_SECRET_KEY']
b = bi.BiUtils()

def register_callbacks(app):
	from app import BACKGROUND_CALLBACK_MANAGER

#######################
# Global
#######################

	app.clientside_callback(
		"""
		function(n_intervals) {
			if (n_intervals > 0) {
				window.location.reload(true);
			}
			return ''; 
		}
		""",
		Output('full-refresh-script','children'),
		Input('full-refresh-interval','n_intervals'),
	)

	@app.callback(
		Output('offcanvas-sidebar','is_open'),
		Input('nav-logo','n_clicks')
	)
	def open_sidebar(n_clicks):
		print('[' + str(datetime.now()) + '] | '+ '[open_sidebar] | ' + str(dash.ctx.triggered_id))
		if dash.ctx.triggered_id == None:
			raise PreventUpdate
		if n_clicks !=None:
			return True

	@app.callback(
		Output('loading-card-quote-container','children'),
		Input('quote-refresh-interval','n_intervals'),
		background=True,
		manager=BACKGROUND_CALLBACK_MANAGER,
	)
	def update_quotes(n_intervals):
		# print('[' + str(datetime.now()) + '] | '+ '[update_quotes] | ' + str(dash.ctx.triggered_id))
		b = bi.BiUtils()
		q = b.get_quote()
		return q

	@app.callback(
		Output('download-results-downloader', 'data'),
		Input('download-results-button','n_clicks'),
		State('download-results-store','data'),
		prevent_initial_call=True
	)
	def download_files(n_clicks, data):
		print(f'[{datetime.now()}] | [download_files] | trig_id: [{dash.ctx.triggered_id}]')
		if n_clicks == None:
			raise PreventUpdate
		else:
			try:
				results = json.loads(data)
			except Exception as e:
				print(f'Error loading data from store: {e}')
				raise PreventUpdate
			for key, value in results.items():
				results[key] = pd.DataFrame(value)
			zip_buffer = io.BytesIO()
			with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
				for name,df in results.items():
					csv_buffer = io.StringIO()
					df.to_csv(csv_buffer, index=False)
					zf.writestr(f"{name}.csv", csv_buffer.getvalue())

			zip_buffer.seek(0)  # Rewind the buffer for reading
			return dcc.send_bytes(zip_buffer.getvalue(), 'results.zip')
			# single file version:
			# df = pd.DataFrame().from_dict(json.loads(data))
			# csv_string = df.to_csv(index=False)
			# return dcc.send_string(csv_string, 'results.csv')

	def start_run_status(tool_key, run_id, progress_list_values=['Loading...']):

		r = redis.from_url(REDISCLOUD_URL)
		init_time = datetime.now().isoformat()
		run_durations_key = f'{tool_key}:run_durations'
		run_id_key = f'{tool_key}:active_queries:{run_id}'
		recent_durations = r.lrange(run_durations_key, 0, -1)
		recent_durations = [json.loads(duration) for duration in recent_durations]
		average_duration = 0
		if not recent_durations:
			print(f'No recent duration data found for key: {run_durations_key}')
		else:
			recent_avg_minutes, recent_avg_seconds = divmod(average_duration, 60)
			print(f'Average analysis duration of the most recent 5 runs: {int(recent_avg_minutes)}:{int(recent_avg_seconds):02d}')
			average_duration = sum(recent_durations) / len(recent_durations)
		payload = {
			'init_time': init_time,
			'average_duration': average_duration,
			'progress_list_values': progress_list_values}
		r.set(run_id_key, json.dumps(payload))
		r.expire(run_id_key, 1800)  # Set the key to expire after 30 minutes (1800 seconds)
		print(f'Set run status metadata values in Redis for key: {run_id_key}')

	def end_run_status(tool_key, run_id):
		r = redis.from_url(REDISCLOUD_URL)
		run_id_key = f'{tool_key}:active_queries:{run_id}'
		run_durations_key = f'{tool_key}:run_durations'
		run_data = json.loads(r.get(run_id_key))
		run_duration = datetime.now() - datetime.fromisoformat(run_data['init_time'])
		print('Caching run duration')
		r.lpush(run_durations_key, json.dumps(run_duration.total_seconds()))
		r.ltrim(run_durations_key, 0, 4)  # Keep only the most recent 5 durations
		r.expire(run_id_key, 30)
		print(f'Cleared run status metadata values from Redis for key: {run_id_key}')
	
	
	@app.callback(
		Output('loading-modal-text','children'),
		Output('loading-modal-list','children'),
		Output('loading-modal-average-duration','children'),
		Input('loading-modal-bar','value'),
		Input('loading-modal-refresh-interval','n_intervals'),
		State('loading-modal-bar','value'),
		State('loading-modal-bar','max'),
		State('loading-modal-data','data'),
		State('session-id-store','data'),
	)
	def display_loading_text(input_progress_value,n_intervals,progress_value,max_value, loading_modal_data, session_id_data):
		print(f'[{datetime.now()}] | [display_loading_text] | trig_id: [{dash.ctx.triggered_id}] | progress_value: [{progress_value}]')
		message = 'Loading...'
		average_duration = None
		recent_avg_minutes = None
		recent_avg_seconds = None
		run_duration = None
		if max_value == None:
			max_value = 0
		steps = []
		if loading_modal_data != None:
			try:
				r = redis.from_url(REDISCLOUD_URL)
				run_key = f'{json.loads(loading_modal_data)}:active_queries:{str(session_id_data)}'
				data = json.loads(r.get(run_key))
				average_duration = data['average_duration']
				init_time = data['init_time']
				progress_list_values = data['progress_list_values']
				recent_avg_minutes, recent_avg_seconds = divmod(average_duration, 60)
				run_duration = datetime.now() - datetime.fromisoformat(init_time)
				run_minutes, run_seconds = divmod(int(run_duration.total_seconds()), 60)
				steps = list(progress_list_values)
				message = str(steps[progress_value])
			except Exception as e:
				pass
		checklist = []
		for step in steps:
			item = dbc.ListGroupItem([
				dbc.Stack([
					html.I(className='bi bi-hourglass',style={'color':'gray'}),
					step,
				],direction='horizontal',gap=3),
			],className='loading-list')
			checklist.append(item)

		for i in range(max_value-1):
			if i <= progress_value:
				try:
					checklist[i].children[0].children[0] = html.I(className='bi bi-check-circle-fill',style={'color':'green'})
				except Exception as e:
					pass

		duration_content = dbc.ListGroupItem([
			dbc.Stack([
				dbc.Stack([
					html.I(className='bi bi-clock',style={'color':'white'}),
					html.Span(f'Running: {int(run_minutes)}:{int(run_seconds):02d}',style={'font-weight':'bold','color':'white'}),
				],direction='horizontal',gap=3,className='align-items-center justify-content-center') if run_duration != None else [],
				dbc.Stack([
					' Average Run Time (Last 5 Runs): ',
					html.Span(f'{int(recent_avg_minutes)}:{int(recent_avg_seconds):02d}',style={'font-weight':'bold','color':'white'}),
				],direction='horizontal',gap=3,className='align-items-center justify-content-center') if average_duration != None else [],
			],direction='horizontal',gap=3,className='align-items-center justify-content-center')
		],className='loading-list')

		return message, checklist, duration_content

#######################
# Transformer
#######################

	@app.callback(
		Output('manage-schedule-modal','is_open'),
		Input('tf-manage-schedule-all', 'n_clicks'),
	)
	def open_manage_schedule_modal(n_clicks):
		print('[' + str(datetime.now()) + '] | '+ '[open_manage_schedule_modal] | ' + str(dash.ctx.triggered_id))
		if dash.ctx.triggered_id == None:
			raise PreventUpdate
		if n_clicks !=None:
			return True

	@app.callback(
		Output('tf-alert-bar-credentials','children',allow_duplicate=True),
		Output('tf-app-card-container','children'),
		Output('tf-google-auth-status-container', 'children'),
		Output('tf-current-time-container-utc','children'),
		Output('tf-current-time-container-local','children'),
		Input('tf-refresh-interval','n_intervals'),
		Input('tf-transform-schedules', 'data'),
		State('tf-transform-schedules', 'data'),
		State('tf-local-tz','value'),
		State('session-id-store','data'),
	)
	def refresh_interval_tick(n_intervals, input_data, data, tz_set, session_id):
		print('[' + str(datetime.now()) + '] | '+ '[refresh_interval_tick] | ' + str(dash.ctx.triggered_id))

		def check_google_credentials(session_id):
			from app import AuthCredentials
			auth_manager = AuthCredentials(CACHE_KEY,ENCRYPTION_SECRET_KEY)
			creds = auth_manager.get_google_credentials(session_id)
			return creds

		r = redis.from_url(REDISCLOUD_URL)
		ui = tf_ui()
		cards = []
		data = None
		for k,v in ui.clients.items():
			try:
				data = json.loads(r.get(f'tf_schedule:{hashlib.md5(k.encode('utf-8')).hexdigest()}'))
			except Exception as e:
				#print(f'Could not get schedule for {k} from redis store: {e}')
				pass
			cardinfo = None
			try:
				cardinfo = data
			except (TypeError,KeyError) as e:
				print(f'tf schedule for {k} is null: {e}')
				pass
			card = ui.create_app_card(k,cardinfo,tz_set)
			cards.append(card)

		alert = None
		try:
			creds = check_google_credentials(session_id)
			if creds == None:
				raise Exception('No credentials found')
		except Exception as e:
			print('***************************')
			print(f'Error getting google credentials: {e}')
			alert_button = dbc.Button(
				dbc.Stack([html.I(className='bi bi-person-fill-lock'),'Reauthorize'],direction='horizontal',gap=3,className='align-items-center justify-content-between'),
				id='reauthorize-alert-button',
				href='/login',
				external_link=True,
				className='p-2 ms-auto'
				)
			alert = dbc.Stack([
				show_alert('Google credentials expired, reauthorize',accessory=alert_button),
				],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
			pass
		auth_status_box = []
		keys_to_get = ['email','name','expires_at']
		try:
			status_keys = []
			for k,v in creds.items():
				for k2,v2 in v.items():
					if k2 in keys_to_get:
						status_keys.append((k2,v2))
			status_keys.append(('expires in',creds['google_oauth_token']['expires_at'] - time.time()))
			auth_status_box = [
				dbc.Stack([
					html.Img(src=creds['user_info']['picture']),
					dbc.Stack(
						[html.Span(f'{k}: {v}') for k,v in status_keys ]
					,gap=3,className='align-items-center justify-content-between'),
				],direction='horizontal',gap=3,className='align-items-center justify-content-between'),
			]
		except Exception as e:
			print(f'Error rendering google credential status: {e}')
			pass
		utc_time_box = [
			html.Span(f'{datetime.utcnow().isoformat()}')
		]
		local_time_box = [
			html.Span(f'{datetime.now().astimezone(pytz.timezone(tz_set)).isoformat()}'),
		]
		return alert, cards, auth_status_box, utc_time_box, local_time_box
		

	@app.callback(
		Output('edit-schedule-modal','is_open'),
		Output('edit-schedule-modal','children'),
		Input({'type':'tf-client-edit-schedule-button','index':ALL},'n_clicks'),
		#State('tf-transform-schedules', 'data'),
		State('tf-local-tz','value'),
		prevent_initial_call=True

	)
	def edit_transform_schedule(n_clicks,tz_set):
		print('[' + str(datetime.now()) + '] | '+ '[edit_transform_schedule] | ' + str(dash.ctx.triggered_id))
		app_name = dash.ctx.triggered_id['index']
		key = str(dash.ctx.triggered_id).replace(' ','').replace("'",'"')
		trig_clicks = dash.ctx.inputs[f'{key}.n_clicks']
		if dash.ctx.triggered_id == None or trig_clicks == None:
			raise PreventUpdate
		elif trig_clicks > 0:
			data_for_editor = None
			try:
				r = redis.from_url(REDISCLOUD_URL)
				data_for_editor = json.loads(r.get(f'tf_schedule:{hashlib.md5(app_name.encode('utf-8')).hexdigest()}'))
				print(f'edit got: {data_for_editor}')
			except Exception as e:
				print('edit: error getting schedule from redis')
				pass
			ui = tf_ui()
			editor = ui.render_schedule_editor(app_name, data_for_editor, tz_set)
			return True, editor
		else:
			raise PreventUpdate

	@app.callback(
		Output('edit-schedule-modal','is_open',allow_duplicate=True),
		Output('tf-transform-schedules', 'data'),
		Input({'type':'tf-client-save-schedule-button','index':ALL},'n_clicks'),
		State({'type':'tf-client-schedule-enabled','index':ALL}, 'on'),
		State({'type':'tf-client-schedule-frequency','index':ALL}, 'value'),
		State({'type':'tf-client-schedule-start-time-hours','index':ALL}, 'value'),
		State({'type':'tf-client-schedule-start-time-mins','index':ALL}, 'value'),
		State({'type':'tf-client-schedule-day-of-week','index': ALL}, 'value'),
		State({'type':'tf-client-schedule-day-of-month','index': ALL}, 'value'),
		State('tf-transform-schedules', 'data'),
		State('tf-local-tz','value'),
		State('session-id-store','data'),
		prevent_initial_call=True
	)
	def save_transform_schedule(n_clicks,enabled,frequency,start_time_hours,start_time_mins,day_of_week,day_of_month,tf_schedule,tz_set,session_id):
		print('[' + str(datetime.now()) + '] | '+ '[save_transform_schedule] | ' + str(dash.ctx.triggered_id))
		app_name = dash.ctx.triggered_id['index']
		key = str(dash.ctx.triggered_id).replace(' ','').replace("'",'"')
		trig_clicks = dash.ctx.inputs[f'{key}.n_clicks']
		if dash.ctx.triggered_id == None or trig_clicks == None:
			raise PreventUpdate
		elif trig_clicks > 0:
			new_tf_schedule = {}
			try:
				r = redis.from_url(REDISCLOUD_URL)
				data = json.loads(r.get(f'tf_schedule:{hashlib.md5(app_name.encode('utf-8')).hexdigest()}'))
			except Exception as e:
				print('save: error getting schedule from redis')
				pass
			enabled = dash.ctx.states[f'{{"index":"{app_name}","type":"tf-client-schedule-enabled"}}.on']
			frequency = dash.ctx.states[f'{{"index":"{app_name}","type":"tf-client-schedule-frequency"}}.value']
			hour = dash.ctx.states[f'{{"index":"{app_name}","type":"tf-client-schedule-start-time-hours"}}.value']
			minute = dash.ctx.states[f'{{"index":"{app_name}","type":"tf-client-schedule-start-time-mins"}}.value']
			day_of_week = dash.ctx.states[f'{{"index":"{app_name}","type":"tf-client-schedule-day-of-week"}}.value']
			day_of_month = dash.ctx.states[f'{{"index":"{app_name}","type":"tf-client-schedule-day-of-month"}}.value']
			new_tf_schedule = {
				'enabled': enabled,
				'frequency': frequency,
				'hour': hour or '00',
				'minute': minute or '00',
				'day_of_week': day_of_week or 'Sunday',
				'day_of_month': day_of_month or '1',
				'last_run': None,
				'next_run': None,
			}
			print('saving schedule as: ')
			print(f'{new_tf_schedule}')
			print('saving to redis')
			try:
				r = redis.from_url(REDISCLOUD_URL)
				r.set(f'tf_schedule:{hashlib.md5(app_name.encode('utf-8')).hexdigest()}', json.dumps(new_tf_schedule))
			except Exception as e:
				print(f'error saving to redis: {e}')
			print('updating celery schedule')
			job_id = f'scheduled_tf:{app_name}'
			ui = tf_ui()
			queue_name = f'{global_config['app_prefix']}-queue'
			tf = ui.clients.get(app_name)['transform']
			magi = MagiScheduler(app.server.extensions["celery"],tz=tz_set,queue=queue_name)
			c_schedule=magi.make_cron_schedule(new_tf_schedule)
			jid = magi.add_job(job_id,f'{tf}',c_schedule, new_tf_schedule['enabled'],app_name,session_id=session_id)
			print(f'Job added: {jid}')

			return False, json.dumps(new_tf_schedule)
		else:
			raise PreventUpdate


	@app.callback(
		Output('tf-transform-staging','data'),
		Input({'type':'tf-client-manual-run-button','index':ALL}, 'n_clicks'),
		prevent_initial_call=True,
	)
	def stage_transform(n_clicks):
		print('[' + str(datetime.now()) + '] | '+ '[stage_transform] | ' + str(dash.ctx.triggered_id))
		app_name = dash.ctx.triggered_id['index']
		key = str(dash.ctx.triggered_id).replace(' ','').replace("'",'"')
		trig_clicks = dash.ctx.inputs[f'{key}.n_clicks']
		if dash.ctx.triggered_id == None or trig_clicks == None:
			raise PreventUpdate
			#return None
		elif trig_clicks > 0:
		# if dash.ctx.triggered_id == None:
		# 	raise PreventUpdate
		# elif n_clicks[0] != None and n_clicks[0] != None and n_clicks[0] > 0:
			print(f'Staging refresh of : {app_name}')
			return {'stage':app_name}
		else:
			raise PreventUpdate
			#return None



	def show_alert(text, color='warning', accessory=None):
		icon = None
		dismissable = True
		if color in ['warning','danger']:
			icon = 'bi bi-exclamation-triangle-fill'
		elif color == 'info':
			icon = 'bi bi-info-circle-fill'
		elif color == 'success':
			icon = 'bi bi-check-circle-fill'
		alert = dbc.Alert([
			dbc.Stack([
				html.I(className=icon),
				html.Span(text,className='me-auto'),
				accessory,
			],direction='horizontal',gap=3,className='align-items-center justify-content-between'),
		],color=color,dismissable=dismissable,style={'width':'100%'})
		return alert

	@app.callback(
		Output('tf-alert-bar','children'),
		Output('tf-transform-schedules', 'data',allow_duplicate=True),
		Input('tf-transform-staging', 'data'),
		State('tf-transform-schedules', 'data'),
		State('session-id-store','data'),
		background=True,
		manager=BACKGROUND_CALLBACK_MANAGER,
		running=[
			(Output('tf-transform-run-progress','style'), None, {'display':'None'}),
			#(Output('crossover-run-analysis-button', 'disabled'), True, False),
			#(Output('crossover-run-analysis-button', 'children'), [dbc.Spinner(size='sm'),' Analyzing...'], [html.I(className='bi bi-play-fill'),' Analyze Crossover']),
		],
		progress=[Output('tf-transform-run-progress','value'), Output('tf-transform-run-progress','max')],
		prevent_initial_call=True,
	)
	def background_run_transform(set_progress, stage_data, schedule_data, session_id):
		print('[' + str(datetime.now()) + '] | '+ '[background_run_transform] | ' + str(dash.ctx.triggered_id))
		if dash.ctx.triggered_id == None:
			raise PreventUpdate
		if stage_data != None:
			print(f'stage data is: {stage_data}')
			set_progress((0, 5))
			init_time = datetime.now()
			idx = stage_data['stage']
			print(f'Attempting to refresh: {idx}')
			magi = MagiScheduler(app.server.extensions["celery"])
			new_tf_schedule = {
				idx: magi.get_default_schedule(),
			}
			if schedule_data != None:
				new_tf_schedule = schedule_data
			time.sleep(1)
			set_progress((2, 5))
			time.sleep(1)
			set_progress((5, 5))
			# tfobjects = {
			# 	'Varys': TransformVarys
			# }
			# tf = tfobjects.get(idx)
			# tf(app.server)
			# job_id = f'runonce_tf:{idx}'
			ui = tf_ui()
			tf = ui.clients.get(idx)['tf_object']
			alert = show_alert(f'Successfully ran transform [{idx}]',color='success')
			try:
				# if idx == 'Varys':
				# 	tf(app.server)
				# else:
				# 	tf()
				tf(session_id=session_id)
			except Exception as e:
				print(f'Error running {idx}: {e}')
				alert = show_alert(f'Error running transform [{idx}]: {e}',color='warning')
				raise e
				pass
			# c_schedule=magi.make_cron_schedule(new_tf_schedule[idx],run_once=True)
			# jid = magi.add_job(job_id,f'tasks.{tf}',c_schedule, new_tf_schedule[idx]['enabled'])
			# print(f'Job scheduled for next minute: {jid}')
			# time.sleep(60)
			# print(f'Removing job from schedule: {jid}')
			# magi.delete_job(jid)
			try:
				new_tf_schedule[idx]['last_run'] = init_time.isoformat()
			except (KeyError,TypeError) as e:
				print(f'No existing key.  {e}')
				pass

			r = redis.from_url(REDISCLOUD_URL)
			key = f'tf_schedule:{hashlib.md5(idx.encode('utf-8')).hexdigest()}'
			rdata = None
			try:
				rdata = json.loads(r.get(key))
			except TypeError as e:
				print('No existing schedule data')
				pass
			print(f'data from redis is: {rdata}')
			if rdata == None:
				rdata = magi.get_default_schedule()
			rdata['last_run'] = init_time.isoformat()		
			r.set(key, json.dumps(rdata))

			set_progress((0, 5))
			return alert, new_tf_schedule


	@app.callback(
		Output({'type':'tf-client-schedule-day-of-week','index':MATCH}, 'className', allow_duplicate=True),
		Output({'type':'tf-client-schedule-day-of-week','index':MATCH}, 'disabled', allow_duplicate=True),
		Output({'type':'tf-client-schedule-day-of-month','index':MATCH}, 'className', allow_duplicate=True),
		Output({'type':'tf-client-schedule-day-of-month','index':MATCH}, 'disabled', allow_duplicate=True),
		Input({'type':'tf-client-schedule-frequency','index':ALL}, 'value'),
		State('tf-transform-schedules', 'data'),
		#prevent_initial_call=True,
	)
	def style_frequency_options(value,schedule_data):
		print('[' + str(datetime.now()) + '] | '+ '[style_frequency_options] | ' + str(dash.ctx.triggered_id))
		if dash.ctx.triggered_id == None:
			raise PreventUpdate
		else:
			try:
				app_name = dash.ctx.triggered_id['index']
				key = str(dash.ctx.triggered_id).replace(' ','').replace("'",'"')
				trig_val = dash.ctx.inputs[f'{key}.value']
				print(f'trigger value is: {trig_val}')
			except Exception as e:
				print(f'likely initial load: {e}')
				print('check for stored frequency value')
				try:
					trig_val = json.loads(schedule_data)[dash.ctx.triggered_id['index']]
					print(f'got stored val: {trig_val}')
				except Exception as ee:
					print(f'no stored val, using default 0: {ee}')
					trig_val = 0
					pass
				pass
			val_map = {
				0:('schedule-option-disabled',True,'schedule-option-disabled',True),
				1:(None,None,'schedule-option-disabled',True),
				2:('schedule-option-disabled',True,None,None),
			}
			o = val_map.get(trig_val)

			return o[0], o[1], o[2], o[3]

	@app.callback(
		Output('tf-manage-schedule-output-container', 'children',allow_duplicate=True),
		Input('tf-manage-schedule-delete-job', 'n_clicks'),
		State('tf-manage-schedule-delete-job-id','value'),
		prevent_initial_call=True,
	)
	def schedule_remove_job(n_clicks,job_id):
		print('[' + str(datetime.now()) + '] | '+ '[schedule_remove_job] | ' + str(dash.ctx.triggered_id))
		if dash.ctx.triggered_id == None:
			raise PreventUpdate
		else:
			magi = MagiScheduler(app.server.extensions["celery"])
			magi.delete_job(job_id)
			app_name = job_id.split(':')[-1]
			print(f'Job deleted: {job_id}')
			try:
				r = redis.from_url(REDISCLOUD_URL)
				r.delete(f'tf_schedule:{hashlib.md5(app_name.encode('utf-8')).hexdigest()}')
				# rdata = json.loads(r.get(f'tf_schedule:{hashlib.md5(app_name)}'))
				# print(f'remove job got: {rdata}')
				# rdata.pop(app_name,None)
				# print(f'after removal: {rdata}')
				# r.set('tf-transform-schedules',json.dumps(rdata))
			except Exception as e:
				print('save: error removing schedule from redis')
				pass
			return f'deleted job: {job_id}'

	@app.callback(
		Output('tf-manage-schedule-output-container', 'children',allow_duplicate=True),
		Input('tf-manage-schedule-list-jobs', 'n_clicks'),
		State('tf-local-tz','value'),
		prevent_initial_call=True,
	)
	def schedule_list_jobs(n_clicks, local_tz):
		print('[' + str(datetime.now()) + '] | '+ '[schedule_list_jobs] | ' + str(dash.ctx.triggered_id))
		if dash.ctx.triggered_id == None:
			raise PreventUpdate
		else:
			tz_set = 'UTC'
			try:
				tz_set = local_tz
			except Exception as e:
				pass
			print(f'trying to use tz: {tz_set}')
			magi = MagiScheduler(app.server.extensions["celery"],tz=tz_set)
			#jobs = magi.list_jobs()
			jobs = magi.jobs
			rows = []
			for k,v in jobs.items():
				o = dbc.Stack([
					html.Span(f'{k}:',style={'width':'100px'}),
					html.Span(f'{v}',style={'width':'200px'}),
				],direction='horizontal',gap=3)
				rows.append(o)

			return rows