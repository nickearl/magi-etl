import os, random
from dotenv import load_dotenv
import dash_bootstrap_components as dbc
from dash import html, dcc
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
from datetime import date, datetime
import redis
import biutils as bi
from dash_app import GlobalUInterface, HelixQueryBuilder, MagiScheduler, conv_tz
from connectors import QueryAthena, AmazonS3Connector
from conf import global_config
import dash_daq as daq
import redbeat
from redbeat import RedBeatSchedulerEntry as Entry
from redbeat.schedulers import RedBeatConfig
from celery.schedules import crontab
import tasks
import pytz

load_dotenv()
PAGE = 'transformer'
REDISCLOUD_URL = os.environ['REDISCLOUD_URL']
pd.set_option('future.no_silent_downcasting', True)

class UInterface:
	def __init__(self, helix_config=None,on_progress=None):
		self.conf = global_config['pages'][PAGE]
		print(f'Initializing {self.conf['display_name']} UI')
		self.init_time = datetime.now()
		self.query_builder = HelixQueryBuilder().from_dict(helix_config) if helix_config else HelixQueryBuilder()
		self.clients = {
			'ForecastData': {
				'name': 'ForecastData',
				'transform': 'tasks.run_transform_forecast',
				'tf_object': tasks.ForecastTracker,
			},
			'TrendingWikis': {
				'name': 'TrendingWikis',
				'transform': 'tasks.run_transform_trending',
				'tf_object': tasks.TransformTrendingWikis,
			},
			# 'KPISummary': {
			# 	'name': 'KPISummary',
			# 	'transform': 'tasks.run_transform_kpi_summary',
			# 	'tf_object': tasks.TransformKpiSummary,
			# },
			'MonetizationMonitor': {
				'name': 'MonetizationMonitor',
				'transform': 'tasks.run_transform_mon',
				'tf_object': tasks.TransformMonetizationMonitor,
			},
			# 'WikiMetadata': {
			# 	'name': 'WikiMetadata',
			# 	'transform': 'tasks.run_wiki_metadata',
			# 	'tf_object': tasks.WikiMetadata,
			# },
			'FranchiseFactor': {
				'name': 'FranchiseFactor',
				'transform': 'tasks.run_transform_franchise',
				'tf_object': tasks.TransformFranchiseFactor,
			}
			
		}
		self.layout = {
			'status_box': dbc.Stack([
				dbc.Stack([
					dbc.Card([
						dbc.CardHeader(['Google Auth']),
						dbc.CardBody([
							dbc.Stack([],gap=3,className='align-items-center justify-content-center',id='tf-google-auth-status-container'),
						]),
					],style={'flex':'1'}),
					dbc.Card([
						dbc.CardHeader(['Current Time']),
						dbc.CardBody([
							dbc.Stack([
								dbc.Stack([
									html.Span('UTC: ',style={'font-weight':'bold'}),
									dbc.Stack([],gap=3,className='align-items-center justify-content-center',id='tf-current-time-container-utc'),
								],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
								dbc.Stack([
									html.Span([
										dcc.Dropdown(
											options=pytz.common_timezones,
											clearable=False,
											id='tf-local-tz',
											style={'width':'150px'},
											value='UTC',
										),
									],style={'font-weight':'bold'}),
									dbc.Stack([],gap=3,className='align-items-center justify-content-center',id='tf-current-time-container-local'),
								],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
							],gap=3,className='align-items-center justify-content-center'),
						]),
					],style={'flex':'2'}),
				],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
			],gap=3,className='align-items-center justify-content-center'),
			'app_card_container': dbc.Stack([
				dbc.Stack([],id='tf-alert-bar',gap=3),
				dbc.Stack([],id='tf-alert-bar-credentials',gap=3),
				dbc.Progress(value=0,striped=True,animated=True,color='#86D7DC',style={'background-color':'#3A434B'},id='tf-transform-run-progress'),
				dbc.Stack([],id='tf-app-card-container',gap=3),
			],gap=3,className='align-items-center justify-content-center'),
			'edit_schedule_modal': dbc.Modal([],size='lg',is_open=False,id='edit-schedule-modal'),
			'manage_schedule': dbc.Stack([
				dbc.Stack([
					dbc.Button(dbc.Stack([html.I(className='bi bi-list-ul'),'Manage Schedule'],direction='horizontal',gap=2), id='tf-manage-schedule-all', className='p-1',color='secondary'),
				],direction='horizontal',gap=3,className='align-items-center justify-content-end'),
				dbc.Modal([
					dbc.Card([
						dbc.Stack([
							dbc.Button(children=[html.I(className='bi bi-plus-circle-fill'),' Delete Job'], id='tf-manage-schedule-delete-job', color='danger',style={'width':'150px'}),
							dcc.Input(placeholder='ex: redbeat:scheduled_tf:{APP}',id='tf-manage-schedule-delete-job-id',style={'width':'300px'}),
						],direction='horizontal',gap=3,className='schedule-stack'),
						dbc.Button(children=[html.I(className='bi bi-list-ul'),' List Jobs'], id='tf-manage-schedule-list-jobs', color='info',style={'width':'150px'}),
						dbc.Container([],id='tf-manage-schedule-output-container')
					]),
				],size='lg',is_open=False,id='manage-schedule-modal'),
			],gap=3,className='align-items-center justify-content-center'),
		}

	def render_schedule_editor(self,app_name: str, data: dict, tz='UTC') -> dbc.Container:
		def set_editor_styles(val: int) -> tuple:
			if val == None or val > 2:
				val = 0
			val_map = {
				0:('schedule-option-disabled',True,'schedule-option-disabled',True), #hide weekly/monthly options when set to daily frequency
				1:(None,None,'schedule-option-disabled',True),
				2:('schedule-option-disabled',True,None,None),
			}
			o = val_map.get(val)
			return o
		hours = ['{:02d}'.format(x) for x in range(24)]
		mins = ['{:02d}'.format(x) for x in range(60)]
		days_of_month = [x+1 for x in range(31)]
		st = set_editor_styles(data['frequency'] if data != None else 0)
		if data == None:
			magi = MagiScheduler()
			data = magi.get_default_schedule()
		editor = dbc.Stack([
			dbc.Card([
				dbc.CardHeader([
					dbc.Stack([
						html.Span(f'Edit Schedule | {app_name}',style={'font-weight':'bold'}),
						dbc.Stack([
							html.Span('Enabled: ',style={'font-weight':'bold'}),
							dbc.InputGroup([
								daq.BooleanSwitch(
									id={'type':'tf-client-schedule-enabled','index':f'{app_name}'},
									on=data['enabled'],
									color='#86D7DC',
								)
							]),
						],direction='horizontal',gap=3,className='align-items-center justify-content-between'),
					],direction='horizontal',gap=3,className='align-items-center justify-content-between'),
				]),
				dbc.CardBody([
					dbc.Stack([
						dbc.Card([
							dbc.CardHeader([
								html.Span('Frequency',style={'font-weight':'bold'}),
							]),
							dbc.CardBody([
								dbc.RadioItems(
									options=[
										{'label': 'Daily', 'value': 0},
										{'label': 'Weekly', 'value': 1},
										{'label': 'Monthly', 'value': 2},
									],
									value=data['frequency'],
									class_name='date-time-picker',
									id={'type':'tf-client-schedule-frequency','index':f'{app_name}'},
								),
							],className='dflex align-items-center justify-content-center'),
						],style={'flex':'1','align-self':'stretch'}),
						dbc.Card([
							dbc.CardHeader([
								html.Span('Schedule',style={'font-weight':'bold'}),
							]),
							dbc.CardBody([
								dbc.Stack([
									dcc.Dropdown(
										options=hours,
										placeholder='HH',
										clearable=False,
										id={'type':'tf-client-schedule-start-time-hours','index':f'{app_name}'},
										style={'width':'75px'},
										className='date-time-picker',
										value=f'{int(data['hour']):02d}',
									),
									html.Span(':',style={'color':'black'},),
									dcc.Dropdown(
										options=mins,
										placeholder='MM',
										clearable=False,
										id={'type':'tf-client-schedule-start-time-mins','index':f'{app_name}'},
										style={'width':'75px'},
										className='date-time-picker',
										value=f'{int(data['minute']):02d}',
									),
									html.Div([],id='tf-selected-tz-display'),
									dbc.Popover([
											'HH:MM, 24 hour format',
										],
										target='time-stack',
										body=True,
										trigger="hover",
									),
								],id='time-stack', direction='horizontal',gap=3,class_name='schedule-stack d-flex justify-content-center'),
								dbc.Stack([
									html.Span('every '),
									dcc.Dropdown(
										options=['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'],
										placeholder='Day of Week',
										value=data['day_of_week'],
										clearable=False,
										id={'type':'tf-client-schedule-day-of-week','index':f'{app_name}'},
										style={'width':'100px'},
										className=st[0],
										disabled=st[1],
									),
								],direction='horizontal',gap=3,class_name='schedule-stack d-flex justify-content-center'),
								dbc.Stack([
									html.Span('on day '),
									dcc.Dropdown(
										options=days_of_month,
										placeholder='Day of Month',
										value=data['day_of_month'],
										clearable=False,
										id={'type':'tf-client-schedule-day-of-month','index':f'{app_name}'},
										style={'width':'50px'},
										className=st[2],
										disabled=st[3],
									),
									html.Span(' of each month')
								],direction='horizontal',gap=3,class_name='schedule-stack d-flex justify-content-center'),
							]),
						],style={'flex':'2','align-self':'stretch'}),
					],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
				]),
				dbc.CardFooter([
					dbc.Button(children=['Save Schedule'], id={'type':'tf-client-save-schedule-button','index':f'{app_name}'}, color='primary'),
				],class_name='d-flex justify-content-end'),
			],style={'width':'100%'}),
		],gap=3,className='align-items-center justify-content-center')
		return editor

	def create_app_card(self,app_name,data, local_tz='UTC'):
		f_display = {
			0: 'daily',
			1: 'weekly',
			2: 'monthly',
		}
		last_run_val = None
		next_run_val = None
		if data == None:
			magi = MagiScheduler()
			data = magi.get_default_schedule()
		run_description = None
		try:
			last_run_val_utc = datetime.fromisoformat(str(data['last_run'])).strftime('%Y-%m-%d %H:%M') # this comes in as UTC
			last_run_val = conv_tz(last_run_val_utc,'UTC',local_tz).strftime('%Y-%m-%d %H:%M')
		except Exception as e:
			#print(f'no last run time: {e}')
			pass
		if data['enabled'] == True:
			now = datetime.now()
			now_floor = now.replace(hour=0, minute=0, second=0, microsecond=0)
			dt_to_cron = {
				0:1, #Mon
				1:2, #Tues
				2:3, #Weds
				3:4, #Thurs
				4:5, #Fri
				5:6, #Sat
				6:0, #Sun
			}
			dow_map = {
				'Sunday': 0,
				'Monday': 1,
				'Tuesday': 2,
				'Wednesday': 3,
				'Thursday': 4,
				'Friday': 5,
				'Saturday': 6,
			}

			dow = int(dow_map.get(data['day_of_week']))
			now_dow = int(dt_to_cron.get(now.weekday()))
			dom = int(data['day_of_month'])
			num_days_month = ( now_floor.replace(day=1) + pd.DateOffset(months=1) ) - now_floor.replace(day=1)
			hod = int(data['hour'])
			moh = int(data['minute'])
			next_run_day = 0 if now_floor + pd.DateOffset(hours=hod,minutes=moh) > now else 1
			next_run_dow = dow - now_dow if dow > now_dow else dow - now_dow + 7
			next_run_dom = dom - int(now.day) if dom > int(now.day) else dom - int(now.day) + num_days_month.days
			calc_map = {
				0: now_floor + pd.DateOffset(days=int(next_run_day),hours=hod,minutes=moh),
				1: now_floor + pd.DateOffset(days=int(next_run_dow),hours=hod,minutes=moh),
				2: now_floor + pd.DateOffset(days=int(next_run_dom),hours=hod,minutes=moh),
			}
			next_run_utc = calc_map.get(data['frequency'])
			#data['next_run'] = calc_map.get(data['frequency'])
			data['next_run'] = conv_tz(next_run_utc,'UTC',local_tz)
			#run_description = f'Runs {f_display.get(data['frequency'])} @ {data['hour']:02}:{data['minute']:02}'
			run_description = f'Runs {f_display.get(data['frequency'])} @ {data['next_run'].hour:02}:{data['next_run'].minute:02}'
			run_description_map = {
				0: '',
				1: f' on {data['day_of_week']}',
				2: f' on the {data['day_of_month']} of the month',
			}
			run_description = run_description + run_description_map.get(data['frequency'])
			try:
				next_run_val = datetime.fromisoformat(str(data['next_run'])).strftime('%Y-%m-%d %H:%M')
			except Exception as e:
				#print(f'no next run time: {e}')
				pass


		card = dbc.Card([
			dbc.CardBody([
				dbc.Stack([
					dbc.Stack([
						dbc.Stack([
							html.Span(f'{app_name}',className='px-2'),
						],direction='horizontal',gap=0,className='schedule-title bg-light align-items-center justify-content-start',style={'flex':'1'}),
						dbc.Stack([
							daq.Indicator(
								color='#86D7DC' if data['enabled'] else '#9B004E',
								value=data['enabled']
							),
							'Enabled' if data['enabled'] == True else 'Disabled',
						],direction='horizontal',gap=3,className='',style={'flex':'1'}),
					],direction='horizontal',gap=3,className='align-items-center justify-content-start',style={'flex':'1'}),
					dbc.Stack([
						html.Span([run_description]),
						html.Span([f'Last run: {last_run_val}']),
						html.Span([f'Next run: {next_run_val}']),
					],direction='horizontal',gap=3,className='align-items-center justify-content-center',style={'flex':'1'}),
					dbc.Stack([
						dbc.ButtonGroup([
							dbc.Button([
								dbc.Stack([html.I(className='bi bi-calendar3-week-fill'),'Edit Schedule'],direction='horizontal',gap=2),
							], id={'type':'tf-client-edit-schedule-button','index':f'{app_name}'}, color='info',className='p-1'),
							dbc.Button([
								dbc.Stack([html.I(className='bi bi-play-fill'),'Run Now'],direction='horizontal',gap=2),
							], id={'type':'tf-client-manual-run-button','index':f'{app_name}'}, color='success',className='p-1'),
						]),
					],direction='horizontal',gap=3,className='align-items-center justify-content-end',style={'flex':'1'}),
				],direction='horizontal',gap=3,className='align-items-center justify-content-between'),
			],className='p-0'),
		],className='p-0')
		return card

def create_app_layout(ui):

	layout = dbc.Stack([
			ui.layout['status_box'],
			ui.layout['app_card_container'],
			ui.layout['manage_schedule'],
			ui.layout['edit_schedule_modal'],
			dcc.Interval(id='tf-refresh-interval',interval=10*1000,n_intervals=0),
			dcc.Store(id='tf-transform-schedules'),
			dcc.Store(id='tf-transform-staging'),
			dcc.Location(id='tf-location-url',refresh=True),
	],gap=3,className='align-items-center justify-content-center'),

	return layout