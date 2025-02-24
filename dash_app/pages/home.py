import os
from dotenv import load_dotenv
import dash_bootstrap_components as dbc
from dash import html, dcc
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import date, datetime
from conf import global_config

load_dotenv()
APP_NAME = os.environ['APP_NAME']
BASE_PATH = APP_NAME.replace(' ','-').lower()

class UInterface:
	def __init__(self):
		print('Initializing Home')
		self.init_time = datetime.now()
		self.layout = {
			'intro': dbc.Card([
				dbc.Stack([
					dcc.Markdown(global_config['intro_text'],className='intro-text px-2'),
					dbc.Stack([
						html.Img(src=global_config['pages']['home']['image'],className='intro-image',style={'max-height':'500px'}),
					], className='d-flex flex-column justify-content-start align-items-center'),
				],direction='horizontal', gap=3)
			],color='light',className='home-panel d-flex align-items-center justify-content-center'),
		}


		toc_links = []
		for k,v in global_config['pages'].items():
			if v['enabled'] and k != 'home':
				p = self.create_home_card(v)
				toc_links.append(p)
		self.layout['toc'] = dbc.Stack(toc_links,gap=5,className='align-items-center justify-content-center')

	def create_home_card(self,config):
	
		content = dbc.Stack([
			html.Span(config['summary_header'],style={'font-weight':'bold','font-size':'1.1rem'}),
			dbc.Stack([
				dcc.Markdown(config['summary_text'],style={'text-align':'left','font-size':'1rem'}),
			],direction='horizontal',gap=3),
		],gap=1, className='align-items-start justify-content-center p-2',style={'color':'black','min-width':'275px'})

		card = dbc.Stack([
				html.H3(config['display_name']),
				dbc.Button([
					dbc.Stack([
						dbc.Stack([
							html.Img(src=str(config['image']),className='directory-image'),
							dcc.Link(f'{config['display_name']}', href=config['full_path'],className='directory-link'),
						],gap=1, className='bg-dark d-flex align-items-center justify-content-center p-2',style={'flex':'1'}),
						dbc.Stack(content, style={'flex':'2'}),
					],direction='horizontal',gap=3,className='align-items-start justify-content-center'),
				],href=config['full_path'],className='bg-light directory-row d-flex align-items-center justify-content-start',style={'background':'none'})
			])
		return card
	
def create_app_layout(ui):

	layout = dbc.Container([
		dbc.Row([
			dbc.Col([
				ui.layout['intro'],
				ui.layout['toc']
			]),
		]),
	],fluid=True,className='d-flex flex-column align-items-center justify-content-center')

	return layout





