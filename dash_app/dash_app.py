import os, json, re, uuid, socket, hashlib, ast, time, requests, pytz
from dotenv import load_dotenv
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import numpy as np
from datetime import date, datetime, timedelta
from flask.helpers import get_root_path
import flask
import redis
from connectors import QueryAthena, AmazonS3Connector
import biutils as bi
from conf import global_config

import redbeat
from redbeat import RedBeatSchedulerEntry as Entry
from redbeat.schedulers import RedBeatConfig
from celery.schedules import crontab

from sklearn.metrics.pairwise import cosine_similarity
from scipy.sparse import lil_matrix,csr_matrix, save_npz, load_npz, hstack
from scipy.sparse.linalg import norm as sparse_norm

load_dotenv()

APP_REFRESH_INTERVAL = os.environ['APP_REFRESH_INTERVAL']
REDISCLOUD_URL = os.environ['REDISCLOUD_URL']

def auto_num_format(raw_number):
	num = float(f'{raw_number:.2g}')
	magnitude = 0
	while abs(num) >= 1000:
		magnitude += 1
		num /= 1000.0
	return '{} {}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), 
		['', 'K', 'M', 'B', 'T'][magnitude])

def create_display_table(df, table_id=None, row_ids=None, href_vals=None, cell_bars=None, cell_highlights=None, col_formats=None, centered_cols=None, col_sizes=None, show_headers=True,header_styles=None,col_styles=None):
	print('bi: creating display table...', end="", flush=True)
	if table_id == None:
		table_id = uuid.uuid4().hex
	if row_ids is None:
		row_ids = [x for x in range(len(df))]
	try:
		row_ids = row_ids.to_list()
	except Exception as e:
		pass
	if href_vals is None:
		href_vals = [None for x in range(len(df))]
	try:
		href_vals = href_vals.to_list()
	except Exception as e:
		pass
	if cell_bars == None:
		cell_bars = [False for x in range(len(df.columns))]
	if cell_highlights == None:
		cell_highlights = [False for x in range(len(df.columns))]
	if col_formats == None:
		col_formats = ['auto' for x in range(len(df.columns))]
	if header_styles == None:
		header_styles = [{} for x in range(len(df.columns))]
	if col_styles == None:
		col_styles = [{} for x in range(len(df.columns))]
	header_cols = []
	header_col_aliases = [str(x).replace('_',' ').title() for x in df.columns]
	data_types = []
	max_lens = []
	align_classes = []
	flex_col_sizes = []
	for i in range(len(df.columns)):
		idx = str(df.columns[i]).lower().replace(' ','-')
		classes = 'gen-table-cell'

		# Determine each columns primary content & max length
		data_type = 'alpha'
		try:
			a_counts = df[df.columns[i]].apply(lambda x: sum(char.isalpha() for char in str(x).replace(" ", "")) )
			n_counts = df[df.columns[i]].apply(lambda x: sum(char.isdigit() for char in str(x).replace(" ", "")) )
			if n_counts.sum() > a_counts.sum():
				data_type = 'numeric'
			max_len= df[df.columns[i]].apply(lambda x: len(str(x))).max()
			if max_len < 5:
				data_type = 'short_alpha'
		except Exception as e:
			print(f'Error parsing field value: {e}')
			data_type = 'other'
			max_len = 0
		data_types.append(data_type)
		max_lens.append(max_len)

		# Set column size
		flex=1
		if col_sizes != None:
			flex = col_sizes[i]
		flex_col_sizes.append(flex)


		# Set alignment
		a_start = 'd-flex align-items-center justify-content-start'
		a_center = 'd-flex align-items-center justify-content-center'
		align_class = a_start
		if centered_cols == None:
			if data_types[i] in ['numeric', 'short_alpha']:
				align_class = a_center
		else:
			if centered_cols[i]:
				align_class = a_center
		align_classes.append(align_class)

		# Create column header
		header_style = header_styles[i]
		header_style['flex'] = flex
		c = dbc.Button(header_col_aliases[i],color='dark',className=f'{classes} {align_classes[i]} gen-table-row-header p-1 mx-2',id={'type':'sort_click','index':idx},style=header_style)
		header_cols.append(c)
	header_row = dbc.Card(
		dbc.Stack(header_cols,direction='horizontal',className='d-flex align-items-center justify-content-between'),
		color='dark',
		className='gen-table-row',
	)
	grid_rows = [header_row] if show_headers else []
	for ii in range(len(df)):
		body_cols = []
		for i in range(len(df.columns)):
			# Set column style
			col_style = col_styles[i]
			flex = flex_col_sizes[i]
			col_style['flex'] = flex
			val = df[df.columns[i]].iloc[ii]
			bar_class = ''
			if cell_bars[i]:
				try:
					max_val = df[df.columns[i]].max()
					percent_val = round( (val / max_val * 100) / 5) * 5
					bar_class = f'grid-cell-bar-chart-{percent_val}'
				except Exception as e:
					#print(f'Error parsing field value as numeric: {e}')
					pass
			if cell_highlights[i]:
				try:
					max_val = df[df.columns[i]].max()
					percent_val = round( (val / max_val * 100) / 5) * 5
					bar_class = f'grid-cell-radial-highlight-{percent_val}'
				except Exception as e:
					#print(f'Error parsing field value as numeric: {e}')
					pass
			if col_formats[i] == 'auto':
				try:
					val = auto_num_format(val)
				except Exception as e:
					pass
			elif col_formats[i] == 'numeric':
				val = f'{val:,.0f}'
			elif col_formats[i] == 'string':
				val = str(val)
			elif col_formats[i] == 'percent':
				val = f'{val:.0%}'
			elif col_formats[i] == 'percent+':
				val = f'{val:+.0%}'


			c = dbc.Stack(val,direction='horizontal',className=f'{classes} {align_classes[i]} {bar_class} gen-table-row p-1 mx-2',style=col_style)
			body_cols.append(c)

		row = dbc.Button(
			dbc.Stack(body_cols,direction='horizontal',className='d-flex align-items-center justify-content-between'),
			external_link=True if href_vals[ii] != None else False,
			href=href_vals[ii],
			target='_blank',
			color='light',
			className='gen-table-row-outer',
			id={'type':'row-click','table':table_id,'index':row_ids[ii]},
		)
		grid_rows.append(row)

	table = dbc.Stack(grid_rows,gap=0,className='gen-table d-flex justify-content-start align-items-start',style={'flex':'1'})
	print('done!')
	return table

def conv_tz(ts: datetime, input_tz: str, output_tz: str) -> datetime:
	if input_tz == output_tz:
		return ts
	in_tz = pytz.timezone(input_tz)
	out_tz = pytz.timezone(output_tz)

	if ts.tzinfo is None:
		localized_timestamp = in_tz.localize(ts)
	else:
		localized_timestamp = ts.astimezone(in_tz)
	return localized_timestamp.astimezone(out_tz)

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

class GlobalUInterface:
	def __init__(self):
		print(f'Initializing Global UI')
		self.init_time = datetime.now()
		self.logo_paths = global_config['logos']
		self.available_templates = ['ggplot2', 'seaborn', 'simple_white', 'plotly', 'plotly_white', 'plotly_dark', 'presentation', 'xgridoff', 'ygridoff', 'gridon', 'none']
		print('Creating nav links...')
		for k,v in global_config['pages'].items():
			print(v['display_name'])
		self.layout = {
		'sidebar' : dbc.Offcanvas(
			dbc.Container([
				dbc.Row([
					dbc.Col(html.Img(src=self.logo_paths['light'], height='30px')),
				], align='center'),
				dbc.Row([
					dbc.Col(dbc.NavbarBrand(global_config['display_name'], className='ms-2'))
				]),
				html.Hr(className='dash-bootstrap', style={'borderWidth': '1vh', 'width': '100%', 'backgroundColor': 'primary', 'opacity':'1'}),
				dbc.Nav([ dbc.NavLink(v['display_name'], href=v['full_path'], active='exact') for k,v in global_config['pages'].items() if v['enabled'] ],
				vertical=True,
				pills=True
				),				  
		], fluid=True,style={'background-color':'info'}),
			id='offcanvas-sidebar',
			is_open=False,
		),
		'navbar': dbc.Stack([
			dbc.Button([
				html.I(className="bi bi-list", style={'font-size': '2em', 'font-weight': 'bold'}),
			],id='nav-logo',color='primary d-flex align-items-center justify-content-center',style={'width':'10%','height':'5vh'}),
			dbc.NavbarBrand(global_config['display_name'],style={'color':'white','font-size': '1.5em', 'font-weight': 'bold'}),
			html.Img(src=self.logo_paths['light'],style={'height':'4vh'}),
		],direction='horizontal',gap=2,className='d-flex justify-content-between align-items-center bg-secondary',style={'height':'5vh','padding':'.5rem'}),
        'footer': dbc.Stack([
            html.Span(f'Nick Earl © {datetime.now().year}', className='footer-text'),
            html.A([html.I(className='bi bi-linkedin'),' linkedin.com/in/nickearl'],href='https://www.linkedin.com/in/nickearl/',target='_blank',className='footer-text'),
            html.A([html.I(className='bi bi-github'),' github.com/nickearl'],href='https://github.com/nickearl/',target='_blank',className='footer-text'),
            html.A([html.I(className='bi bi-at'),' nickearl.net'],href='https://www.nickearl.net',target='_blank',className='footer-text'),
        ],direction='horizontal',gap=3, className='footer d-flex justify-content-center align-items-center'),
		'loading_modal': dbc.Modal([
			dbc.Card([
				dbc.CardHeader([
					dbc.Progress(id='loading-modal-bar',value=0, striped=True, animated=True, color='#86D7DC',style={'background-color':'#3A434B'}),
					html.H3(['Loading...'],id='loading-modal-text',style={'color':'white'}),
					dbc.ListGroup([],id='loading-modal-list'),
				]),
				dbc.CardBody([
					html.Img(src='assets/images/loading_loop.gif',style={'width':'100%','height':'auto'}),
					dbc.ListGroup([],id='loading-modal-average-duration'),
				]),
			],className='loading-card'),
			dcc.Store(id='loading-modal-data'),
			dcc.Interval(id='loading-modal-refresh-interval', interval=1 * 1000, n_intervals=0)
		],is_open=False,backdrop='static',keyboard=False,className='loading-modal',id='loading-modal'),
		}

class HelixQueryBuilder:
	def __init__(self, year=None, month=None, confidence=0.0, dimensions=[], metrics=[],taxonomy_granularity={},filter_groups=[],filter_count=0,min_count=None,min_metric=None, refresh=False):
		b = bi.BiUtils()
		self.conf = {
			'cache_path': 'helix:cache:',
			'cache_ttl': 60*60*24*30,  # 30 days
		}
		self.attribute_info = {
			'vertical': {
				'unnest': False,
				'display_name': 'Vertical',
				'display_name_plural': 'Verticals',
				'icon': 'bi bi-pie-chart-fill',
				'description': 'The primary (MediaWiki) vertical of the content, such as Movies, TV, or Games.',
				'aliases': [],
			},
			'region': {
				'unnest': False,
				'display_name': 'Region',
				'display_name_plural': 'Regions',
				'icon': 'bi bi-map',
				'description': 'sales region such as NA, EMEA, or APAC.',
				'alises': ['sales region'],
			},
			'wiki': {
				'unnest': False,
				'display_name': 'Wiki',
				'display_name_plural': 'Wikis',
				'icon': 'bi bi-book',
				'description': 'Combines traffic from all wiki_ids that share the same subdomain (ie, all language versions of onepiece.acme.com).',
				'aliases': ['subdomain', 'community'],
			},
			'franchise': {
				'unnest': True,
				'display_name': 'Franchise',
				'display_name_plural': 'Franchises',
				'icon': 'bi bi-diagram-3',
				'description': 'The primary franchise of the content, such as Star Wars, Marvel, Harry Potter, etc',
				'aliases': ['ip', 'intellectual property', 'brand', 'series', 'universe'],
			},
			'genre': {
				'unnest': True,
				'display_name': 'Genre',
				'display_name_plural': 'Genres',
				'icon': 'bi bi-star-fill',
				'description': 'The primary genre of the content, such as Action, Comedy, RPG, etc.',
				'aliases': ['category'],
			},
			'subgenre': {
				'unnest': True,
				'display_name': 'Subgenre',
				'display_name_plural': 'Subgenres',
				'icon': 'bi bi-node-plus',
				'description': 'The primary subgenre of the content, such as Superhero-Action, Supernatural-Fantasy, or Space-Sci-Fi.',
				'alises': ['category','subcategory'],
			},
			'theme': {
				'unnest': True,
				'display_name': 'Theme',
				'display_name_plural': 'Themes',
				'icon': 'bi bi-palette',
				'description': 'The primary theme of the content, such as Quest, Friendship, Love Triangle, etc.',
				'aliases': ['topic','subject','subject matter','meaning','message','moral','lesson'],
			},
			'country': {
				'unnest': False,
				'display_name': 'Country',
				'display_name_plural': 'Countries',
				'icon': 'bi bi-globe2',
				'description': 'acme-defined country name, such as United States, Germany, United Kingdom, etc.',
				'aliases': ['nation'],
			},
			'subcontinent': {
				'unnest': False,
				'display_name': 'Subcontinent',
				'display_name_plural': 'Subcontinents',
				'icon': 'bi bi-globe-europe-africa',
				'description': 'acme sales subcontinent, such as Northern America, Western Europe, etc.',
				'aliases': ['continent','region'],
			},
			'wiki_id': {
				'unnest': False,
				'display_name': 'Wiki ID',
				'display_name_plural': 'Wiki IDs',
				'icon': 'bi bi-hash',
				'description': 'The unique MediaWiki ID for a wiki, representing a specific language version of the content such as 857 for starwars.acme.com (en).',
				'aliases': [],
			},
			'all_themes': {
				'unnest': False,
				'display_name': 'All Themes',
				'display_name_plural': 'All Themes',
				'icon': 'bi bi-palette',
				'description': 'All themes associated with the content.',
				'aliases': ['topics','subjects','subject matters','meanings','messages','morals','lessons'],
			},
			'all_genres': {
				'unnest': False,
				'display_name': 'All Genres',
				'display_name_plural': 'All Genres',
				'icon': 'bi bi-star-fill',
				'description': 'All genres associated with the content.',
				'aliases': ['categories'],
			},
			'all_subgenres': {
				'unnest': False,
				'display_name': 'All Subgenres',
				'display_name_plural': 'All Subgenres',
				'icon': 'bi bi-node-plus',
				'description': 'All subgenres associated with the content.',
				'aliases': ['categories','subcategories'],
			},
		}
		self.metric_info = {
			'users': {
				'display_name': 'Users',
				'display_name_plural': 'Users',
				'icon': 'bi bi-person',
				'calculation': 'count(distinct amplitude_id)',
				'description': 'The number of unique users who visited content matching the selection criteria during the specified time period.',
				'aliases': ['visitors','viewers','audience','traffic','people','readers','consumers','gamers','players','fans'],
			},
			'pageviews': {
				'display_name': 'Pageviews',
				'display_name_plural': 'Pageviews',
				'icon': 'bi bi-eye',
				'calculation': 'sum(pageviews)',
				'description': 'The total number of pageviews for content matching the selection criteria during the specified time period.',
				'aliases': ['views','hits','traffic'],
			},
			'page_count': {
				'display_name': 'Page Count',
				'display_name_plural': 'Page Counts',
				'icon': 'bi bi-file-earmark-text',
				'calculation': 'count(distinct wiki_article_id)',
				'description': 'The number of unique pages matching the selection criteria during the specified time period.',
				'aliases': ['articles','pages','urls','content']
			},
		}
		self.year=b.calendar.latest_complete_month_start.strftime('%Y') if year == None else year
		self.month=b.calendar.latest_complete_month_start.strftime('%m') if month == None else month
		self.confidence = confidence
		self.dimensions = dimensions
		self.metrics = metrics
		self.taxonomy_granularity = {
			'vertical': 'site',
			# 'franchise': 'site', # No page-level granularity for Franchise
			'genre': 'site',
			'subgenre': 'site',
			'theme': 'site',
		} if taxonomy_granularity == {} else taxonomy_granularity
		self.filter_groups = filter_groups
		self.filter_count = filter_count
		self.min_count = min_count if min_count else 1000
		self.min_metric = min_metric if min_metric else [x for x in self.metric_info.keys()][0]
		self.base_query = r"""with configs as (
			select
					'$$year$$' as year,
					'$$month$$' as month,
					$$minimum_confidence_threshold$$ as minimum_confidence_threshold
			),
			tax as (
				select
					article_id,
					wiki_id,
					url,
					transform(filtered_verticals, x -> lower(x[2]))  as all_verticals,
					transform(filtered_verticals, x -> x[1]) as all_verticals_con,
					transform(filtered_genres, x -> lower(x[2]))  as all_genres,
					transform(filtered_genres, x -> x[1])  as all_genres_con,
					transform(filtered_subgenres, x -> lower(x[2]))  as all_subgenres,
					transform(filtered_subgenres, x -> x[1])  as all_subgenres_con,
					transform(filtered_themes, x -> lower(x[2]))  as all_themes,
					transform(filtered_themes, x -> x[1])  as all_themes_con,
					transform(filtered_franchises, x -> lower(x[2]))  as all_franchises,
					transform(filtered_franchises, x -> x[1])  as all_franchises_con,
					case when cardinality(filtered_verticals) > 0 then transform(filtered_verticals, x -> lower(x[2]))[1] else null end as main_vertical,
					case when cardinality(filtered_genres) > 0 then transform(filtered_genres, x -> lower(x[2]))[1] else null end as main_genre,
					case when cardinality(filtered_subgenres) > 0 then transform(filtered_subgenres, x -> lower(x[2]))[1] else null end as main_subgenre,
					case when cardinality(filtered_themes) > 0 then transform(filtered_themes, x -> lower(x[2]))[1] else null end as main_theme,
					case when cardinality(filtered_franchises) > 0 then transform(filtered_franchises, x -> lower(x[2]))[1] else null end as main_franchise
					from (
						select
							content_ids['article_id'] as article_id,
							content_ids['wiki_id'] as wiki_id,
							url,
							filter(
								$$granularity_all_verticals$$,
								x ->x[1] >= threshold.minimum_confidence_threshold
							) as filtered_verticals,
							filter(
								$$granularity_all_genres$$,
								x ->x[1] >= threshold.minimum_confidence_threshold
							) as filtered_genres,
							filter(
								$$granularity_all_subgenres$$,
								x ->x[1] >= threshold.minimum_confidence_threshold
							) as filtered_subgenres,
							filter(
								$$granularity_all_themes$$,
								x ->x[1] >= threshold.minimum_confidence_threshold
							) as filtered_themes,
							filter(
								zip(array[1], array[site_all_franchises]),
								x ->x[1] >= threshold.minimum_confidence_threshold
							) as filtered_franchises
						from ccdr_proposed.taxonomy_dplat
						cross join (select minimum_confidence_threshold from configs) as threshold
					)
			),
			amp as (
				select
					*
				from AwsDataCatalog.amplitude."amplitude_core_monthly_rollup"
				where brand = 'acme'
				and platform = 'Web'
			),
			helix as (
				select * from (
					select
						t1.year as year,
						t1.month as month,
						all_verticals,
						all_verticals_con,
						all_genres,
						all_genres_con,
						all_subgenres,
						all_subgenres_con,
						all_themes,
						all_themes_con,
						all_franchises,
						all_franchises_con,
						main_vertical,
						main_genre,
						main_subgenre,
						main_theme,
						main_franchise,
						pageviews,
						amplitude_id,
						t1.content_id as article_id,
						cast(cast(floor(t1.wiki_id) as bigint) as varchar) AS wiki_id,
						t1.page_url as page_url,
						lower(split_part(split_part(t1.page_url,'://',2),'/',1)) as wiki,
						lower(t3.vertical_name) as vertical,
						concat(cast(t1.wiki_id as varchar),'-',t1.content_id) as wiki_article_id,
						lower(t4.acme_country) as country,
						lower(t4.acme_sales_region) as region,
						lower(t4.sales_insights_subcontinent) as subcontinent
					from (
					(select * from amp) as t1
					full outer join
					(select * from tax) as t2
					on cast(t1.wiki_id as varchar) = cast(t2.wiki_id as varchar)
					and cast(t1.content_id as varchar) = cast(t2.article_id as varchar)
					)
					left join (select wiki_id,vertical_name,domain from statsdb.dimension_wikis) as t3
					on cast(t1.wiki_id as varchar) = cast(t3.wiki_id as varchar)
					left join (select amplitude_country,acme_country,acme_sales_region,sales_insights_subcontinent from nearl.country_map) as t4
					on cast(t1.country as varchar) = cast(t4.amplitude_country as varchar)
					)
				where year = (select year from configs) and month = (select month from configs)
				$$helix_where_clause$$
			)
			"""
		self.data = {x: None for x in self.attribute_info.keys()}
		if refresh:
			self.refresh_data()
		self.config_ui = {
			'confidence_slider': html.Div([
				dbc.Label([
					dbc.Stack(['Confidence',html.I(className='bi bi-info-circle-fill')],direction='horizontal',gap=2,className='align-items-center justify-content-center'),
					],
					id='helix-config-confidence-label',
					style={'font-weight':'bold'}
				),
				# dbc.Popover([
				# 	dbc.PopoverHeader('Minimum Confidence Threshold'),
				# 	dbc.PopoverBody([
				# 		dbc.Stack([
				# 			html.Span('Sets the minimum confidence score required for pages containing the attribute to be considered in the plan.'),
				# 			html.Span([
				# 				html.Span("Relevance (Confidence):",style={'font-weight':'bold'}),
				# 				html.Span(" Helix's confidence in the relevance of this attribute to the content, calculated per page."),
				# 			]),
				# 			html.Span([
				# 				html.Span("Scale (Users):",style={'font-weight':'bold'}),
				# 				html.Span(" The number of unique users who have interacted with pages matching the selection criteria."),
				# 			]),
				# 		],gap=3,className='align-items-start justify-content-center'),
				# 	]),
				# ],target='helix-config-confidence-label',trigger='hover',placement='bottom-start',style={'width':'65vw'}),
				self.render_tooltip(
					title='Minimum Confidence Threshold',
					body_content=[
						html.Span('Sets the minimum confidence score required for pages containing the attribute to be considered in the plan.'),
						html.Span([
							html.Span("Relevance (Confidence):",style={'font-weight':'bold'}),
							html.Span(" Helix's confidence in the relevance of this attribute to the content, calculated per page."),
						]),
						html.Span([
							html.Span("Scale (Users):",style={'font-weight':'bold'}),
							html.Span(" The number of unique users who have interacted with pages matching the selection criteria."),
						]),
					],
					target='helix-config-confidence-label'
				),
				dcc.Slider(
					id={'type':'helix-config','cat':'confidence','index':'confidence'},
					min=0,
					max=1,
					step=.01,
					value=.6,
					marks={
					0: {'label': 'More Scale (Users)', 'style': {'font-weight': 'bold','left':'10%'}},  # Left side
					1: {'label':'More Relevance (Confidence)', 'style': {'font-weight': 'bold','left':'90%'}}  # Right side
					},
					tooltip={'placement':'bottom','always_visible':True}
				),
			],style={'width': '100%', 'margin': '0 auto', 'padding': '0 20px', 'box-sizing': 'border-box', 'overflow': 'visible' }),
			'taxonomy_granularity_selector': dbc.Stack([
				dbc.Label(['Taxonomy Granularity ',html.I(className='bi bi-info-circle-fill')],
					id='helix-config-taxonomy-granularity-label',
					style={'font-weight':'bold'}
				),
				self.render_tooltip(
					title='Taxonomy Granularity',
					body_content=[
						html.Span('Toggle between site-level and page-level taxonomy classifications.'),
						html.Span([
							html.Span('Site-Level:',style={'font-weight':'bold'}),
							html.Span(' Taxonomy values returned for each page reflect the entire content of the wiki, not the page itself directly.'),
							html.Span(' Example Page: simpsons.acme.com/wiki/Treehouse_of_Horror_II'),
							html.Span(' Example Top Themes: Animation, Comedy, Racing'),
							html.Span(' Values like Racing are unrelated to the page, but relevant to other pages in the wiki (such as Simpsons video games)'),
						]),
						html.Span([
							html.Span('Page-Level:',style={'font-weight':'bold'}),
							html.Span(' Taxonomy values returned for each page reflect the content of the page itself.'),
							html.Span(' Example Page: simpsons.acme.com/wiki/Treehouse_of_Horror_II'),
							html.Span(' Example Top Themes: Horror, Chaos, Murder'),
							html.Span(' Values directly reflect the content of the page itself, even if they are not necessarily representative of the wiki overall'),
						]),
					],
					target='helix-config-taxonomy-granularity-label'),
				dbc.Stack([
					dbc.Stack([
						dbc.Label(f'{attr_name.title()}',style={'font-weight':'bold'}),
						dbc.RadioItems(
							id={'type':'helix-config','cat':'taxonomy-granularity','index':attr_name},
							options=[
								{'label': 'Site', 'value': 'site'},
								{'label': 'Page', 'value': 'page'}
							],
							value='site',
						),
					],gap=1,className='align-items-center justify-content-center') for attr_name in ['vertical','genre','subgenre','theme']
				],direction='horizontal',gap=3,className='align-items-center justify-content-center',style={'border':'1px #d5d8dc solid', 'border-radius':'.5rem','padding':'.25rem'}),

			],gap=3,className='align-items-start justify-content-start'),
			'date_selector': dbc.Stack([
				dbc.Stack([
					dbc.Label('Year:',style={'font-weight':'bold'}),
					dcc.Dropdown(
						id={'type':'helix-config','cat':'year','index':'year'},
						options=[{'label': str(x), 'value': x} for x in range(2024, datetime.now().year + 1)],
						value=f'{b.calendar.latest_complete_month_start:%Y}',
						style={'width':'100%'}
					),
				],gap=0,className='align-items-start justify-content-start',style={'flex':'1'}),
				dbc.Stack([
					dbc.Label('Month:',style={'font-weight':'bold'}),
					dcc.Dropdown(
						id={'type':'helix-config','cat':'month','index':'month'},
						options=[{'label': date(1900, x, 1).strftime("%b"), 'value': x} for x in range(1, 13)],
						value=f'{b.calendar.latest_complete_month_start:%b}',
						style={'width':'100%'}
					),
				],gap=0,className='align-items-start justify-content-start',style={'flex':'1'}),
			],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
		}
		self.config_ui['config_card'] = dbc.Card([
			dbc.CardHeader([
				dbc.Stack([
					html.I(className='bi bi-gear-fill'),
					html.Span('Configure Helix',style={'font-weight':'bold'}),
				],direction='horizontal',gap=3,className='align-items-center justify-content-start'),
			]),
			dbc.CardBody([
				dbc.Stack([
					# Date Selector
					self.config_ui['date_selector'],
					# Confidence slider
					self.config_ui['confidence_slider'],
					# Granularity selector
					self.config_ui['taxonomy_granularity_selector'],
				],gap=3,className='align-items-center justify-content-center'),
			]),
		],className='shadow-lg rounded',style={'width':'40vw'})
		self.config_ui['filter_card'] = dbc.Card([
					dbc.CardHeader([
						dbc.Stack([
							html.I(className='bi bi-funnel-fill'),
							html.Span(['Add Filters ',html.I(className='bi bi-info-circle-fill',id='helix-config-filter-card-info')],style={'font-weight':'bold'}),
							self.render_tooltip(
								title='Filters',
								body_content=[
									html.Span('Select attributes to include or exclude from the results.'),
									html.Span('Filtering happens at page-level and is not case sensitive.'),
									html.Span('Examples:',style={'font-weight':'bold'}),
									html.Span('Sales Region must be either "NA" or "LATAM":'),
									html.Img(src='assets/images/tooltip_helix_filters_1.png',style={'max-width':'35vw','min-width':'20vw'}),
									html.Span('Both "country is United States" and "vertical is games" must be true to be included in the results'),
									html.Img(src='assets/images/tooltip_helix_filters_2.png',style={'max-width':'35vw','min-width':'20vw'}),
								],
								target='helix-config-filter-card-info'
							),
						],direction='horizontal',gap=3,className='align-items-center justify-content-start'),
					]),
					dbc.CardBody([
						dbc.Stack([
							dbc.Stack([],id={'type':'helix-config','cat':'filter-group','group_id':'prime','parent_group_id':'_prime','index':'nested-groups-container'},gap=3,className='align-items-center justify-content-center'), # filter boxes go here
							dbc.Button([html.I(className='bi bi-plus-circle-fill')],
								id={'type':'helix-config','cat':'filter-group','group_id':'prime','parent_group_id':'_prime','index':'add-filter-button'},
								color='primary',
								style={'flex':'1','align-self':'stretch'}
							),
						],gap=3,className='align-items-center justify-content-center'),
					]),
				],className='shadow-lg rounded',style={'width':'40vw', 'align-self':'stretch'})
		self.config_ui['dimension_card'] = dbc.Card([
				dbc.CardHeader([
					dbc.Stack([
						html.I(className='bi bi-columns-gap'),
						html.Span(['Select Dimensions ',html.I(className='bi bi-info-circle-fill',id='helix-config-dimension-card-info')],style={'font-weight':'bold'}),
						self.render_tooltip(
							title='Dimensions',
							body_content=[
								html.Span('Select the dimensions to include in the results.'),
								html.Span('Examples:',style={'font-weight':'bold'}),
								html.Span('Group results by theme, broken out by wiki:'),
								html.Img(src='assets/images/tooltip_helix_dimensions.png',style={'max-width':'35vw','min-width':'20vw'}),
							],
							target='helix-config-dimension-card-info'
						),
					],direction='horizontal',gap=3,className='align-items-center justify-content-start'),
				]),
				dbc.CardBody([
					dbc.Stack([
						self.render_dropdown_selector('dimension','prime','0'),
					],gap=3,
					className='align-items-center justify-content-center',
					id={'type':'helix-config','cat':'dropdown-container','group_id':'dimension','index':'dimension'},
					),
				]),
			],className='shadow-lg rounded',style={'width':'40vw', 'align-self':'stretch'})
		self.config_ui['metric_card'] = dbc.Card([
				dbc.CardHeader([
					dbc.Stack([
						html.I(className='bi bi-bar-chart-fill'),
						html.Span(['Select Metrics ',html.I(className='bi bi-info-circle-fill',id='helix-config-metric-card-info')],style={'font-weight':'bold'}),
						self.render_tooltip(
							title='Metrics',
							body_content=[
								html.Span('Select the metrics to include in the results.'),
								html.Span('Examples:',style={'font-weight':'bold'}),
								html.Img(src='assets/images/tooltip_helix_metrics.png',style={'max-width':'35vw','min-width':'20vw'}),
							],
							target='helix-config-metric-card-info'
						),
					],direction='horizontal',gap=3,className='align-items-center justify-content-start'),
				]),
				dbc.CardBody([
					dbc.Stack([
						self.render_dropdown_selector('metric','prime','0'),
					],gap=3,
					className='align-items-center justify-content-center',
					id={'type':'helix-config','cat':'dropdown-container','group_id':'metric','index':'metric'}
					),
				]),
			],className='shadow-lg rounded',style={'width':'40vw', 'align-self':'stretch'})

	def refresh_data(self, force_refresh=False, on_progress=None):
		print('HQB: Refreshing data')
		def stringify(value):
			if pd.isnull(value):
				return None
			if isinstance(value, (int, float)):
				return str(int(value)) if value == int(value) else str(value)

			if isinstance(value, bool):
				return str(value).lower()

			if isinstance(value, str):
				return value.strip() 
			return str(value)

		r = redis.from_url(REDISCLOUD_URL)
		self.dimensions = []
		queries = {}
		for k in self.attribute_info.keys():
			self.dimensions = [k]
			print(f'attribute: {k}')
			print(f'helix config: {self.to_dict()}')
			print(f'helix cache id: {self.generate_cache_key()}')
			queries[k] = {
				'query': self.build_query(),
				'cache_key': self.generate_cache_key()
			}
			print(f'final cache ID: {queries[k]['cache_key']}')
		missing_queries = {}
		cached_results = {}
		
		for name,query_configs in queries.items():			
			if not force_refresh:
				cache_key = query_configs['cache_key']

				try:
					print(f'PB: Checking cache for: {cache_key}')
					# Check if data is cached
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
					qa = QueryAthena(catalog="AwsDataCatalog", database="amplitude", query=query_configs['query'])
					resp_list = qa.run_query_async()
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
		qa = QueryAthena()
		results = qa.run_multiple_queries(missing_queries)
		
		# Cache results and release locks
		for name, result in results.items():
			if result['status'] == 'SUCCEEDED' and result['df'] is not None:
				cache_key = queries[name]['cache_key']
				try:
					print(f'attempting to cache result for {name}: {cache_key}, num rows: {result['df'].shape[0]}')
					r.set(cache_key, json.dumps(result['df'].to_dict(orient='records')))
					r.expire(cache_key, self.conf['cache_ttl'])
					print(f"Cached result for '{name}'")
				except Exception as e:
					print(f"Error caching result for '{name}': {e}")
			# Release lock after query is done
			r.delete(f'_{cache_key}')

		# Combine results into self.data
		final_results = {}
		for name in queries.keys():
			if name in results and results[name]['status'] == 'SUCCEEDED':
				final_results[name] = results[name]['df']
			else:
				final_results[name] = cached_results.get(name, None)
			# Ensure wiki_id (or other similar columns) are converted to strings
			if not final_results[name].empty and name in final_results[name].columns:
				final_results[name][name] = final_results[name][name].apply(lambda x: stringify(x))
			self.data[name] = final_results[name]

		return final_results

	def render_config_card(self):

		style = {'font-weight':'bold','font-size':'.9rem'}

		def construct_filter_card(filter_group):
			exclude = filter_group['exclude']
			attribute = filter_group['attribute']
			subgroups = filter_group['subgroups']
			values = filter_group['values']
			operator = filter_group['logical_operator']
			badges = []
			_badges = [dbc.Badge(x, color='primary', pill=True,className='ms-auto',style=style) for x in values]
			for badge in _badges:
				badges.append(badge)
				badges.append(f' {operator} ')
			badges = dbc.Stack([
				html.Span(f'{attribute.title()}:',style={'font-weight':'bold','font-size':'.9rem','flex':'1'}),
				dbc.Stack(badges,direction='horizontal',gap=1,className='align-items-center justify-content-between',style={'flex':'2'}),
			],direction='horizontal',gap=1,className='align-items-center justify-content-between')
			content = [construct_filter_card(x) for x in subgroups] if len(subgroups)>0 else badges
			border_style = '1px solid #86D7DC' if exclude else '1px solid darkgray'

			card = dbc.Card([
				dbc.CardHeader([
					html.Span(f'{'Exclude' if exclude else 'Include'} {'all of' if operator == 'and' else 'any of'}:',style=style),
				]),
				dbc.CardBody([
					dbc.Stack(content,direction='horizontal',gap=1,className='align-items-center justify-content-between'),
				]),],
			style={'flex':'1','padding':'.5rem','border':border_style,'align-self':'stretch','width':'100%'})
			return card
			
		helix_config_content = []
		confidence_content = dbc.Card([
			dbc.Stack([
				html.Span('Min. Confidence Threshold:',className='fs-6',style={'font-weight':'bold'}),
				dbc.Badge(self.confidence, color='primary',pill=True,className='ms-auto fs-6'),
			],direction='horizontal',gap=3,className='align-items-center justify-content-between'),
		],style={'flex':'1','padding':'.5rem','align-self':'stretch'})
		helix_config_content.append(confidence_content)

		granularity_content = []
		for attribute,granularity in self.taxonomy_granularity.items():
			granularity_content.append(
				dbc.Stack([
					html.Span(f'{attribute.title()}:',style={'font-weight':'bold','font-size':'.9rem'}),
					dbc.Badge(granularity, color='primary', pill=True,className='ms-auto',style=style),
				],direction='horizontal',gap=1,className='align-items-center justify-content-between')
			)
		granularity = dbc.Stack([
			dbc.Card([
				dbc.CardHeader([html.Span('Taxonomy Granularity',className='fs-6',style={'font-weight':'bold'}),]),
				dbc.CardBody([
					dbc.Stack(granularity_content,gap=1,className='align-items-start justify-content-center',style={'flex':'1'}),
				]),],
				style={'flex':'1','padding':'.5rem','align-self':'stretch','width':'100%'},
				color='secondary'
			)
		],gap=1,className='align-items-center justify-content-start')
		filters_content = []
		for filter_group in self.filter_groups:
			filters_content.append(construct_filter_card(filter_group))
		filters = dbc.Stack([
			dbc.Card([
				dbc.CardHeader([html.Span('Filters',className='fs-6',style={'font-weight':'bold'}),]),
				dbc.CardBody([
					dbc.Stack(filters_content) if len(filters_content) > 0 else html.Span('< No filters applied >'),
				]),],
				style={'flex':'2','padding':'.5rem','align-self':'stretch','width':'100%'},
			)
		],gap=1,className='align-items-center justify-content-start')
		helix_config_content.append(
			dbc.Stack([
				granularity,
				filters,
			],direction='horizontal',gap=1,className='align-items-center justify-content-between')
		)
		dimension_content = []
		for dimension in self.dimensions:
			dimension_content.append(
				dbc.Stack([
					html.Span(f'{self.attribute_info[dimension]["display_name"].title()}:',style={'font-weight':'bold','font-size':'.9rem'}),
					dbc.Badge(dimension, color='primary', pill=True,className='ms-auto',style=style),
				],direction='horizontal',gap=1,className='align-items-center justify-content-between')
			)
		dimensions = dbc.Stack([
			dbc.Card([
				dbc.CardHeader([html.Span('Dimensions',className='fs-6',style={'font-weight':'bold'}),]),
				dbc.CardBody([
					dbc.Stack(dimension_content) if len(dimension_content) > 0 else html.Span('< No dimensions selected >'),
				]),],
				style={'flex':'1','padding':'.5rem','align-self':'stretch','width':'100%'},
				color='info'
			)
		],gap=1,className='align-items-center justify-content-start')
		helix_config_content.append(dimensions)
		metric_content = []
		for metric in self.metrics:
			metric_content.append(
				dbc.Stack([
					html.Span(f'{self.metric_info[metric]["display_name"].title()}:',style={'font-weight':'bold','font-size':'.9rem'}),
					dbc.Badge(metric, color='primary', pill=True,className='ms-auto',style=style),
				],direction='horizontal',gap=1,className='align-items-center justify-content-between')
			)
		metrics = dbc.Stack([
			dbc.Card([
				dbc.CardHeader([html.Span('Metrics',className='fs-6',style={'font-weight':'bold'}),]),
				dbc.CardBody([
					dbc.Stack(metric_content) if len(metric_content) > 0 else html.Span('< No metrics selected >'),
				]),],
				style={'flex':'1','padding':'.5rem','align-self':'stretch','width':'100%'},
				color='success'
			)
		],gap=1,className='align-items-center justify-content-start')
		helix_config_content.append(metrics)
		stack = dbc.Stack(
			helix_config_content,
		gap=3,className='align-items-center justify-content-center')
		return stack
	
	def render_dropdown_selector(self, type, parent_id, idx):
		data = self.attribute_info if type == 'dimension' else self.metric_info
		options = [{'label': v['display_name'], 'value': k} for k,v in data.items()]
		dropdown = dcc.Dropdown(
			id={'type':'helix-config','cat':'dropdown-group','group_id':type,'parent_id':parent_id,'index':idx},
			options=options,
			style={'width':'100%'}
		)
		return dropdown
	
	def render_filter_group(self, group_id=None, parent_group_id='prime', nested_groups=[]):
		print('HQB: Creating filter group')
		group_id = uuid.uuid4().hex if group_id is None else str(group_id)
		filter = dbc.Card([
			dbc.CardHeader([
				dbc.Stack([
					dbc.Stack([
						dbc.Stack([ # Select Attribute
							dbc.Stack([
								# Include/Exclude toggle
								dbc.RadioItems(
									id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'include-exclude-toggle'},
									options=[
										{'label': 'Include', 'value': 'include'},
										{'label': 'Exclude', 'value': 'exclude'}
									],
									value='include',
									style={'flex':'1'},
									label_style={'font-weight':'bold'},
								),
								dcc.Dropdown(
									id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'attribute-type-dropdown'},
									options=[{'label': v['display_name_plural'], 'value': k} for k,v in self.attribute_info.items()],
									style={'flex':'2'},
								),
								html.Span('Matching ',style={'font-weight':'bold','flex':'1'}),
								dbc.RadioItems(
									id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'attribute-match-toggle'},
									options=[
										{'label': 'Any Of', 'value': 'or'},
										{'label': 'All Of', 'value': 'and'}
									],
									value='or',
									style={'flex':'1'},
									label_style={'font-weight':'bold'},
								),
							],direction='horizontal',gap=2,className='align-items-center justify-content-center'),
						],gap=3,className='align-items-center justify-content-center'),
						dbc.Button([html.I(className='bi bi-x-circle-fill')],
				 			id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'remove-filter-button'},
							color='danger',
							style={'flex':'1','max-width':'5vw','align-self':'stretch'}),
					],direction='horizontal',gap=2,className='align-items-center justify-content-between'),	
				],gap=3,className='align-items-center justify-content-center'),
			]),
			dbc.CardBody([
				dbc.Stack([
					dbc.Stack([
						dcc.Loading(
							dcc.Dropdown(
								id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'attribute-value-dropdown'},
								options=[],
								multi=True,
								style={'flex':'1','align-self':'stretch'},
								disabled=True,
							),
							parent_style={'flex':'1','align-self':'stretch'},
						),
						dbc.Button([html.I(className='bi bi-plus-circle-fill')],
							id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'add-filter-button'},
							color='primary',
							style={'flex':'1','max-width':'5vw','align-self':'stretch'}),
					],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
					dbc.Stack(
						nested_groups,
						gap=3,
						className='align-items-center justify-content-center',
						id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'nested-groups-container'}
					),
				],
				id={'type':'helix-config','cat':'filter-group','group_id':group_id,'parent_group_id':parent_group_id,'index':'nested-groups'},
				gap=3,className='align-items-center justify-content-center'
				), # nsted groups
			]),
		],style={'flex':'1','align-self':'stretch'})
		return filter

	def render_tooltip(self, title, body_content, target, icon=None, placement='auto'):
		icon = icon or 'bi bi-info-circle-fill'
		tooltip = dbc.Popover([
			dbc.PopoverHeader([
				dbc.Stack([
					html.I(className=icon),
					html.Span(title,style={'font-weight':'bold'}),
			],direction='horizontal', gap=2,className='align-items-center justify-content-start'),
			]),
			dbc.PopoverBody([
				dbc.Stack(body_content,gap=3,className='align-items-start justify-content-center'),
			]),
		],target=target,trigger='hover',placement=placement)
		return tooltip

	def add_filter_group(self, group_id, attribute, values, logical_operator='or', exclude=False, subgroups=None):
		# Add a filter group to the query config. Groups can optionally include nested subgroups.
		self.filter_groups.append({
			'group_id': group_id,
			'attribute': attribute,
			'values': values,
			'logical_operator': logical_operator,
			'exclude': exclude,
			'subgroups': subgroups or []  # Recursive definition for nested groups
		})

	def to_dict(self):
		return {
			'year': self.year,
			'month': self.month,
			'confidence': self.confidence,
			'dimensions': self.dimensions,
			'metrics': self.metrics,
			'taxonomy_granularity': self.taxonomy_granularity,
			'filter_groups': self.filter_groups,
			'filter_count': self.filter_count,
			'min_count': self.min_count,
			'min_metric': self.min_metric,
		}

	@classmethod
	def from_dict(cls, config):
		b = bi.BiUtils()
		year=b.calendar.latest_complete_month_start.strftime('%Y') if 'year' not in config.keys() else config['year']
		month=b.calendar.latest_complete_month_start.strftime('%m') if 'month' not in config.keys() else config['month']
		confidence = 0.0 if 'confidence' not in config.keys() else config['confidence']
		dimensions = [] if 'dimensions' not in config.keys() else config['dimensions']
		metrics = [] if 'metrics' not in config.keys() else config['metrics']
		taxonomy_granularity = {
			'vertical': 'site',
			'genre': 'site',
			'subgenre': 'site',
			'theme': 'site',
		} if 'taxonomy_granularity' not in config.keys() else config['taxonomy_granularity']
		filter_groups = [] if 'filter_groups' not in config.keys() else config['filter_groups']
		filter_count = 0 if 'filter_count' not in config.keys() else config['filter_count']
		min_count = None if 'min_count' not in config.keys() else config['min_count']
		min_metric = None if 'min_metric' not in config.keys() else config['min_metric']
		obj = cls(
			year=year,
			month=month,
			confidence=confidence,
			dimensions=dimensions,
			metrics=metrics,
			taxonomy_granularity=taxonomy_granularity,
			filter_groups=filter_groups,
			filter_count=filter_count,
			min_count=min_count,
			min_metric=min_metric

		)
		return obj

	def generate_cache_key(self):
		key_data = json.dumps(self.to_dict(), sort_keys=True)
		hash_val = hashlib.md5(key_data.encode('utf-8')).hexdigest()
		key = f'{self.conf['cache_path']}{hash_val}'
		return key
		
	def build_query(self):
		def build_group_sql(group):
			if group['attribute'] != None and self.attribute_info[group['attribute']]['unnest']:
				conditions = [
					f"cardinality(array_intersect(all_{group['attribute']}s, transform(array['{value}'], x -> lower(x)))) > 0" for value in group['values']
				]
			else:
				conditions = [
					f"lower({group['attribute']}) = lower('{value}')" for value in group['values']
				]

			group_sql = f" {' or '.join(conditions) } " if group['logical_operator'] == 'or' else f" {' and '.join(conditions) } "
			if group['exclude']:
				group_sql = f'NOT ({group_sql})'

			if 'subgroups' in group and group['subgroups']:
				subgroup_sqls = [build_group_sql(subgroup) for subgroup in group['subgroups']]
				subgroup_combination = f" {' or '.join(subgroup_sqls) } " if group['logical_operator'] == 'or' else f" {' and '.join(subgroup_sqls) } "
				group_sql = f"({group_sql} and ({subgroup_combination}))" if group['values'] else f'({subgroup_combination})'
			return f'({group_sql})'
		
		def sanitize_value(value, value_type):
			if value_type == 'string':
				if not isinstance(value, str):
					return ""
				# Allow only alphanumeric characters, spaces, underscores, hyphens, periods
				if not re.match(r'^[a-zA-Z0-9_\-\.\s]+$', value):
					raise ValueError(f"Invalid string value: {value}")
				return value

			elif value_type == 'float':
				if not isinstance(value, (float, int)):
					return 0.0
				return float(value)

			elif value_type == 'array':
				if not isinstance(value, list):
					return ""
				return f"{', '.join(f'\'{sanitize_value(v, "string")}\'' for v in value)}"

			else:
				raise ValueError(f"Unsupported value type: {value_type}")

		def generate_dynamic_attribute_cte(attributes, metrics, cte_name='dynamic_query', limit=None):

			# Collecting select columns
			select_parts = []
			from_parts = []
			group_by_parts = []
			cross_join_parts = []

			if not attributes:
				pass
			else:
				for idx, attribute in enumerate(attributes, start=1):
					if attribute not in self.attribute_info:
						raise ValueError(f'Unsupported attribute: {attribute}')
					unnest = self.attribute_info[attribute]['unnest']
					if unnest:
						# Handle unnesting
						select_parts.append(f'trim({attribute}_value) as {attribute}')
						from_parts.append(f'all_{attribute}s as {attribute}s')
						cross_join_parts.append(f'cross join unnest({attribute}s) as t{idx} ({attribute}_value)')
					else:
						# Handle standard attributes
						select_parts.append(f'{attribute}')
						from_parts.append(f'{attribute}')
					group_by_parts.append(str(idx))
			if not metrics:
				metrics = ['users','pageviews','page_count']
			for idx, metric in enumerate(metrics, start=len(attributes) + 1):
				if metric not in self.metric_info:
					raise ValueError(f'Unsupported metric: {metric}')
				calculation = self.metric_info[metric]['calculation']
				select_parts.append(f'{calculation} as {metric}')


			unnest_from_clause = ',\n'.join(from_parts)
			cross_joins = '\n'.join(cross_join_parts)
			from_clause = f'' if self.min_count== 0 else f'and {self.min_metric} >= {self.min_count}'
			if cross_joins:
				from_clause = f"(\nselect\n{unnest_from_clause},\namplitude_id,\nwiki_article_id,\npageviews\nfrom helix\nwhere cardinality({', '.join(f'all_{attr}s' for attr in attributes if self.attribute_info[attr]['unnest'])}) > 0\n    )\n    {cross_joins}"
			else:
				from_clause = "helix"

			select_clause = f'{',\n'.join(select_parts)}' if select_parts else ''
			group_by_clause = f'group by {', '.join(group_by_parts)}' if group_by_parts else ''
			order_by_clause = f'\norder by {str(len(attributes) + 1)} desc' if group_by_parts else ''
			limit_clause = f'\nlimit {limit}' if limit is not None else ''

			cte_template = f"""
				select
					{select_clause}
				from {from_clause}
				{group_by_clause}
				{order_by_clause}
				{limit_clause}
			"""
			if self.min_count and self.min_count>0:
				cte_template = f'select * from ({cte_template}) where {self.min_metric} >= {self.min_count}'
			cte_template = f"""
				{cte_name} as (
					{cte_template}
				)
			"""
			return cte_template.strip()

		filter_groups_sql = [build_group_sql(group) for group in self.filter_groups if not (group['values'] == [] and group['subgroups'] == [] )]
		where_clause = f'and ({" and ".join(filter_groups_sql) if filter_groups_sql else "1=1"})'

		query = self.base_query.replace('$$year$$', self.year)
		query = query.replace('$$month$$', self.month)
		query = query.replace('$$minimum_confidence_threshold$$', str(self.confidence))
		for attribute_name,granularity in self.taxonomy_granularity.items():
			query = query.replace(f'$$granularity_all_{attribute_name}s$$', f'{granularity}_all_{attribute_name}s')
		query = query.replace('$$helix_where_clause$$', where_clause)
		query = ',\n'.join([query,generate_dynamic_attribute_cte(attributes=self.dimensions, metrics=self.metrics, cte_name='dynamic_query')])
		query = '\n'.join([query,'select * from dynamic_query'])
		return query

class MagiScheduler:
	def __init__(self,server=None,tz='UTC',queue=None):
		self.init_time = datetime.now()
		self.calendar = AnchorCalendar()
		self.schedule_tz = pytz.timezone(tz)
		self.server = server
		self.queue = queue
		self.jobs = None
		if server != None:
			print(f'-- Server: {server}')
			self.jobs = self.list_jobs()

	def list_jobs(self):
		print('Listing jobs')
		config = RedBeatConfig(self.server)
		schedule_key = config.schedule_key
		print(f'schedule key: {schedule_key}')
		r = redbeat.schedulers.get_redis(self.server)
		rjobs = r.zrange(schedule_key, 0, -1, withscores=False)
		jobs = {x: Entry.from_key(key=x, app=self.server) for x in rjobs}
		print(f'jobs: {jobs}')
		return jobs

	def add_job(self, job_id, task, schedule, enabled, *args, **kwargs):
		print(f'adding job, args: {[*args]}')
		options = {} if self.queue is None else {'queue': self.queue}
		entry = Entry(job_id, task, schedule, args=list(args), kwargs=kwargs, app=self.server, options=options)
		entry.save()
		return job_id

	def delete_job(self,job_id):
		entry = Entry.from_key(job_id, app=self.server)
		entry.delete()

	def get_default_schedule(self):
		o = {
			'enabled': False,
			'frequency': 0,
			'hour': 0,
			'minute': 0,
			'day_of_week': 1,
			'day_of_month': 1,
			'last_run': None,
			'next_run': None,
		}
		return o
	
	# def convert_to_utc(self, dt: datetime) -> datetime:
	# 	local_dt = self.schedule_tz.localize(dt) if dt.tzinfo is None else dt.astimezone(self.schedule_tz)
	# 	return local_dt.astimezone(pytz.UTC)

	def make_cron_schedule(self, schedule, run_once=False):
		print('making cron schedule, got:')
		print(f'provided with schedule: {schedule}')
		if run_once:
			run_at = conv_tz(datetime.now(),self.schedule_tz.zone,'UTC') + pd.DateOffset(minutes=1)
			#run_at_utc = self.convert_to_utc(run_at)
			return crontab(hour=run_at.hour, minute=run_at.minute)

		if schedule is None:
			schedule = self.get_default_schedule()
		
		print(f'using schedule: {schedule}')

		# dow_m = {
		# 	'Sunday': 0, 'Monday': 1, 'Tuesday': 2, 'Wednesday': 3,
		# 	'Thursday': 4, 'Friday': 5, 'Saturday': 6,
		# }

		# Create a datetime object for the scheduled time in the specified timezone
		now_local = datetime.now(self.schedule_tz)
		print(f'now_local | {type(now_local)}: {now_local}')
		local_scheduled_time = self.schedule_tz.localize(datetime(
			now_local.year, now_local.month, now_local.day,
			int(schedule['hour']), int(schedule['minute'])
		))
		print(f'local_scheduled_time | {type(local_scheduled_time)}: {local_scheduled_time}')

		# Convert to UTC
		# utc_scheduled_time = local_scheduled_time.astimezone(pytz.UTC)
		utc_scheduled_time = conv_tz(local_scheduled_time,self.schedule_tz.zone,'UTC')
		print(f'utc_scheduled_time | {type(utc_scheduled_time)}: {utc_scheduled_time}')

		# Extract UTC values
		utc_hour = utc_scheduled_time.hour
		utc_minute = utc_scheduled_time.minute
		utc_day_of_week = utc_scheduled_time.weekday()  # Monday=0, Sunday=6

		cron = {
			0: crontab(hour=utc_hour, minute=utc_minute),
			1: crontab(day_of_week=utc_day_of_week, hour=utc_hour, minute=utc_minute),
			2: crontab(day_of_month=int(schedule['day_of_month']), hour=utc_hour, minute=utc_minute),
		}
		print(f'cron: {cron}')
		print(f'frequency: {schedule["frequency"]}')
		

		return cron.get(schedule['frequency'])

class TaxonomyRelationships:
	def __init__(self):
		r"""
		== Usage ==

		Inputs:
		raw_taxonony_df: "select * from ccdr_proposed.taxonomy_dplat"
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
		related = tax.get_similar_content(df=df, input_wiki_id='857', similarity_matrix=similarity_matrix, n=5, alpha=0.8)
		print(related)

		"""
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
			'raw_taxonomy': 'select * from ccdr_proposed.taxonomy_dplat',
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
						from ccdr_proposed.taxonomy_dplat
						group by 1
						order by 2 desc
					),
				traffic as (
					select
					* from (
						select
							cast(cast(floor(wiki_id) as bigint) as varchar) AS wiki_id,
							count(distinct amplitude_id) as users,
							sum(pageviews) as pageviews,
							count(distinct concat(cast(wiki_id as varchar),'-',content_id)) as page_count
						from AwsDataCatalog.amplitude."amplitude_core_monthly_rollup"
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
					left join statsdb.dimension_wikis t3
					on cast(t1.wiki_id as varchar) = cast(t3.wiki_id as varchar)
					order by users desc
				)
				select * from combined
			""".replace('$$year$$', f'{self.calendar.latest_complete_month_start:%Y}').replace('$$month$$', f'{self.calendar.latest_complete_month_start:%m}')
		}
		self.refresh_data()
		self.build_datasets()
	
	def refresh_data(self):
		print(f'TR: Fetching data for Helix Recommendations')

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
		raw_taxonony_df: "select * from ccdr_proposed.taxonomy_dplat"
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
			if not isinstance(x, list):
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
			return csr_matrix((len(df), 0))  
		all_categories = sorted(all_categories)  # ascending order
		cat_index = {c: i for i, c in enumerate(all_categories)}
		print(f"Total categories extracted: {len(all_categories)}")

		feature_matrix = lil_matrix((len(df), len(all_categories)), dtype=float)

		for i, row in df.iterrows():
			for col in df.columns:
				for cat_dict in row[col]:
					cat_name = f"{col}:{cat_dict['value']}"
					j = cat_index.get(cat_name, None)
					if j is not None:
						feature_matrix[i, j] = cat_dict['confidence']
		feature_matrix = feature_matrix.tocsr()
		row_norms = sparse_norm(feature_matrix, axis=1)
		row_norms[row_norms == 0] = 1 
		feature_matrix = feature_matrix.multiply(1 / row_norms.reshape(-1, 1)) * weight

		return feature_matrix

	def calculate_similarity(self,feature_matrices, save=False):

		# weighted_matrices = [
		# 	matrix * weights[key] for key, matrix in feature_matrices.items()
		# ]
		weighted_matrices = [matrix for key, matrix in feature_matrices.items()]
		combined_feature_matrix = hstack(weighted_matrices).tocsr()
		print(f'Combined feature matrix shape: {combined_feature_matrix.shape}')

		similarity_matrix = csr_matrix(cosine_similarity(combined_feature_matrix))
		print(f'Similarity matrix shape: {similarity_matrix.shape}')
		if save:
			save_npz(f'similarity_matrix', similarity_matrix)
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
	

def serve_protected_layout():
	def layout():
		if global_config['enable_google_auth']:
			if 'session_id' not in flask.session:
				return html.Div(["You are not authorized to view this page. Please ",html.A("log in.",href='/login')])
		print('Serving protected layout...')
		ui = GlobalUInterface()
		return dbc.Container([
			dbc.Row([
				dbc.Col([
					ui.layout['sidebar'],
					ui.layout['navbar'],
					html.Br(),
				]),
			]),
			dash.page_container,
			ui.layout['loading_modal'],
			html.Br(),
			dbc.Row([
				dbc.Col([
					ui.layout['footer'],
					html.Div([],id='dev-null'),
				], className='d-flex align-items-center justify-content-center'),
			]),
			dcc.Interval(id='full-refresh-interval', interval=int(APP_REFRESH_INTERVAL) * 60 * 1000, n_intervals=0),
			dcc.Interval(id='quote-refresh-interval', interval=8 * 1000, n_intervals=0),
			dcc.Store(id='session-id-store', data=flask.session.get('session_id')),
			dcc.Store(id='helix-config-store'),
			dcc.Store(id='download-results-store'),
			dcc.Download(id='download-results-downloader'),
		], fluid=True, style={'height': '100vh'})
	return layout

def create_dash_app(server, url_base_pathname, assets_folder, meta_tags, use_pages=False, pages_folder=''):

	print('root path: {}'.format(get_root_path('dash_app')))
	pd.options.mode.copy_on_write = True
	pd.options.display.float_format = '{:.2f}'.format
	pd.options.display.precision = 4
	pio.templates.default = "plotly_white"

	app = dash.Dash(
		server=server,
		assets_folder=assets_folder,
		meta_tags=meta_tags,
		routes_pathname_prefix=url_base_pathname,
		suppress_callback_exceptions=True,
		prevent_initial_callbacks='initial_duplicate',
		update_title=None,
		use_pages=use_pages,
		pages_folder=pages_folder,
		external_stylesheets=[dbc.themes.FLATLY, dbc.icons.BOOTSTRAP,dbc.icons.FONT_AWESOME])
	print('host ip: ' + str(socket.gethostbyname(socket.gethostname())))
	print('environment: ' + str(os.environ['DEPLOY_ENV']))
	app.layout = serve_protected_layout()
	return app
