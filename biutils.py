import os, json, random, uuid
from dotenv import load_dotenv
from datetime import date, datetime, timedelta
import dash_bootstrap_components as dbc
from dash import html, dcc
import plotly.io as pio
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import pandas as pd
import numpy as np
import pytz
import redis

try:
	REDISCLOUD_URL = os.environ['REDISCLOUD_URL']
except Exception as e:
	print(f'No redis url set: {e}')

class BiUtils:
	"""
	General utility functions for BI dashboards
	"""
	def __init__(self):
		self.init_time = datetime.now()
		self.calendar = AnchorCalendar()
		self.brands = {
			'acme_anvils': {
				'name': 'Acme Anvils',
				'display_name': 'Acme Anvils',
				'description': 'The best anvils in the business!'
			},
			'widget_wonders': {
				'name': 'Widget Wonders',
				'display_name': 'Widget Wonders',
				'description': 'Wondrous widgets for all your needs!'
			},
			'gizmo_galaxy': {
				'name': 'Gizmo Galaxy',
				'display_name': 'Gizmo Galaxy',
				'description': 'A galaxy of gizmos at your fingertips!'
			},
			'doohickey_depot': {
				'name': 'Doohickey Depot',
				'display_name': 'Doohickey Depot',
				'description': 'Depot of delightful doohickeys!'
			},
			'thingamajig_thrills': {
				'name': 'Thingamajig Thrills',
				'display_name': 'Thingamajig Thrills',
				'description': 'Thrilling thingamajigs for everyone!'
			},
			'whatchamacallit_world': {
				'name': 'Whatchamacallit World',
				'display_name': 'Whatchamacallit World',
				'description': 'World of wonderful whatchamacallits!'
			},
			'contraption_corner': {
				'name': 'Contraption Corner',
				'display_name': 'Contraption Corner',
				'description': 'Corner of creative contraptions!'
			},
			'gadget_gurus': {
				'name': 'Gadget Gurus',
				'display_name': 'Gadget Gurus',
				'description': 'Gurus of great gadgets!'
			},
		}
		self.styles = {
			'color_sequence': ['#FD486D', '#9F4A86', '#F5D107', '#86D7DC', '#333D79', '#E5732D', '#4CAF8E', '#722B5C', '#FFC10A', '#005580'],
			'color_sequence_text': ['white','white','black','black','white','white','black','white','black','white'],
			'portrait_colors': ['#86D7DC', '#9B004E','#FA005A','#FFC500','#520044'],
			'comp_colors':['#54898d','#9F4A86'],
			'icon_map' : {
				# 'retro_chart.png',
				'other': 'retro_sunset.png',
				'tv': 'retro_tv.png',
				'lifestyle': 'retro_lifestyle.png',
				'comics': 'retro_comics.png',
				'books': 'retro_books.png',
				'anime': 'retro_anime_2.png',
				# 'anime': 'retro_anime.png',
				# 'retro_spock_hand.png',
				'music': 'retro_music.png',
				'games': 'retro_games.png',
				# 'retro_taylor.png',
				'movies': 'retro_movies.png',
			},
			'icon_map_slack': {
				# 'retro_chart.png',
				'other': ':retro_sunset:',
				'tv': ':retro_tv:',
				'lifestyle': ':retro_lifestyle:',
				'comics': ':retro_comics:',
				'books': ':retro_books:',
				'anime': ':retro_anime:',
				# 'anime': 'retro_anime.png',
				# 'retro_spock_hand.png',
				'music': ':retro_music:',
				'games': ':retro_games:',
				# 'retro_taylor.png',
				'movies': ':retro_movies:',
			} 
		}
	
	def mark_timestamp(self, start_time:datetime=None):
		# Print a brief timestamp & optional elapsed time since start_time
		cur_t = datetime.now()
		o = 'Time: ' + cur_t.strftime('%H:%M:%S')
		if start_time is not None:
			e = cur_t - start_time
			e = int(e.total_seconds())
			ef = '{:02}:{:02}:{:02}'.format(e // 3600, e % 3600 // 60, e % 60)
			o = o + ' | Elapsed: ' + ef
		return o

	def gen_date_intervals(self, start_date, end_date, freq='1D', is_range=False):
		# Use pd.date_range to generate a list of dates or date ranges
		r = pd.date_range(start = start_date, end = end_date, freq = freq)
		r_start_list = []
		r_end_list = []
		for p in r:	
			if freq.lower()[-1:] == 'd':
				pstart = p
			elif freq.lower()[-1:] == 'm':
				pstart = p.replace(day=1)
			elif freq.lower()[-1:] == 'y':
				pstart = p.replace(day=1,month=1)
			elif freq.lower()[-1:] == 'w':
				pstart = p.replace(day=1,month=1)
			r_start_list.append(pstart.strftime('%Y-%m-%d'))
			r_end_list.append(p.strftime('%Y-%m-%d'))
		if is_range:
			rlist = []
			i = 0
			while i < len(r_start_list):
				rlist.append((r_start_list[i],r_end_list[i]))
				i = i+1
			return rlist
		else:
			return r_start_list

	def ag_grid_color_scale(self, df, n_bins=11, columns='all', theme='Teal', invert=False, mode='number'):
		#csamples = np.arange(0, 1, 1.0 / n_bins).tolist()
		csamples = np.linspace(0,1, n_bins).tolist()
		cscale = px.colors.sample_colorscale(theme,csamples, low=0.0, high=1.0, colortype='rgb')
		if invert:
			cscale = cscale[::-1]
		bounds = [i * (1.0 / n_bins) for i in range(n_bins + 1)]
		if columns == 'all':
			df_numeric_columns = df.select_dtypes('number')
		else:
			df_numeric_columns = df[columns]

		df_max = df_numeric_columns.max().max()
		df_min = df_numeric_columns.min().min()
		df_mean = np.nanmean(df_numeric_columns)
		df_stdev = np.nanstd(df_numeric_columns)

		if mode == 'number':
			ranges = [((df_max - df_min) * i) + df_min for i in bounds]
		if mode == 'signed':
			ranges = []
			lcnt = 1
			for i in bounds:
				if lcnt < np.ceil(round(n_bins/2)):
					ranges.append((0 - df_min * i) + df_min)
				else:
					ranges.append(((df_max - 0) * i) + 0)
				lcnt = lcnt + 1
		if mode == 'percent':
			ranges = []
			lcnt = 1
			for i in bounds:
				if lcnt < np.ceil(round(n_bins/2)):
					ranges.append(((0 - (-1) * i) + (-1)))
				else:
					ranges.append(((df_max - 0) * i) + 0)
				lcnt = lcnt + 1

		styleConditions = []
		legend = []
		for i in range(1, len(bounds)):
			min_bound = ranges[i - 1]
			max_bound = ranges[i]
			if i == len(bounds) - 1:
				max_bound += 1

			backgroundColor = cscale[i - 1]
			color = 'white' if i > len(bounds) / 2.0 else 'inherit'

			styleConditions.append(
				{
					"condition": f"params.value >= {min_bound} && params.value < {max_bound}",
					"style": {"backgroundColor": backgroundColor, "color": color},
				}
			)

			legend.append(
				html.Div(
					[
						html.Div(
							style={
								"backgroundColor": backgroundColor,
								"borderLeft": "1px rgb(50, 50, 50) solid",
								"height": "10px",
							}
						),
						html.Small(round(min_bound, 2), style={"paddingLeft": "2px"}),
					],
					style={"display": "inline-block", "width": "60px"},
				)
			)
		sc = {
			'style': styleConditions,
			'legend': html.Div(legend, style={"padding": "5px 0 5px 0"})
		}
		return sc
		#return styleConditions, html.Div(legend, style={"padding": "5px 0 5px 0"})

	def col_to_string(self,df,column,separator):
		print('Converting column [' + column + '] to [' + separator + ']-delimited string')
		c = []
		for row in df[column]:
			if(row != 'undefined' and str(row) != 'nan'):
				c.append('\'' + str(row) + '\'')
		s = separator.join(c)
		return s

	def auto_num_format(self,raw_number):
		num = float(f'{raw_number:.2g}')
		magnitude = 0
		while abs(num) >= 1000:
			magnitude += 1
			num /= 1000.0
		return '{} {}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), 
							 ['', 'K', 'M', 'B', 'T'][magnitude])

	def hex_to_rgb(self,hex_val):
		val = hex_val.replace('#','')
		return tuple(int(val[i:i+2],16) for i in (0, 2, 4))

	def conv_tz(self,ts, input_tz, output_tz):
		in_tz = pytz.timezone(input_tz)
		out_tz = pytz.timezone(output_tz)
		localized_timestamp = in_tz.localize(ts)
		d = localized_timestamp.astimezone(out_tz)
		return d

	def first_day_of_iso_week(year, week):
		jan4 = datetime(year, 1, 4)
		start_iso_week, start_iso_day = jan4.isocalendar()[1:3]
		weeks_diff = week - start_iso_week
		days_to_monday = timedelta(days=(1-start_iso_day))
		return jan4 + days_to_monday + timedelta(weeks=weeks_diff)

	def show_alert(self, text, color='warning'):
		icon = None
		dismissable = True
		if color == 'warning':
			icon = 'bi bi-exclamation-triangle-fill'
		if color == 'danger':
			icon = 'bi bi-exclamation-triangle-fill'
		elif color == 'info':
			icon = 'bi bi-info-circle-fill'
		elif color == 'success':
			icon = 'bi bi-check-circle-fill'
		alert = dbc.Alert([
			dbc.Stack([
				html.I(className=icon),
				html.Span(text),
			],direction='horizontal',gap=3,className='align-items-center justify-content-center'),
		],color=color,dismissable=dismissable,className='w-100')
		return alert

	def create_display_table(self, df, table_id=None, row_ids=None, href_vals=None, cell_bars=None, cell_highlights=None, col_formats=None, centered_cols=None, col_sizes=None, show_headers=True,header_styles=None,col_styles=None):
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
						val = self.auto_num_format(val)
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
		
	def get_quote(self):
		pathname = 'dash_app/assets/data/quotes.csv'
		df = pd.read_csv(pathname)
		df = df.sample().fillna(0)
		quotee = df['quotee'].iloc[0]
		source_media = df['source_media'].iloc[0]
		if source_media == 0:
			source_media = ''
		quote_text = df['quote_text'].iloc[0]
		image_path = 'assets/images/default.png'
		try:
			image_path = 'assets/images/' + df['image'].iloc[0]
		except Exception as e:
			print('no image available, using default')
			pass
		cid = random.randrange(0,len(self.styles['portrait_colors']))
		color_code = self.styles['portrait_colors'][cid]
		card = dbc.Container([
			dbc.Card([
				dbc.CardBody([
					dbc.Stack([
							dbc.Stack([
								html.Div([],className='quote-card-image',style={
									'background-image':f'url("{image_path}"),radial-gradient(circle at center, {color_code} 0, #3b434b 85%)',
									'background-size':'100px 100px',
									'width':'100px',
									'height':'100px',
								}),
								html.Span(f'"{quote_text}"',className='quote-card-text'),
							],direction='horizontal',gap=3),
							dbc.Stack([
								html.Span(f'- {quotee}',className='quote-card-quotee'),
								html.Span(f'{source_media}',className='quote-card-source-media'),
							],className='d-flex justify-content-end align-items-end'),
					],gap=3),
				]),
			],className='quote-card'),
		],fluid=True)

		return card
	
	def blend_colors(self, colors, weights):
		import matplotlib.colors as mcolors
		import numpy as np

		weights = np.array(weights) / np.sum(weights)
		rgba_colors = np.array([mcolors.hex2color(color) for color in colors])  # Convert HEX to RGB (0-1 range)
		blended_color = np.dot(weights, rgba_colors)
		return mcolors.to_hex(blended_color)

class AnchorCalendar:
	def __init__(self, anchor_date=datetime.now()):
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



