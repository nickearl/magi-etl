from datetime import date, datetime
import re

"""
Set global configurations
"""
global_config = {
	'app_name': 'Magi', # Keep it simple, this becomes the url base path
	'display_name': 'Magi', # optional, replace with a string value to use as a different display name ie 'Dash Template | Example App' 
	'app_prefix': 'magi',
	'logos': {
		'light': 'assets/images/spock_sunglasses.png',
		'dark': 'assets/images/spock_sunglasses.png',
	},
	'colors': {
		'bold': ["#FA005A", "#86D7DC", "#FFC500", "#520044", "#9B004E","#FA005A", "#86D7DC", "#FFC500", "#520044", "#9B004E"],
		'portrait_colors': ['#86D7DC', '#9B004E','#FA005A','#FFC500','#520044'],
	},
	'intro_text': """
	### **Magi**
	#### **Python ETL for LLM-supported BI dashboards**
	- Manage scheduled ETL jobs for BI dashboards
	- Utilizes Celery, Redis, Flask, and Dash to create a simple, scalable ETL management tool
	- Supports LLM RAG (ChatGPT) for AI-powered data processing
	- Easily deployable to Heroku
	- Secure, with Google OAuth and Slack integration
	- Customizable, with a simple configuration file
	- Easily extensible, with a simple plugin system
	- Built with love by Nick Earl

	""",
	'enable_google_auth': True,
	'enable_slack': False,
	'footer_text': f'Nick Earl © {datetime.now().year}',
}
global_config['pages'] = {
	'home': {
		'prefix':'home',
		'display_name': 'Home',
		'summary_header': 'Home Header',
		'summary_text': """
					- Explore the various tools and insights available in this app.

				""",
		'enabled': True,
	},
	'transformer': {
		'prefix':'tf',
		'display_name': 'Transformer',
		'summary_header': 'Manage scheduled ETL jobs',
		'summary_text': """
			- Manage scheduled ETL jobs for BI dashboards and agent knowledge bases
			- Schedules jobs via Celery with Redbeat
				""",
		'enabled': True,
	},
}


"""
Set defaults, etc
"""
global_config['display_name'] = global_config['display_name'] if global_config['display_name'] and not global_config['display_name'] == '' else global_config['app_name']
global_config['base_path'] = re.sub(r'[_\s]+', '-', global_config['app_name']).lower()
for page_name, page_config in global_config['pages'].items():
	page_key = re.sub(r'[_\s]+', '-', page_name).lower()
	page_config['path'] = f'/{page_key}' if page_name.lower() != 'home' else '/'
	page_config['full_path'] = f'/{global_config['base_path']}{page_config['path']}'
	page_config['image'] = page_config['image'] if 'image' in page_config.keys() and not page_config['image'] == '' else f'assets/images/{page_key}.webp'
	page_config['cache_path'] = f'{global_config['app_prefix']}:{page_config['prefix']}:cache'
	page_config['s3_path'] = page_config['s3_path'] if 's3_path' in page_config.keys() and not page_config['s3_path'] == '' else f'{global_config['base_path']}/{page_key}/'
	page_config['cache_ttl'] = page_config['cache_ttl'] if 'cache_ttl' in page_config.keys() else 60*60*24*30  # 30 days