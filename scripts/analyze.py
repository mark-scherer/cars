'''
Analyze
- creates plots and csvs based on current db data

Analysis Plan
	1. market overview
		- quick look to get ballpark for price, mileage, availability
	2. iterative filters
		- cycle thru customizable filter and sort params until shows top picks
	3. daily scraping
		- continuously see top options available now

NOT ACCOUNTED FOR IN FILTER, need to check manually
	- accident history
	- maintence history / condition
	- pictures exist
	- cosmetic defects
	- KBB value
'''

import sys, os
import json
import dateutil.parser, time, datetime
import matplotlib.dates as mdates
import itertools
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '../utils/'))

import py_utils
import py_postgres
import plotter

# control vars
CONFIG_PUBLIC_PATH = 'incl/config_public.json'
CONFIG_SECRET_PATH = 'incl/config_secret.json'
SELECTION_PATH = 'incl/selection_params.json'
OUTPUT_PATH = 'results/ranked_listings.csv'
ACTIVE_PERIOD = 60*60*24 # s
VERBOSE = True

# use local copies of data instead of connecting to DB
LOCAL_MODE = False
LOCAL_VEHCILES_PATH = '../../../Downloads/vehicles.csv'
LOCAL_LISTINGS_PATH = '../../../Downloads/vehicle_listings.csv'

DATA_QUERY = '''
	select 
		vehicles.make,
		vehicles.model,
		vehicles.version,
		vehicles.year, 
		vehicles.drivetrain,
		vehicles.color,
		vehicles.estimated_value,
		vehicles._owner as parsed_owner,
		vehicles.distance,
		vehicles.score_adjustments,
		vehicles.sold,
		listings.*
	from vehicles 
	join vehicle_listings listings
		on vehicles.vin = listings.vin
	order by vehicles.vin
'''
DATA_QUERY_HEADERS = [
	'make',
	'model',
	'version',
	'year',
	'drivetrain',
	'color',
	'estimated_value',
	'parsed_owner',
	'distance',
	'score_adjustments',
	'sold',
	'listing_id',
	'scrape_time',
	'vin',
	'source',
	'owner',
	'zip',
	'mileage',
	'price',
	'title',
	'remote'
]

ACTIVE_COLOR = 'b'
INACTIVE_COLOR = 'r'
CHOSEN_COLOR = 'g'
LOCAL_COLOR = 'b'
REMOTE_COLOR = 'orange'
DEFAULT_COLOR = 'm'

LISTING_FIELDS = [
	'source',
	'owner',
	'ownerName',
	'zip',
	'price',
	'mileage',
	'scrape_time',
	'remote'
]
def format_vehicle_link(vehicle, source):
	link = None
	if source == 'auto_trader':
		link = f'https://www.autotrader.com/cars-for-sale/vehicledetails.xhtml?vin={vehicle["vin"]}'
	elif source == 'autolist':
		if vehicle['model'] == 'grand_cherokee':
			link = f'https://www.autolist.com/jeep-grand+cherokee#vin={vehicle["vin"]}'
		elif vehicle['model'] == 'compass':
			link = f'https://www.autolist.com/jeep-compass#vin={vehicle["vin"]}'
		else:
			raise ValueError(f'format_vehicle_link: unsupported model for given source: {vehicle["model"]}, {source}')
	elif source == 'edmunds':
		if vehicle['model'] == 'grand_cherokee':
			link = f'https://www.edmunds.com/jeep/grand-cherokee/{vehicle["year"]}/vin/{vehicle["vin"]}/'
		elif vehicle['model'] == 'compass':
			link = f'https://www.edmunds.com/jeep/compass/{vehicle["year"]}/vin/{vehicle["vin"]}/'
		else:
			raise ValueError(f'format_vehicle_link: unsupported model for given source: {vehicle["model"]}, {source}')
	elif source == 'carvana':
		link = "carvana linking not currently supported"
	else:
		raise ValueError(f'format_vehicle_link: unsupported source: {source}')

	return link

def get_data(config):
	data = []
	if LOCAL_MODE:
		raw_vehicles = py_utils.read_csv(LOCAL_VEHCILES_PATH)
		raw_listings = py_utils.read_csv(LOCAL_LISTINGS_PATH)

		# brute force join but whatever
		joined_data = []
		for vehicle in raw_vehicles:
			for listing in raw_listings:
				if listing['vin'] is not None and listing['vin'] != "" and vehicle['vin'] == listing['vin']:
					joined_data.append({
						**py_utils.dict_pick(vehicle, ['make', 'model', 'version', 'year']),
						**py_utils.dict_omit(listing, ['created_on']),
						'scrape_time': dateutil.parser.parse(listing['created_on'])
					})
		data = sorted(joined_data, key=lambda j: j['vin'])
	else:
		py_postgres.connect(config['pg_config'])
		data = py_postgres.query(DATA_QUERY, headers=DATA_QUERY_HEADERS)
	print(f'found {len(data)} listings, LOCAL_MODE: {LOCAL_MODE}')
	return data

def prep_listings(raw_model_data, options={}):
	vehicles = []
	for row in raw_model_data:
		try:
			if row['zip'] is None or row['zip'] == '':
				row['zip'] = 0
			vehicles.append({
				**row,
				'year': int(row['year']),
				'zip': int(row['zip']),
				'price': int(row['price']),
				'mileage': int(row['mileage']),
			})
		except ValueError as error:
			if ('verbose' in options and options['verbose']):
				print(f'error parsing row, skipping: \n\trow: {row}\n\terror: {error}\n')
	return vehicles

def get_unique_vehicles(listings):
	vin_func = lambda listing: listing['vin']
	grouped_listings = itertools.groupby(sorted(listings, key=vin_func), vin_func)
	
	unique_vehicles = {}
	for vin, listings in grouped_listings:
		listings_list = sorted(list(listings), key=lambda l: l['scrape_time'].timestamp())
		unique_vehicles[vin] = {
			**py_utils.dict_omit(listings_list[0], LISTING_FIELDS),
			'listings': [py_utils.dict_pick(l, LISTING_FIELDS) for l in listings_list]
		}
	return unique_vehicles

def augment_vehicles(unique_vehicles):
	all_listings = [v['listings'] for v in unique_vehicles.values()]
	flat_listings = [l for vehicle_listings in all_listings for l in vehicle_listings]
	most_recent_scrape = max([l['scrape_time'].timestamp() for l in flat_listings])

	augmented_vehicles = {}
	for vin, vehicle in unique_vehicles.items():

		# ensure latest_listing is cheapest active listing
		raw_latest_listing = vehicle['listings'][len(vehicle['listings']) - 1]
		latest_listing = raw_latest_listing
		for listing in vehicle['listings']:
			if listing['scrape_time'].timestamp() > raw_latest_listing['scrape_time'].timestamp() - ACTIVE_PERIOD \
				and listing['source'] !=  raw_latest_listing['source'] \
				and listing['price'] < latest_listing['price']:
				latest_listing = listing

		earliest_listing = vehicle['listings'][0]
		days_detected = (latest_listing['scrape_time'].timestamp() - earliest_listing['scrape_time'].timestamp())/60/60/24
		active = latest_listing['scrape_time'].timestamp() > datetime.datetime.now().timestamp() - ACTIVE_PERIOD

		active_sources = []
		all_sources = []
		active_links = {}
		for listing in vehicle['listings']:
			if listing['source'] not in all_sources:
				all_sources.append(listing['source'])
			if listing['scrape_time'].timestamp() > datetime.datetime.now().timestamp() - ACTIVE_PERIOD \
				and listing['source'] not in active_sources:
				active_sources.append(listing['source'])
				active_links[listing['source']] = format_vehicle_link(vehicle, listing['source'])

		net_price_change = latest_listing['price'] - earliest_listing['price']
		price_increases = 0
		price_decreases = 0
		prev_price = earliest_listing['price']
		for listing in vehicle['listings']:
			if listing['price'] > prev_price:
				price_increases += 1
			if listing['price'] < prev_price:
				price_decreases += 1
			prev_price = listing['price']

		augmented_vehicles[vin] = {
			**vehicle,
			'earliest_listing': earliest_listing,
			'latest_listing': latest_listing,
			'days_detected': days_detected,
			'active': active,
			'all_sources': sorted(all_sources),
			'active_sources': sorted(active_sources),
			'active_links': active_links,
			'net_price_change': net_price_change,
			'price_increases': price_increases,
			'price_decreases': price_decreases
		}

	return augmented_vehicles

def filter_vehicles(all_vehicles, _filter):
	passes = []
	for vehicle in all_vehicles:
		_pass = True
		
		if 'model' in _filter:
			if vehicle['model'] not in _filter['models']:
				_pass = False
		if 'color_not' in _filter:
			if vehicle['color'] in _filter['color_not']:
				_pass = False
		if 'active' in _filter:
			if vehicle['active'] != True:
				_pass = False
		if 'not_sold' in _filter:
			if vehicle['sold'] != False:
				_pass = False
		
		if 'max_miles' in _filter:
			if vehicle['latest_listing']['mileage'] > _filter['max_miles']:
				_pass = False
		if 'min_miles' in _filter:
			if vehicle['latest_listing']['mileage'] < _filter['min_miles']:
				_pass = False
		if 'max_price' in _filter:
			if vehicle['latest_listing']['price'] > _filter['max_price']:
				_pass = False
		if 'min_price' in _filter:
			if vehicle['latest_listing']['price'] < _filter['min_price']:
				_pass = False

		if "min_year_by_model" in _filter:
			if vehicle['model'] in _filter["min_year_by_model"]:
				if vehicle['year'] < _filter["min_year_by_model"][vehicle['model']]:
					_pass = False

		if _pass:
			passes.append(vehicle)
	print(f'filter_vehicles: filtered {len(all_vehicles)} vehicles down to {len(passes)}')
	return passes



def plot_vehicles(plot_data, config):
	def thousands_format_func(number): return format(int(number), ',')

	def year_data_func(v): return v['year']
	year_axis_format = {'label': 'year', 'tick_interval': 1}

	def mileage_data_func(v): return v['latest_listing']['mileage']
	mileage_axis_format = {'label': 'mileage', 'tick_format_func': lambda x, pos: thousands_format_func(x)}

	def listings_time_func(v): return [l['scrape_time'] for l in v['listings']]
	def listings_plot_order_func(v): return -1*v['latest_listing']['scrape_time'].timestamp()
	# time_format = {'label': 'scrape time', 'tick_locator': mdates.DayLocator(bymonthday=1, interval=7), 'tick_format_func': lambda x, pos: mdates.num2date(x).strftime('%m-%d-%y')}
	time_format = {'label': 'scrape time', 'tick_locator': mdates.WeekdayLocator(byweekday=1), 'tick_format_func': lambda x, pos: mdates.num2date(x).strftime('%m-%d-%y')}

	def price_data_func(v): return v['latest_listing']['price']
	def listings_price_func(v): return [l['price'] for l in v['listings']]
	price_axis_format = {'label': 'price', 'tick_format_func': lambda x, pos: f'${thousands_format_func(x/1000)}k'}

	def days_detected_data_func(v): return v['days_detected']
	days_detected_format = {'label': 'days_detected', 'min_tick_interval': 1, 'tick_format_func': lambda x, pos: round(x)}

	def net_price_change_data_func(v): return v['net_price_change']
	net_price_change_format = {'label': 'net_price_change', 'tick_format_func': lambda x, pos: f'${x}'}

	def score_func(v): return v['score']
	score_axis_format = {'label': 'score'}

	def active_inactive_color_func(v): 
		if v['vin'] == config['chosen_vin']:
			return CHOSEN_COLOR
		elif v['active'] == True:
			return ACTIVE_COLOR 
		else:
			return INACTIVE_COLOR
	active_inactive_color_legend = [
		{'label': 'active', 'color': ACTIVE_COLOR},
		{'label': 'inactive', 'color': INACTIVE_COLOR},
		{'label': 'chosen', 'color': CHOSEN_COLOR},
	]

	def remote_color_func(v): 
		if v['vin'] == config['chosen_vin']:
			return CHOSEN_COLOR
		elif v['latest_listing']['remote'] == True or v['latest_listing']['remote'] == 't':
			return REMOTE_COLOR 
		else:
			return LOCAL_COLOR
	remote_color_legend = [
		{'label': 'local', 'color': LOCAL_COLOR},
		{'label': 'remote', 'color': REMOTE_COLOR},
		{'label': 'chosen', 'color': CHOSEN_COLOR},
	]

	def constant_color_func(v): return DEFAULT_COLOR

	if len(list(plot_data['models'].keys())) != 2:
		raise ValueError('analyze.plot_vehicles currently configured for exactly 2 models')

	# TURNED OFF: wasn't looking at & slowed down script
	# plotter.double_scatter(
	# 	list(plot_data['models'].values())[0]['all_vehicles'], list(plot_data['models'].values())[1]['all_vehicles'],
	# 	mileage_data_func, price_data_func, remote_color_func,
	# 	list(plot_data['models'].keys())[0], list(plot_data['models'].keys())[1],
	# 	'price evaluation', legend_entries=remote_color_legend,
	# 	x_axis_format=mileage_axis_format, y_axis_format=price_axis_format
	# )

	plotter.double_scatter(
		plot_data['all']['all_vehicles'], plot_data['filtered_for_csv'],
		mileage_data_func, price_data_func, remote_color_func,
		'all vehicles', 'filtered vehciles',
		'filtered listings', legend_entries=remote_color_legend,
		x_axis_format=mileage_axis_format, y_axis_format=price_axis_format
	)

	# plotter.single_histogram(
	# 	plot_data['filtered_for_plot'], score_func,
	# 	'historic scores',
	# 	score_format
	# )
	plotter.single_scatter(
		plot_data['filtered_for_plot'],
		price_data_func, score_func, active_inactive_color_func, 
		'historic scores', legend_entries=active_inactive_color_legend,
		x_axis_format=price_axis_format, y_axis_format=score_axis_format
	)

def sort_listings(filtered_listings, sort_config):
	scalar_fields = ['price', 'mileage']
	dict_fields = ['drivetrain', 'color', 'model', 'version']
	bool_fields = ['remote']
	
	def score_vehicle(vehicle):
		score = 0
		for param, value in sort_config.items():
			if param in scalar_fields:
				if param in LISTING_FIELDS and param in vehicle['latest_listing']:
					score = score + value*vehicle['latest_listing'][param]
				elif param in vehicle:
					score = score + value*vehicle[param]
			elif param in dict_fields:
				if param in LISTING_FIELDS and param in vehicle['latest_listing'] and vehicle['latest_listing'][param] in value:
					score = score + value[vehicle['latest_listing'][param]]
				elif param in vehicle and vehicle[param] in value:
					score = score + value[vehicle[param]]
			elif param in bool_fields:
				if param in LISTING_FIELDS and param in vehicle['latest_listing'] and vehicle['latest_listing'][param]:
					score = score + value
				elif param in vehicle and vehicle[param]:
					score = score + value
			elif param == 'distance_by_dealer':
				distance_by_dealer_scored = False
				distance_by_dealer_adj = None
				for dealer, scoring_info in py_utils.dict_omit(value, ['default']).items():
					if not distance_by_dealer_scored and 'parsed_owner' in vehicle and vehicle['parsed_owner'] is not None and dealer.lower() in vehicle['parsed_owner'].lower():
						score = score + vehicle['distance']*scoring_info['per_mile'] + scoring_info['flat_fee']
						distance_by_dealer_scored = True
				if not distance_by_dealer_scored and 'distance' in vehicle and vehicle['distance'] is not None:
					score = score + vehicle['distance']*value['default']['per_mile'] + value['default']['flat_fee']
			else:
				raise ValueError(f'scoring not implemented for field: {param}')
		for reason, adjustment in vehicle['score_adjustments'].items():
			score += adjustment
		return score
	
	for vehicle in filtered_listings:
		vehicle['score'] = score_vehicle(vehicle)
	return sorted(filtered_listings, key=lambda v: v['score'])

def dump_ranked_listings(all_scored_vehicles, ranked_choices, sort_config):
	# vehicle fields to dump first
	DUMP_FIELDS_LISTING = [
		'price_f',
		'mileage_f',
		'source'
	]
	DUMP_FIELDS_VEHICLE = [
		'year',
		'model',
		'version',
		'drivetrain',
		'color',
		'distance_f',
		'score_adjustments',
		'estimated_value',
		'listing_discount',
		'days_detected',
		'active_sources',
		'vin',
		'link_0',
		'link_1',
		'link_2',
		'link_3'
	]

	sorted_scores = sorted([v['score'] for v in all_scored_vehicles if 'score' in v])
	dumpable_listings = []
	rank = 1
	for vehicle in ranked_choices:
		# format for dump
		vehicle['days_detected'] = round(vehicle['days_detected'])
		vehicle['score_f'] = f'{round(vehicle["score"] / 1000, 1)}k'
		# vehicle['latest_listing']['price_f'] = f"{vehicle['latest_listing']['price']:,}"
		vehicle['latest_listing']['price_f'] = f'${round(vehicle["latest_listing"]["price"] / 1000, 1)}k'
		vehicle['latest_listing']['mileage_f'] = f"{vehicle['latest_listing']['mileage']:,}"
		vehicle['distance_f'] = round(vehicle['distance']) if 'distance' in vehicle and vehicle['distance'] is not None else None
		if vehicle['estimated_value']:
			vehicle['listing_discount'] = f"{round(((vehicle['estimated_value'] - vehicle['latest_listing']['price']) / vehicle['estimated_value'])*100)}%"

		written_links = 0
		max_links = 4
		for link in list(vehicle['active_links'].values()):
			if written_links >= max_links:
				raise ValueError(f'dump_ranked_listings: unsupported number of links: {len(vehicle["active_links"])}')
			vehicle[f'link_{written_links}'] = link
			written_links += 1

		dumpable_listings.append({
			'rank': rank,
			'score_f': vehicle['score_f'],
			'alltime_score_%': round(np.searchsorted(sorted_scores, vehicle['score'])/len(sorted_scores)*100,1),
			**py_utils.dict_pick(vehicle['latest_listing'], DUMP_FIELDS_LISTING),
			**py_utils.dict_pick(vehicle, DUMP_FIELDS_VEHICLE)
		})
		rank += 1
	py_utils.write_csv(OUTPUT_PATH, dumpable_listings)

def main():
	config_public = json.load(open(CONFIG_PUBLIC_PATH))
	config_secret = json.load(open(CONFIG_SECRET_PATH))
	config = {**config_public, **config_secret}
	selections_params = json.load(open(SELECTION_PATH)) 

	src_data = get_data(config)
	plot_data = {
		'all': {
			'all_vehicles': [],
			'active_vehicles': [],
			'inactive_vehicles': []
		},
		'models': {}
	}
	for model, model_config in config['scrape_configs'].items():
		raw_model_data = [row for row in src_data if row['model'] == model]

		model_listings = prep_listings(raw_model_data, {'verbose': VERBOSE})
		raw_model_vehicles = get_unique_vehicles(model_listings)
		model_vehicles = augment_vehicles(raw_model_vehicles)

		all_vehicles = [vehicle for vehicle in model_vehicles.values()]
		active_vehicles = [vehicle for vehicle in model_vehicles.values() if vehicle['active'] == True]
		inactive_vehicles = [vehicle for vehicle in model_vehicles.values() if vehicle['active'] == False]
		print(f'{model}: found {len(active_vehicles)} active and {len(inactive_vehicles)} inactive vehicles')

		plot_data['models'][model] = {
			'all_vehicles': all_vehicles,
			'active_vehicles': active_vehicles,
			'inactive_vehicles': inactive_vehicles
		}
		plot_data['all']['all_vehicles'] = plot_data['all']['all_vehicles'] + all_vehicles
		plot_data['all']['active_vehicles'] = plot_data['all']['active_vehicles'] + active_vehicles
		plot_data['all']['inactive_vehicles'] = plot_data['all']['inactive_vehicles'] + active_vehicles
	plot_data['filtered_for_csv'] = filter_vehicles(plot_data['all']['all_vehicles'], selections_params['filters_for_csv'])
	plot_data['filtered_for_plot'] = sort_listings(filter_vehicles(plot_data['all']['all_vehicles'], selections_params['filters_for_plot']), selections_params['sort'])

	plot_vehicles(plot_data, config)

	ranked_choices = sort_listings(plot_data['filtered_for_csv'], selections_params['sort'])
	dump_ranked_listings(plot_data['filtered_for_plot'], ranked_choices, selections_params)
	

main()