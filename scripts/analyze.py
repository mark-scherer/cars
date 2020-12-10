'''
Analyze to do:
	1. plot models side-by-side <- handle case of n models
	2. make plots prettier
	3. analyze plans and make buying strategy
'''

import sys, os
import json
import dateutil.parser,time
import matplotlib.dates as mdates
import itertools

sys.path.append(os.path.join(os.path.dirname(__file__), '../utils/'))

import py_utils
import py_postgres
import plotter

CONFIG_PUBLIC_PATH = 'incl/config_public.json'
CONFIG_SECRET_PATH = 'incl/config_secret.json'
ACTIVE_PERIOD = 60 # s
VERBOSE = False

DATA_QUERY = '''
	select 
		vehicles.make,
		vehicles.model,
		vehicles.version,
		vehicles.year, 
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
	'listing_id',
	'scrape_time',
	'vin',
	'source',
	'owner',
	'zip',
	'mileage',
	'price',
	'title'
]

ACTIVE_COLOR = 'g'
INACTIVE_COLOR = 'r'
DEFAULT_COLOR = 'm'

LISTING_FIELDS = [
	'owner',
	'ownerName',
	'zip',
	'price',
	'mileage',
	'scrape_time'
]

def prep_listings(raw_model_data, options={}):
	vehicles = []
	for row in raw_model_data:
		try:
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
		earliest_listing = vehicle['listings'][0]
		latest_listing = vehicle['listings'][len(vehicle['listings']) - 1]
		days_detected = (latest_listing['scrape_time'].timestamp() - earliest_listing['scrape_time'].timestamp())/60/60/24
		active = latest_listing['scrape_time'].timestamp() > most_recent_scrape - ACTIVE_PERIOD

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
			'net_price_change': net_price_change,
			'price_increases': price_increases,
			'price_decreases': price_decreases
		}

	return augmented_vehicles

def plot_vehicles(vehicles, active_vehicles, inactive_vehicles, model):
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

	def active_inactive_color_func(v): return ACTIVE_COLOR if v['active'] == True else INACTIVE_COLOR
	active_inactive_color_legend = [
		{'label': 'active', 'color': ACTIVE_COLOR},
		{'label': 'inactive', 'color': INACTIVE_COLOR}
	]

	def constant_color_func(v): return DEFAULT_COLOR

	### summary of vehicle volume & price trends
	# price over time by vehicle
	price_series_format = {'marker_style': '.', 'first_marker_style': {'color': 'g', 'style': 'o'}, 'last_marker_style': {'color': 'r', 'style': 'o'}}
	plotter.single_line_plot(
		vehicles,
		listings_time_func, listings_price_func, 
		f'{model}: listing trends', legend_entries=None,
		plot_order_func=listings_plot_order_func, x_axis_format=time_format, y_axis_format=price_axis_format, series_format=price_series_format
	)

	### price evalutation
	# mileage vs price colored by active status, single plot
	plotter.single_scatter(
		vehicles,
		mileage_data_func, price_data_func, active_inactive_color_func,
		f'{model}: price evaluation', legend_entries=active_inactive_color_legend,
		x_axis_format=mileage_axis_format, y_axis_format=price_axis_format
	)

	### year, mileage & price comparison
	# year vs. mileage scatter colored by price
	plotter.single_scatter(
		vehicles,
		year_data_func, mileage_data_func, price_data_func,
		f'{model}: year, mileage & price comparsion',
		x_axis_format=year_axis_format, y_axis_format=mileage_axis_format, color_axis_format=price_axis_format
	)

	### price & days detected correlation
	# mileage vs price by days detected
	plotter.single_scatter(
		vehicles,
		mileage_data_func, price_data_func, days_detected_data_func,
		f'{model}: price & days detected correlation',
		x_axis_format=mileage_axis_format, y_axis_format=price_axis_format, color_axis_format=days_detected_format
	)

def main():
	config_public = json.load(open(CONFIG_PUBLIC_PATH))
	config_secret = json.load(open(CONFIG_SECRET_PATH))
	config = {**config_public, **config_secret}

	py_postgres.connect(config['pg_config'])
	data = py_postgres.query(DATA_QUERY, headers=DATA_QUERY_HEADERS)

	for model, model_config in config['scrape_configs'].items():
		raw_model_data = [row for row in data if row['model'] == model]

		model_listings = prep_listings(raw_model_data, {'verbose': VERBOSE})
		raw_model_vehicles = get_unique_vehicles(model_listings)
		model_vehicles = augment_vehicles(raw_model_vehicles)

		all_vehicles = [vehicle for vehicle in model_vehicles.values()]
		active_vehicles = [vehicle for vehicle in model_vehicles.values() if vehicle['active'] == True]
		inactive_vehicles = [vehicle for vehicle in model_vehicles.values() if vehicle['active'] == False]
		print(f'{model}: found {len(active_vehicles)} active and {len(inactive_vehicles)} inactive vehicles')

		plot_vehicles(all_vehicles, active_vehicles, inactive_vehicles, model)



main()