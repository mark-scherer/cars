## plotting interface

import os
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors
from matplotlib.ticker import FuncFormatter, MultipleLocator
from matplotlib.axis import Axis 

import py_utils

SINGLE_FIGURE_SIZE = (8, 5)
DOUBLE_FIGURE_SIZE = (14, 5)
PADDING_COEF = 0.1
RESULTS_DIR = 'results'

DEFAULT_SIZE = None
DEFAULT_MARKER = None
DEFAULT_COLORMAP = 'winter_r'

# params:
	# colorbar: figure will add colorbar?
def _format_figure(colorbar):
	if colorbar:
		plt.subplots_adjust(right=0.8, wspace=0.3)

def _save_figure(fig, title):
	if not os.path.exists(RESULTS_DIR):
		os.makedirs(RESULTS_DIR)
	filepath = f'{RESULTS_DIR}/{title.replace(" ", "_").replace(":", "")}.png'
	fig.savefig(filepath)

def _create_legend(ax, legend_entries):
	handles = []
	for entry in legend_entries:
		handles.append(mpatches.Patch(**py_utils.dict_omit(entry, ['type', 'label'])))
	ax.legend(handles, [e['label'] for e in legend_entries])

def _format_axis(ax, axis, format_params):
	axis_ref = None
	set_label_func = None
	limit_func = None
	get_ticks_func = None
	if axis == 'x':
		axis_ref = ax.xaxis
		set_label_func = ax.set_xlabel
		limit_func = ax.set_xlim
		get_ticks_func = ax.get_xticks
	elif axis == 'y':
		axis_ref = ax.yaxis
		set_label_func = ax.set_ylabel
		limit_func = ax.set_ylim
		get_ticks_func = ax.get_yticks
	elif axis == 'color':
		def color_set_label_func(label):
			ax.set_xlabel(label, labelpad=10)
			ax.get_xaxis().set_label_position('top') 

		axis_ref = ax.get_yaxis()
		set_label_func = color_set_label_func
		limit_func = ax.set_ylim
		get_ticks_func = ax.get_yticks

	# label
	if 'label' in format_params:
		set_label_func(format_params['label'])

	# limits
	if 'limits' in format_params:
		limit_func(format_params['limits'])

	# tick interval
	if 'tick_interval' in format_params:
		axis_ref.set_major_locator(MultipleLocator(base=format_params['tick_interval']))
	if 'tick_locator' in format_params:
		axis_ref.set_major_locator(format_params['tick_locator'])
	if 'min_tick_interval' in format_params:
		auto_ticks = get_ticks_func()
		auto_tick_interval = (max(auto_ticks) - min(auto_ticks)) / (len(auto_ticks) - 1)
		axis_ref.set_major_locator(MultipleLocator(base=max([format_params['min_tick_interval'], auto_tick_interval])))

	# format ticks
	if 'tick_format_func' in format_params:
		axis_ref.set_major_formatter(FuncFormatter(format_params['tick_format_func']))

def _format_series(ax, line, format_params):

	# markers
	if 'marker_style' in format_params:
		line.set_marker(format_params['marker_style'])
	if 'first_marker_style' in format_params:
		ax.scatter(line.get_xdata()[0], line.get_ydata()[0], c=format_params['first_marker_style']['color'], marker=format_params['first_marker_style']['style'], zorder=4)
	if 'last_marker_style' in format_params:
		ax.scatter(line.get_xdata()[-1], line.get_ydata()[-1], c=format_params['last_marker_style']['color'], marker=format_params['last_marker_style']['style'], zorder=4)

def _calc_axis_lims(data):
	_min = min(data)
	_max = max(data)
	padding = PADDING_COEF*(_max - _min)
	return [_min - padding, _max + padding]

def _create_colorbar(fig, axes, colormap, color_data, color_axis_format):
	norm = mcolors.Normalize(vmin=min(color_data), vmax=max(color_data))
	cbar_ax = fig.add_axes([0.85, 0.15, 0.05, 0.7])
	cbar = fig.colorbar(plt.cm.ScalarMappable(cmap=colormap, norm=norm), cax=cbar_ax)
	_format_axis(cbar.ax, 'color', color_axis_format)

# if color_axis_format is specified, will add colorbar
def _make_scatter(
	data,
	fig, ax,
	x_func, y_func, color_func, 
	title, legend_entries,
	size, marker, colormap,
	x_axis_format, y_axis_format, color_axis_format
):
	_x = [x_func(v) for v in data]
	_y = [y_func(v) for v in data]
	_c = [color_func(v) for v in data]
	
	ax.scatter(_x, _y, s=size, c=_c, marker=marker, cmap=colormap)

	# colorbar
	if color_axis_format:
		_create_colorbar(fig, [ax], colormap, _c, color_axis_format)
	
	# labels, legend
	ax.set_title(title)
	if legend_entries:
		_create_legend(ax, legend_entries)

	# axis formatting
	if x_axis_format:
		_format_axis(ax, 'x', x_axis_format)
	if y_axis_format:
		_format_axis(ax, 'y', y_axis_format)

# notes
	# data should be iterable to apply x_func, y_func
	# x_func, y_func should produce lists to plot
def _make_line_plot(
	data,
	ax,
	x_func, y_func, 
	title, legend_entries,
	size, marker,
	plot_order_func, x_axis_format, y_axis_format, series_format
):
	for d in sorted(data, key=plot_order_func):
		_x = x_func(d)
		_y = y_func(d)

		lines = ax.plot(_x, _y)

		if series_format is not None:
			for l in lines:
				_format_series(ax, l, series_format)

	ax.set_title(title)
	if legend_entries:
		_create_legend(ax, legend_entries)

	# axis formatting
	if x_axis_format:
		_format_axis(ax, 'x', x_axis_format)
	if y_axis_format:
		_format_axis(ax, 'y', y_axis_format)



# optional params handled by generic methods:
	# legend_entries: _create_legend(...)
	# *_axis_format: _format_axis(...)
# if color_axis_format is specified, will add colorbar
def single_scatter(
	data,
	x_func, y_func, color_func, 
	title, legend_entries=None,
	size=DEFAULT_SIZE, marker=DEFAULT_MARKER, colormap=DEFAULT_COLORMAP,
	x_axis_format=None, y_axis_format=None, color_axis_format=None
):
	fig, ax = plt.subplots(figsize=SINGLE_FIGURE_SIZE)
	_format_figure(color_axis_format is not None)
	_make_scatter(
		data,
		fig, ax,
		x_func, y_func, color_func, 
		title, legend_entries=legend_entries,
		size=size, marker=marker, colormap=colormap,
		x_axis_format=x_axis_format, y_axis_format=y_axis_format, color_axis_format=color_axis_format
	)
	_save_figure(fig, title)
	plt.show()

# if color_axis_format is specified, will add colorbar
def double_scatter(
	data_1, data_2,
	x_func, y_func, color_func,
	data_1_label, data_2_label,
	title, legend_entries=None,
	size=DEFAULT_SIZE, marker=DEFAULT_MARKER, colormap=DEFAULT_COLORMAP,
	x_axis_format={}, y_axis_format={}, color_axis_format={}
):
	all_x_data = [x_func(v) for v in data_1 + data_2]
	all_y_data = [y_func(v) for v in data_1 + data_2]
	all_color_data = [color_func(v) for v in data_1 + data_2]

	x_axis_format.update({'limits': _calc_axis_lims(all_x_data)})
	y_axis_format.update({'limits': _calc_axis_lims(all_y_data)})

	fig, axes = plt.subplots(nrows=1, ncols=2, figsize=DOUBLE_FIGURE_SIZE)
	_format_figure(color_axis_format is not None)

	# do not let _make_scatter(...) add colorbar to individual plots, add here once to figure
	_make_scatter(
		data_1,
		fig, axes[0],
		x_func, y_func, color_func, 
		data_1_label, legend_entries=legend_entries,
		size=size, marker=marker, colormap=colormap,
		x_axis_format=x_axis_format, y_axis_format=y_axis_format, color_axis_format=None
	)
	_make_scatter(
		data_2,
		fig, axes[1],
		x_func, y_func, color_func, 
		data_2_label, legend_entries=legend_entries,
		size=size, marker=marker, colormap=colormap,
		x_axis_format=x_axis_format, y_axis_format=y_axis_format, color_axis_format=None
	)

	if color_axis_format:
		_create_colorbar(fig, axes, colormap, all_color_data, color_axis_format)

	fig.suptitle(title)
	_save_figure(fig, title)
	plt.show()

# optional params handled by generic methods:
	# legend_entries: _create_legend(...)
	# *_axis_format: _format_axis(...)
	# series_format: _format_series(...) <- all series same formatting!
def single_line_plot(
	data,
	x_func, y_func, 
	title, legend_entries=None,
	size=DEFAULT_SIZE, marker=DEFAULT_MARKER,
	plot_order_func=None, x_axis_format={}, y_axis_format={}, series_format={}
):
	fig, ax = plt.subplots(figsize=SINGLE_FIGURE_SIZE)
	_format_figure(False)
	_make_line_plot(
		data,
		ax,
		x_func, y_func,
		title, legend_entries=legend_entries,
		size=size, marker=marker,
		plot_order_func=plot_order_func, x_axis_format=x_axis_format, y_axis_format=y_axis_format, series_format=series_format
	)
	_save_figure(fig, title)
	plt.show()

