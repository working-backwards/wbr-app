import datetime
import logging
import tempfile
import traceback
from json import JSONEncoder
from typing import List

import dateutil
import dateutil.relativedelta
import numpy
import numpy as np
import pandas as pd
import requests
import yaml
from yaml import SafeLoader
from yaml._yaml import ScannerError

from src.wbr import WBR
from src.wbr_utility import if_else, put_into_map, if_else_supplier, append_to_list, is_last_day_of_month


class SixTwelveChart:
    def __init__(self):
        self.plotStyle = "6_12_chart"
        self.title = ""
        self.yLabel = ""
        self.yScale = ""
        self.boxTotalScale = "%"
        self.axes = 0
        self.xAxis = []
        self.yAxis = []
        self.table = {}
        self.tooltip = "false"


class MetricObject:
    def __init__(self):
        self.current = []
        self.previous = []


class Deck:
    def __init__(self):
        self.blocks: List[SixTwelveChart, TrailingTable, EmbeddedContent, SectionBody] = list()
        self.title = ""
        self.weekEnding = ""
        self.blockStartingNumber = 1
        self.xAxisMonthlyDisplay = None


class Rows:
    def __init__(self):
        self.rowHeader = ""
        self.rowData = []
        self.rowStyle = ""
        self.yScale = ""


class TrailingTable:
    def __init__(self):
        self.plotStyle = ""
        self.title = ""
        self.headers = []
        self.rows = []


class EmbeddedContent:
    def __init__(self):
        self.plotStyle = "embedded_content"
        self.id = ""
        self.source = ""
        self.name = ""
        self.height = ""
        self.width = ""
        self.title = ""


class SectionBody:
    def __init__(self):
        self.plotStyle = "section"
        self.title = ""


class Encoder(JSONEncoder):
    def default(self, o):
        return o.__dict__


class SafeLineLoader(SafeLoader):
    def construct_mapping(self, node, deep=False):
        mapping = super(SafeLineLoader, self).construct_mapping(node, deep=deep)
        # Add 1 so line numbering starts at 1
        mapping['__line__'] = node.start_mark.line + 1
        return mapping


def get_primary_and_secondary_axis_value_list(series, is_single_axis):
    """
    Processes a time series and determines primary and secondary axis values for weekly
    and monthly data points. Also decides whether the data can be displayed on a single axis.

    Args:
        series (list or array-like): A series containing numerical data for both weekly
                                     (first 7 elements) and monthly (next 13 elements) time periods.
        is_single_axis (bool): A flag indicating whether the data should be plotted on a single axis.

    Returns:
        tuple:
            - primary_and_secondary_axis_value_list (list): Contains two dictionaries:
              - 'primaryAxis': weekly data padded with empty strings for alignment with monthly data.
              - 'secondaryAxis': monthly data padded with empty strings for alignment with weekly data.
            - is_single_axis (bool): Updated flag indicating whether a single axis should be used
                                     based on the ratio of monthly to weekly maximum values.
    """

    # Extract weekly (first 7 elements) and monthly (next 13 elements) data from the series
    weekly_series = series[0:6]
    monthly_series = series[7:19]

    # Check if the series contains float or integer values and compute maximum values
    if weekly_series.dtype.type is numpy.float64 or weekly_series.dtype.type is numpy.int:
        # Mask NaN values and calculate the maximum of weekly and monthly series
        weekly_max = numpy.ma.array(weekly_series, mask=numpy.isnan(series[0:6])).max()
        monthly_max = numpy.ma.array(monthly_series, mask=numpy.isnan(series[7:19])).max()

        # Determine if both weekly and monthly data can be shown on a single axis
        is_single_axis = True if weekly_max > 0 and 0 < monthly_max / weekly_max <= 3 else False

    # Replace empty strings or NaN values in the series with empty strings for display purposes
    series = ["" if val == "" or np.isnan(val) else val for val in series]

    # Initialize the list to hold primary and secondary axis values
    primary_and_secondary_axis_value_list = []

    # Prepare primary (weekly) axis values with padding to align with monthly data
    wkd = series[0:6]
    wkd.extend(['', '', '', '', '', '', '', '', '', '', '', '', ''])  # Pad to align with monthly data length

    # Prepare secondary (monthly) axis values with padding to align with weekly data
    md = ['', '', '', '', '', '', '']  # Pad to align with weekly data length
    md.extend(series[7:19])

    # Store the primary and secondary axis values in dictionaries
    primary_axis_values = {'primaryAxis': wkd}
    secondary_axis_values = {'secondaryAxis': md}

    # Add the dictionaries to the result list
    primary_and_secondary_axis_value_list.append(primary_axis_values)
    primary_and_secondary_axis_value_list.append(secondary_axis_values)

    # Return the axis values and the updated single-axis flag
    return primary_and_secondary_axis_value_list, is_single_axis


def _6_12_chart(decks, plot, wbr1: WBR, block_number):
    """
    Builds a "6-12 Chart" for data visualization, determining whether to use single or dual axes
    based on data series metrics. The chart includes current and prior year data, axis labels,
    table data, and other configurable aspects like scaling and tooltips.

    Args:
        decks (Deck): Object containing the deck of blocks/charts being created.
        plot (dict): Configuration for the plot block, containing parameters such as title, y-axis scaling, etc.
        wbr1 (WBR): Data object that contains the current week's report data, metrics, and configurations.
        block_number (str): The block number for which the chart is being built, useful for logging and error handling.

    Raises:
        SyntaxError: If required metrics are not specified in the plot configuration.
        KeyError: If a specified metric is not found in the WBR data.
        Exception: Generic error in case of any issues during the chart-building process.

    """

    is_single_axis = False  # Flag to determine if a single axis is sufficient for display.
    plotting_dict = plot['block']
    six_twelve_chart = get_6_12_chart_instance(plotting_dict, wbr1)

    # Determine the end date, accounting for whether it's the last day of the month.
    end_date = if_else_supplier(wbr1, lambda wbr: is_last_day_of_month(wbr.cy_week_ending),
                                lambda wbr: wbr.cy_week_ending,
                                lambda wbr: wbr.cy_week_ending.replace(day=1) - datetime.timedelta(days=1))

    # Get the fiscal start date based on the current week and fiscal month configuration.
    fiscal_start = get_month_start(wbr1.cy_week_ending.month, wbr1.cy_week_ending.year,
                                   datetime.datetime.strptime(wbr1.fiscal_month, '%b').month)

    # Determine the starting month for the x-axis display.
    is_trailing_twelve_months, month_start = _get_x_axis_start_month(block_number, decks, end_date, plotting_dict, wbr1)

    # Set the x-axis label based on the determined start month.
    six_twelve_chart.xAxis = get_x_axis_label(wbr1, month_start)

    # Validate that metrics are defined in the plot configuration.
    if 'metrics' not in plotting_dict:
        raise SyntaxError(f"Bad Request! Metrics are not specified in the configuration for block {block_number} line: "
                          f"{plotting_dict['__line__']}")

    metrices = plotting_dict['metrics']

    # Iterate over each metric in the metrics dictionary to populate the chart.
    is_single_axis = process_metric(
        block_number,
        fiscal_start,
        is_single_axis,
        is_trailing_twelve_months,
        metrices,
        six_twelve_chart,
        wbr1
    )

    # Set the number of axes based on whether a single or dual-axis is needed.
    six_twelve_chart.axes = plotting_dict['axes'] if 'axes' in plotting_dict else (1 if is_single_axis else 2)

    # Append the completed chart to the deck of blocks.
    decks.blocks.append(six_twelve_chart)


def process_metric(
        block_number,
        fiscal_start,
        is_single_axis,
        is_trailing_twelve_months,
        metrics,
        six_twelve_chart,
        wbr1
):
    """
    Processes metrics to build chart data for the 6-12 chart, including handling current and prior year data,
    line style configuration, and box totals.

    Args:
        block_number (str): The block number, useful for logging and error handling.
        fiscal_start (datetime): The start of the fiscal period.
        is_single_axis (bool): Flag indicating whether the chart should use a single axis.
        is_trailing_twelve_months (bool): Flag indicating if the data represents trailing twelve months.
        metrics (dict): Dictionary of metrics and their configuration from the plotting YAML.
        six_twelve_chart (SixTwelveChart): Chart object to which the processed data is added.
        wbr1 (WBR): Data object containing the report metrics and configurations.

    Returns:
        bool: Updated flag indicating if the chart should use a single axis.

    Raises:
        KeyError: If a metric is not found in the WBR data.
        Exception: For any other errors encountered during the metric processing.
    """

    box_value_list = []

    for metric, metric_configs in metrics.items():
        try:
            if metric == '__line__':
                continue  # Skip the '__line__' key used for configuration tracking.

            # Process the current and prior year data for the metric.
            metric_object, is_single_axis = _process_metric_data(
                metric, metric_configs, wbr1, fiscal_start, is_trailing_twelve_months, is_single_axis
            )

            # Build the dictionary for the metric's line style, legend, and other configurations.
            metrics_dictionary = _build_metric_dictionary(metric, metric_configs, metric_object)

            # Append the metric configuration to the chart's y-axis data.
            six_twelve_chart.yAxis.append(metrics_dictionary)

            # Configure the table headers for the box totals and append box total values.
            box_value_list = _update_box_totals(
                metric, metrics_dictionary, wbr1, box_value_list, six_twelve_chart
            )

        except Exception as error:
            # Log any errors that occur during the chart-building process.
            logging.error(error, exc_info=True)
            raise Exception(f"Error occurred while building block {block_number}, error: {error}, "
                            f"yaml line number {metric_configs['__line__']}")

    six_twelve_chart.table["tableBody"] = box_value_list
    return is_single_axis


def _process_metric_data(metric, metric_configs, wbr1, fiscal_start, is_trailing_twelve_months, is_single_axis):
    """
    Retrieves and processes both current and prior year data for a metric.

    Args:
        metric (str): The metric to be processed.
        metric_configs (dict): Configuration for the metric from the plotting YAML.
        wbr1 (WBR): Data object containing the report metrics.
        fiscal_start (datetime): The start of the fiscal period.
        is_trailing_twelve_months (bool): Flag indicating if the data represents trailing twelve months.
        is_single_axis (bool): Flag indicating whether the chart should use a single axis.

    Returns:
        MetricObject: An object containing both current and prior year metric data.
        bool: Updated flag indicating if the chart should use a single axis.

    Raises:
        KeyError: If the metric is not found in the WBR data.
    """

    if metric not in wbr1.metrics.columns:
        raise KeyError(f"Metric '{metric}' not found in the data at line {metric_configs['__line__']}")

    metric_object = MetricObject()

    # Process current year data.
    metric_data_series = get_metric_series_data(wbr1, metric, fiscal_start, is_trailing_twelve_months)
    metric_object.current, is_single_axis = get_primary_and_secondary_axis_value_list(
        metric_data_series, is_single_axis
    )

    # Process prior year data if configured.
    if "PY__" + metric in wbr1.metrics and ('graph_prior_year_flag' not in metric_configs or
                                            metric_configs['graph_prior_year_flag']):
        metric_data_series = get_metric_series_data(
            wbr1, 'PY__' + metric, fiscal_start, is_trailing_twelve_months
        )
        metric_object.previous, is_single_axis = get_primary_and_secondary_axis_value_list(
            metric_data_series, is_single_axis
        )

    return metric_object, is_single_axis


def _build_metric_dictionary(metric, metric_configs, metric_object):
    """
    Builds a dictionary containing the metric's line style, legend name, and other configurations.

    Args:
        metric (str): The name of the metric.
        metric_configs (dict): The configuration for the metric from the plotting YAML.
        metric_object (MetricObject): The object containing the metric's current and prior year data.

    Returns:
        dict: A dictionary containing the line style, legend name, and other metric properties.
    """

    metrics_dictionary = {"lineStyle": metric_configs.get('line_style', "primary")}

    # Configure line style, defaulting to 'primary'.

    # Handle special case where the line style is set to 'target'.
    if_else(metric_object,
            lambda x: metrics_dictionary['lineStyle'] == 'target',
            lambda x: put_into_map(x, metrics_dictionary, "Target"),
            lambda x: put_into_map(x, metrics_dictionary, "metric"))

    # Configure legend name, defaulting to the metric name if not provided.
    metrics_dictionary["legendName"] = metric_configs.get('legend_name', metric)

    return metrics_dictionary


def _update_box_totals(metric, metrics_dictionary, wbr1, box_value_list, six_twelve_chart):
    """
    Updates the box totals for the given metric and configures the table headers.

    Args:
        metric (str): The metric being processed.
        metrics_dictionary (dict): Dictionary containing the metric's properties.
        wbr1 (WBR): Data object containing the report metrics.
        box_value_list (list): List of box total values to update.
        six_twelve_chart (SixTwelveChart): Chart object to which the processed data is added.

    Returns:
        list: Updated list of box total values.
    """

    # Set the table headers for the box totals.
    box_axis_list = list(wbr1.box_totals['Axis'])
    six_twelve_chart.table["tableHeader"] = box_axis_list

    # Configure the box total scale based on whether the metric is a BPS metric.
    six_twelve_chart.boxTotalScale = 'bps' if (
            metric in wbr1.bps_metrics or metric in wbr1.function_bps_metrics) else "%"

    # Append the box total values for the metric, handling NaN and string values.
    if metrics_dictionary['lineStyle'] != 'target':
        box_value_list.append([value if not isinstance(value, str) and not numpy.isnan(value) else "N/A"
                               for value in wbr1.box_totals[metric]])

    return box_value_list


def _get_x_axis_start_month(block_number, decks, end_date, plotting_dict, wbr1):
    """
    Determines the start month for the x-axis based on plot configuration or deck settings.
    """
    if 'x_axis_monthly_display' in plotting_dict:
        month_start, is_trailing_twelve_months = get_x_axis_display_start_month(
            block_number, end_date, plotting_dict['x_axis_monthly_display'], wbr1, plotting_dict['__line__']
        )
    elif decks.xAxisMonthlyDisplay is not None:
        month_start, is_trailing_twelve_months = get_x_axis_display_start_month(
            block_number, end_date, decks.xAxisMonthlyDisplay, wbr1, plotting_dict['__line__']
        )
    else:
        # Default to a 12-month trailing view.
        month_start = (end_date - dateutil.relativedelta.relativedelta(months=11)).strftime("%b")
        is_trailing_twelve_months = True

    return is_trailing_twelve_months, month_start


def get_6_12_chart_instance(plotting_dict, wbr1):
    """
    Initializes the SixTwelveChart with basic properties such as title, y-scale, and tooltip.
    """
    six_twelve_chart = SixTwelveChart()  # Initialize the chart object.
    # Set chart title if provided in the plotting dictionary.
    six_twelve_chart.title = plotting_dict['title'] if 'title' in plotting_dict else None
    # Set y-axis scaling if provided, otherwise default to an empty string.
    six_twelve_chart.yScale = plotting_dict['y_scaling'] \
        if 'y_scaling' in plotting_dict and plotting_dict['y_scaling'] is not None else ""
    # Set tooltip based on the WBR configuration.
    six_twelve_chart.tooltip = "true" if 'tooltip' in wbr1.cfg['setup'] and wbr1.cfg['setup']['tooltip'] else "false"
    return six_twelve_chart


def get_x_axis_display_start_month(block_number, end_date: datetime, month_start, wbr1, line):
    """
    Determines the start month for the X-axis display based on the provided `month_start` value.
    Supports two options: 'fiscal_year' and 'trailing_twelve_months'.

    Args:
        block_number (int): The block number, useful for logging and error handling.
        end_date (datetime): The end date for the current period.
        month_start (str): The type of month start to display ('fiscal_year' or 'trailing_twelve_months').
        wbr1 (WBR): The WBR object containing fiscal month and other configurations.
        line (int): The line number in the configuration file, used for error logging.

    Returns:
        tuple:
            - str: The starting month for the X-axis.
            - bool: A flag indicating whether the display is for trailing twelve months.

    Raises:
        Exception: If `month_start` is not 'fiscal_year' or 'trailing_twelve_months'.
    """

    if month_start == 'fiscal_year':
        # Return the month following the fiscal year-end month as the fiscal year start month.
        # Convert the fiscal end month (wbr1.fiscal_month) into a datetime object, add one month,
        # and return the month name in abbreviated format ("%b").
        return (datetime.datetime.strptime(wbr1.fiscal_month, "%b") +
                dateutil.relativedelta.relativedelta(months=1)).strftime("%b"), False

    elif month_start == 'trailing_twelve_months':
        # Return the month that is 11 months prior to the `end_date`, representing the start of the trailing twelve
        # months.
        return (end_date - dateutil.relativedelta.relativedelta(months=11)).strftime("%b"), True

    else:
        # Raise an error if the `month_start` value is not 'fiscal_year' or 'trailing_twelve_months'.
        raise Exception(f"Expected 'fiscal_year' or 'trailing_twelve_months' but got {month_start} "
                        f"for block {block_number} at line: {line}")


def get_month_start(week_ending_month, week_ending_year, fiscal_month):
    """
    Determines the start month for the fiscal year by adjusting the given week-ending month to align with the fiscal
    month.
    Once aligned, it returns the last day of the month that is 11 months prior to the aligned fiscal month.

    Args:
        week_ending_month (int): The month of the week-ending date (1 = January, 12 = December).
        week_ending_year (int): The year of the week-ending date.
        fiscal_month (int): The month that corresponds to the end of the fiscal year.

    Returns:
        datetime: The last day of the month that is 11 months prior to the fiscal month in the adjusted year.
    """
    while week_ending_month != fiscal_month:
        if week_ending_month == 12 and week_ending_month != fiscal_month:
            # If the month is December and still not the fiscal month, increment the year and set the month to January.
            week_ending_year += 1
            week_ending_month = 1
        else:
            # Increment the month until it matches the fiscal month.
            week_ending_month += 1

    # Return the last day of the fiscal month, subtracting 11 months to get the start of the 12-month period.
    return last_day_of_month(datetime.date(week_ending_year, fiscal_month, 1)) - dateutil.relativedelta.relativedelta(
        months=11)


def last_day_of_month(any_day):
    """
    Returns the last day of the month for a given date.

    Args:
        any_day (datetime.date): A date within the month for which the last day is to be determined.

    Returns:
        datetime.date: The last day of the month in which `any_day` falls.
    """
    # The day 28 exists in every month. Adding 4 days guarantees that the date is in the next month.
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
    # Subtracting the number of days in the current day of `next_month` brings us back to the last day of the previous
    # month.
    return next_month - datetime.timedelta(days=next_month.day)


def get_metric_series_data(wbr1, metric, fiscal_start, is_trailing_twelve_months):
    """
    Retrieves and constructs a time series for the specified metric, aligning it with the fiscal start
    or a trailing twelve-month period.

    Args:
        wbr1 (WBR): The WBR object containing metrics data and other configurations.
        metric (str): The name of the metric to retrieve the time series data for.
        fiscal_start (datetime): The start date of the fiscal period.
        is_trailing_twelve_months (bool): If True, the series will cover the trailing twelve months.

    Returns:
        pd.Series: A pandas Series representing the time series of the specified metric,
        with up to 12 months of data.
    """

    # Start by taking the first 6 data points of the metric.
    metric_series = wbr1.metrics[metric].copy()[0:6]

    # Concatenate a NaN value for padding, then reset index for proper series alignment.
    metric_series = pd.concat([metric_series, pd.Series(np.nan)]).reset_index(drop=True)

    # Copy the metric data for the months beyond the first 6 data points.
    months_data = list(wbr1.metrics[metric].copy()[7:])

    # Copy the corresponding dates from the 'Date' column in the WBR metrics.
    axis_data = list(wbr1.metrics['Date'].copy()[7:])

    # Initialize conditions to track the month matching and total number of months processed.
    month_cond = False
    total_months = 1

    # Iterate through the months data to collect up to 12 months of aligned metric data.
    for i in range(len(months_data)):
        # Check if the current date aligns with the fiscal start date or the trailing twelve months condition is met.
        if ((str(axis_data[i]).replace(' 00:00:00', '').lower() == str(
                fiscal_start) or is_trailing_twelve_months or month_cond) and total_months <= 12):
            # Set month condition to True after the first match, and increment total_months.
            month_cond = True
            total_months += 1

            # Append the corresponding month's data to the metric series.
            metric_series = pd.concat([metric_series, pd.Series(months_data[i])]).reset_index(drop=True)

    return metric_series


def get_x_axis_label(wbr1, month_start):
    """
    Generates the x-axis labels for a chart, starting from a specific month and including up to 12 months of data.

    Args:
        wbr1 (WBR): The WBR object that contains the graph axis labels.
        month_start (str): The name of the month from which to start generating x-axis labels.

    Returns:
        list: A list of x-axis labels, starting with the first 7 labels from the WBR object, followed by
        up to 12 months of data starting from `month_start`.
    """
    # Start with the first 7 x-axis labels.
    x_axis_label = list(wbr1.graph_axis_label)[0:7]

    # Track whether we've encountered the starting month and count the total number of months added.
    month_cond = False
    total_months = 1

    # Extract the remaining month labels from the 8th element onward.
    month_labels = list(wbr1.graph_axis_label[7:])

    # Iterate over the month labels, appending them once the start month is found.
    for month_label in month_labels:
        # Check if the current month matches the start month or if the condition to append is already True.
        if (str(month_label).lower() == month_start.lower() or month_cond) and total_months <= 12:
            month_cond = True  # Start appending months once the condition is satisfied.
            total_months += 1  # Increment the count of months added.
            x_axis_label.append(month_label)

    return x_axis_label


def get_six_weeks_table_row_data(wbr1, metric, line_number):
    """
    Retrieves and constructs row data for a six-week table block, using metric data from the WBR object.

    Args:
        wbr1 (WBR): The WBR object containing metrics and box totals data.
        metric (str): The name of the metric to retrieve the six-week data for.
        line_number (int): The line number in the configuration file, used for error reporting.

    Returns:
        list: A list of six-week table data, containing either the metric values or " " (blank)
        for NaN values or unsupported metrics.

    Raises:
        Exception: If the metric is a 'MOM' type (Month-over-Month), as it is not supported in the six-week table.
        Exception: If configuration is invalid at the provided line number.
    """

    # Retrieve the metric data for the first 6 weeks.
    metric_data = wbr1.metrics[metric]

    # Replace NaN values with blank spaces for the six weeks of data.
    six_weeks_table_data = [" " if numpy.isnan(metric_data[i]) else metric_data[i] for i in range(0, 6)]

    # Raise an exception if the metric is a Month-over-Month (MOM) type, as it's unsupported for six weeks tables.
    if "MOM" in metric:
        raise Exception(f"MOM type of metric not supported in the 6 weeks table block. "
                        f"Please check your configuration at line: {line_number}")

    # If the metric is not a Week-over-Week (WOW) type, append additional data from box_totals.
    if "WOW" not in metric:
        # Check the 6th week's box total value, append " " if it is NaN or 'N/A', otherwise append the value.
        if_else(wbr1.box_totals.loc[5, metric], lambda x: x == 'N/A' or numpy.isnan(x),
                lambda x: append_to_list(" ", six_weeks_table_data),
                lambda x: append_to_list(x, six_weeks_table_data))

        # Check the 8th week's box total value, and apply the same logic as for the 6th week's value.
        if_else(wbr1.box_totals.loc[7, metric], lambda x: x == 'N/A' or numpy.isnan(x),
                lambda x: append_to_list(" ", six_weeks_table_data),
                lambda x: append_to_list(x, six_weeks_table_data))
    else:
        # If the metric is WOW type, append two blank spaces as placeholders for the box total values.
        six_weeks_table_data.append(" ")
        six_weeks_table_data.append(" ")

    return six_weeks_table_data


def get_twelve_months_table_row(wbr1, metric, itr_start):
    """
    Retrieves a row of data for a twelve-month table block, extracting metric values from the WBR object.

    Args:
        wbr1 (WBR): The WBR object containing metrics data.
        metric (str): The name of the metric to retrieve data for.
        itr_start (int): The starting index for extracting twelve months of data.

    Returns:
        list: A list containing twelve months of metric data, with NaN values replaced by blank spaces.
    """

    # Retrieve the metric data for the specified metric from the WBR object.
    metric_data = wbr1.metrics[metric]

    # Generate a list for twelve months of data, replacing NaN values with blank spaces.
    return [" " if numpy.isnan(metric_data[i]) else metric_data[i] for i in range(itr_start, itr_start + 12)]


def _6_weeks_table(decks, plot, wbr1: WBR, block_number):
    """
    Constructs a 6-week table block for a specified plot using data from a WBR object.

    Args:
        decks: An object representing the collection of blocks for the report.
        plot: A dictionary containing plotting configurations for the table block.
        wbr1 (WBR): The WBR object containing metrics data.
        block_number (str): The identifier for the block being constructed.

    Raises:
        SyntaxError: If rows are not specified in the plotting configuration.
        KeyError: If a specified metric is not found in the WBR metrics.
        Exception: For general errors that occur while building the block.
    """

    # Retrieve the block configuration from the plot.
    plotting_dict = plot['block']

    # Initialize the table object for the six weeks table.
    six_weeks_table = TrailingTable()
    six_weeks_table.plotStyle = "6_week_table"  # Set the plot style.

    # Set the title for the table if provided in the plotting configuration.
    if 'title' in plotting_dict:
        six_weeks_table.title = plotting_dict['title']

    # Create the column headers for the table.
    table_column_header = [wbr1.graph_axis_label[i] for i in range(0, 6)]
    table_column_header.append("QTD")  # Add QTD column header.
    table_column_header.append("YTD")  # Add YTD column header.
    build_six_weeks_table(block_number, plotting_dict, six_weeks_table, table_column_header, wbr1)

    # Append the completed six weeks table to the decks collection.
    decks.blocks.append(six_weeks_table)


def build_six_weeks_table(
        block_number: str,
        plotting_dict: dict,
        six_weeks_table: TrailingTable,
        table_column_header: list,
        wbr1: WBR
):
    """
    Builds a six weeks table using the specified plotting configuration.

    Args:
        block_number (str): The block number for logging and error handling.
        plotting_dict (dict): The configuration dictionary for the plotting that includes rows and other properties.
        six_weeks_table (TrailingTable): The table object to populate with rows and headers.
        table_column_header (list): The headers for the table, representing the weeks and additional metrics.
        wbr1 (WBR): The WBR object containing metric data necessary for building table rows.

    Raises:
        SyntaxError: If the 'rows' key is not present in the plotting configuration.
        Exception: If an error occurs while constructing the rows from the configuration, with context on the failure.
    """
    six_weeks_table.headers = table_column_header  # Assign headers to the table.
    # Validate that rows are specified in the plotting configuration.
    if 'rows' not in plotting_dict:
        raise SyntaxError(f"Bad Request! rows are not specified in the configuration for block: {block_number} line: "
                          f"{plotting_dict['__line__']}")
    else:
        # Iterate over each row configuration to build table rows.
        for row_configs in plotting_dict['rows']:
            try:
                row = build_six_week_table_row(row_configs, wbr1)

                # Append the constructed row to the table.
                six_weeks_table.rows.append(row)
            except Exception as e:
                # Log any errors and raise an exception with context for debugging.
                logging.error(e, exc_info=True)
                raise Exception(f"Error occurred while building block {block_number}, error: {e}, yaml line number "
                                f"{row_configs['__line__']}")


def build_six_week_table_row(row_configs: dict, wbr1: WBR):
    """
    Constructs a row for the six weeks table based on the provided configuration.

    Args:
        row_configs (dict): A dictionary containing the configuration for the row, which includes
                            'header', 'metric', 'style', and 'y_scaling'.
        wbr1 (WBR): The WBR object containing metric data necessary for retrieving the metric values.

    Raises:
        KeyError: If the specified metric is not found in the WBR metrics dataframe.

    Returns:
        Rows: A Rows object populated with the configured header, data, style, and scaling information.
    """
    row_config = row_configs['row']
    row = Rows()  # Initialize a new row object.
    # Set the row header if provided.
    if 'header' in row_config:
        row.rowHeader = row_config['header']
    # Validate and retrieve the metric data for the row.
    if 'metric' in row_config:
        if row_config['metric'] not in wbr1.metrics.columns:
            raise KeyError(
                f"Error in yaml at line: {row_config['__line__']}, Metric {row_config['metric']} not found in "
                f"the dataframe, please check if you have defined this metric in metric section")
        # Get data for the six weeks table row.
        row.rowData = get_six_weeks_table_row_data(wbr1, row_config['metric'], row_config['__line__'])
    # Set additional properties for the row if specified.
    if 'style' in row_config:
        row.rowStyle = row_config['style']
    if 'y_scaling' in row_config:
        row.yScale = row_config['y_scaling']
    return row


def _12_months_table(decks, plot, wbr1: WBR, block_number):
    """
    Constructs and populates a 12-months table based on the provided plotting configuration and WBR data.

    Args:
        decks (Decks): The Decks object to which the table will be appended.
        plot (dict): A dictionary containing the configuration for the plot, including block settings.
        wbr1 (WBR): The WBR object containing financial data and metadata.
        block_number (str): The number representing the current block in the configuration.

    Raises:
        ValueError: If a valid month_start cannot be determined or if other configuration errors occur.

    Returns:
        None: This function does not return a value; it appends the created table to the decks.
    """
    plotting_dict = plot['block']
    twelve_months_table = TrailingTable()
    twelve_months_table.plotStyle = "12_MonthsTable"
    if 'title' in plotting_dict:
        twelve_months_table.title = plotting_dict['title']

    itr_start = 7

    if 'x_axis_monthly_display' in plotting_dict:
        month_start = plotting_dict['x_axis_monthly_display']
    elif decks.xAxisMonthlyDisplay is not None:
        month_start = decks.xAxisMonthlyDisplay
    else:
        month_start = 'trailing_twelve_months'

    # Determine the fiscal month if month_start is 'fiscal_year'
    if month_start == 'fiscal_year':
        fiscal_month = (datetime.datetime.strptime(wbr1.fiscal_month, "%b") +
                        dateutil.relativedelta.relativedelta(months=1)).strftime("%b")

        # Calculate the starting index for the twelve-month table
        itr_start = next(
            (i for i, month in enumerate(wbr1.graph_axis_label[7:])
             if month.lower() == fiscal_month.lower()),
            len(wbr1.graph_axis_label[7:])  # Fallback in case fiscal_month is not found
        )

    build_12_months_table(block_number, itr_start, plotting_dict, twelve_months_table, wbr1)

    decks.blocks.append(twelve_months_table)


def build_12_months_table(block_number, itr_start, plotting_dict, twelve_months_table, wbr1):
    """
    Constructs and populates a 12-months table with the specified headers and rows based on the provided
    plotting configuration and WBR data.

    Args:
        block_number (str): The number representing the current block in the configuration.
        itr_start (int): The starting index for the 12-month period in the graph axis labels.
        plotting_dict (dict): A dictionary containing the configuration for the plot, including row settings.
        twelve_months_table (TrailingTable): The table object to be populated with data.
        wbr1 (WBR): The WBR object containing financial data and metadata.

    Raises:
        SyntaxError: If the 'rows' key is not present in the plotting_dict.
        Exception: If an error occurs while constructing a row.

    Returns:
        None: This function does not return a value; it appends the constructed rows to the twelve_months_table.
    """
    # Set the headers for the twelve-months table
    twelve_months_table.headers = wbr1.graph_axis_label[itr_start:itr_start + 12]
    if 'rows' not in plotting_dict:
        raise SyntaxError(f"Bad Request! rows are not specified in the configuration at block: {block_number} at line: "
                          f"{plotting_dict['__line__']}")
    for row_configs in plotting_dict['rows']:
        try:
            row = build_twelve_month_table_row(itr_start, row_configs, wbr1)

            twelve_months_table.rows.append(row)
        except Exception as e:
            logging.error(e, exc_info=True)
            raise Exception(f"Error occurred while building block {block_number}, error: {e}, yaml line number "
                            f"{row_configs['__line__']}")


def build_twelve_month_table_row(itr_start, row_configs, wbr1):
    """
    Constructs a row for a twelve-months table based on the provided row configuration and WBR data.

    Args:
        itr_start (int): The starting index for the 12-month period in the graph axis labels.
        row_configs (dict): A dictionary containing the configuration for the row, including metric and style settings.
        wbr1 (WBR): The WBR object containing financial data and metadata.

    Raises:
        KeyError: If the specified metric is not found in the WBR metrics dataframe.

    Returns:
        Rows: A Rows object populated with the specified header, style, scaling, and data for the twelve-months table.
    """
    row_config = row_configs['row']
    row = Rows()
    if 'header' in row_config:
        row.rowHeader = row_config['header']
    if 'style' in row_config:
        row.rowStyle = row_config['style']
    if 'y_scaling' in row_config:
        row.yScale = row_config['y_scaling']
    if 'metric' in row_config:
        if row_config['metric'] not in wbr1.metrics.columns:
            raise KeyError(
                f"Error in yaml at line: {row_config['__line__']}, Metric {row_config['metric']} not found in "
                f"the dataframe, please check if you have defined this metric in metric section")
        row.rowData = get_twelve_months_table_row(wbr1, row_config['metric'], itr_start)
    return row


def append_section_to_deck(decks, plot):
    """
    Appends a new section to the provided decks based on the configuration in the plotting dictionary.

    Args:
        decks (Decks): The Decks object to which the new section will be added.
        plot (dict): A dictionary containing the configuration for the block, including optional title.
    """
    plotting_dict = plot['block']
    section = SectionBody()
    if 'title' in plotting_dict:
        section.title = plotting_dict['title']
    decks.blocks.append(section)


def append_embedded_content_to_deck(decks, plot):
    """
    Appends embedded content to the provided decks based on the configuration in the plotting dictionary.

    Args:
        decks (Decks): The Decks object to which the embedded content will be added.
        plot (dict): A dictionary containing the configuration for the block, including the source of the content,
                     and optional title, name, width, and height.
    """
    plotting_dict = plot['block']
    embedded_content = EmbeddedContent()
    embedded_content.source = plotting_dict['source']
    embedded_content.id = "iframe_id"
    if 'title' in plotting_dict:
        embedded_content.title = plotting_dict['title']
    if 'name' in plotting_dict:
        embedded_content.name = plotting_dict['name']
    if "width" in plotting_dict:
        embedded_content.width = int(plotting_dict['width'][:-2])
    if 'height' in plotting_dict:
        embedded_content.height = int(plotting_dict['height'][:-2])
    decks.blocks.append(embedded_content)


def get_wbr_deck(wbr1: WBR) -> Deck:
    """
    Constructs a Deck object based on the configuration provided in the wbr1 object.

    Args:
        wbr1 (WBR): An instance of the WBR class containing configuration data for the deck.

    Returns:
        Deck: A Deck object populated with plots, titles, and other settings defined in the wbr1 configuration.
    """
    plots = wbr1.cfg['deck']
    deck = Deck()

    if 'x_axis_monthly_display' in wbr1.cfg['setup']:
        deck.xAxisMonthlyDisplay = wbr1.cfg['setup']['x_axis_monthly_display']

    for i in range(len(plots)):
        build_a_block(deck, i, plots, wbr1)

    deck.title = wbr1.cfg['setup']['title']

    week_ending = datetime.datetime.strptime(wbr1.cfg['setup']['week_ending'], '%d-%b-%Y')
    deck.weekEnding = week_ending.strftime("%d") + " " + week_ending.strftime("%B") + " " + week_ending.strftime("%Y")

    if 'block_starting_number' in wbr1.cfg['setup']:
        deck.blockStartingNumber = wbr1.cfg['setup']['block_starting_number']

    return deck


def build_a_block(deck: Deck, i: int, plots: list, wbr1: WBR):
    """
    Builds a block in the given deck based on the configuration specified in the plots.

    Args:
        deck (Deck): The Deck object to which the block will be added.
        i (int): The index of the current block in the plots list.
        plots (list): A list of plot configurations, each containing a block configuration.
        wbr1 (WBR): An instance of the WBR class containing additional configuration data.

    Raises:
        Exception: If the block configuration is invalid or if the UI type is not recognized.
    """
    if 'block' not in plots[i]:
        raise Exception(f"Invalid block configuration for block number {str(i + 1)} in DECK Section at line:"
                        f" {plots[i]['__line__']}")
    plotting_dict = plots[i]['block']
    if 'ui_type' not in plotting_dict or plotting_dict['ui_type'] is None:
        raise Exception(f"UI Type can not be Null for Block Number {str(i + 1)} in DECK Section at line:"
                        f" {plotting_dict['__line__']}")
    elif plotting_dict['ui_type'] == '6_12Graph':
        _6_12_chart(deck, plots[i], wbr1, str(i + 1))
    elif plotting_dict['ui_type'] == '6_WeeksTable':
        _6_weeks_table(deck, plots[i], wbr1, str(i + 1))
    elif plotting_dict['ui_type'] == '12_MonthsTable':
        _12_months_table(deck, plots[i], wbr1, str(i + 1))
    elif plotting_dict['ui_type'] == 'section':
        append_section_to_deck(deck, plots[i])
    elif plotting_dict['ui_type'] == 'embedded_content':
        append_embedded_content_to_deck(deck, plots[i])
    else:
        raise Exception(
            f"Invalid UI Type for block number {str(i + 1)} in DECK Section at line: {plotting_dict['__line__']}"
        )


def get_dict(column):
    metric_dict = {'column': column, 'aggf': 'sum'}
    return metric_dict


def get_metric_block(metric, target):
    metric_block_config = {metric: {'line_style': 'primary', 'graph_prior_year_flag': True}}
    if target is not None:
        metric_block_config[target] = {'line_style': 'target', 'graph_prior_year_flag': False}
    return metric_block_config


format_dict = {
    lambda series: series.describe()["mean"] / 1000000000 > 1: "##BB",
    lambda series: series.describe()["mean"] / 1000000 > 1: "##MM",
    lambda series: series.describe()["mean"] / 1000 > 1: "##KK",
    lambda series: ((series.dropna() >= 0) & (series.dropna() <= 1)).all(): "##%"
}


def get_scaling(series_data):
    for format_func, mask in format_dict.items():
        if format_func(series_data):
            return mask


def generate_custom_yaml(temp_file, csv_data):
    """
    Generates a custom YAML configuration file based on the provided CSV data.

    Args:
        temp_file (file-like object): A writable file object where the generated YAML will be saved.
        csv_data (pandas.DataFrame): A DataFrame containing the data from a CSV file, including metrics and dates.
    """
    columns = list(csv_data.columns)
    configs = {}

    # Configuration for WBR setup
    wbr_setup_config = {
        'week_ending': 'Please enter a week ending date, <dd-MMM-YYYY> eg: 25-SEP-2021',
        'week_number': 'Enter the week number of week ending date',
        'title': 'A title for your WBR',
        'x_axis_monthly_display': 'trailing_twelve_months'
    }

    configs['setup'] = wbr_setup_config

    metric_config_dict = {}

    # Generate metric configurations
    for column in columns:
        if column != 'Date' and (csv_data[column].dtype == int or csv_data[column].dtype == float):
            metric_config_dict[column] = get_dict(column)

    configs['metrics'] = metric_config_dict

    metric_keyset = list(metric_config_dict.keys())
    blocks = []

    # Generate deck configurations for each metric
    for metric in metric_keyset:
        deck_config_dict = {}

        # Check if there is a target metric for the current metric
        suffixes = ["__Target", "__target"]

        # find first from list or else None
        target = next((metric + suffix for suffix in suffixes if metric + suffix in metric_keyset), None)

        if target:
            metric_keyset.remove(target)

        # Get the mean column value for scaling
        mean_column_value = get_scaling(csv_data[metric])

        # Create block configuration for the metric
        block_config = {
            'ui_type': '6_12Graph',
            'title': metric,
            'y_scaling': mean_column_value,
            'metrics': get_metric_block(metric, target)
        }

        deck_config_dict['block'] = block_config
        blocks.append(deck_config_dict)

    configs['deck'] = blocks

    with open(temp_file.name, 'a') as file:
        yaml.dump(configs, file, sort_keys=False)


def load_yaml_from_stream(config_file):
    # Create a temporary file to save the configuration file
    with tempfile.NamedTemporaryFile(mode="a", delete=False) as temp_file:
        config_file.save(temp_file.name)
        try:
            # Load the YAML configuration from the temporary file
            return yaml.load(open(temp_file.name), SafeLineLoader)
        except (ScannerError, yaml.YAMLError) as e:
            logging.error(e, exc_info=True)
            error_message = traceback.format_exc().split('.yaml')[-1].replace(',', '').replace('"', '')
            # Return an error response if there is an issue with the YAML configuration
            raise Exception(
                f"Could not create WBR metrics due to incorrect yaml, caused due to error in {error_message}")
        finally:
            temp_file.close()


def load_yaml_from_url(url: str):
    # Retrieve the file content from the URL
    response = requests.get(url, allow_redirects=True)
    # Convert bytes to string
    content = response.content.decode("utf-8")
    try:
        # Load the yaml
        return yaml.load(content, SafeLineLoader)
    except (ScannerError, yaml.YAMLError) as e:
        logging.error(e, exc_info=True)
        error_message = traceback.format_exc().split('.yaml')[-1].replace(',', '').replace('"', '')
        # Return an error response if there is an issue with the YAML configuration
        raise Exception(f"Could not create WBR metrics due to incorrect yaml, caused due to error in {error_message}")
