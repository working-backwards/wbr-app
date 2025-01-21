import calendar
import datetime
from typing import Any, Callable

import dateutil
import numpy as np
import pandas as pd
from dateutil import relativedelta


def append_to_list(data: Any, to_append_list: list):
    to_append_list.append(data)


def put_into_map(data: Any, dictionary: dict, key: Any):
    dictionary[key] = data


def if_else(
        data: Any,
        predicate: Callable[[Any], bool],
        true_consumer: Callable[[Any], None],
        fallback: Callable[[Any], None]
) -> None:
    true_consumer(data) if predicate(data) else fallback(data)


def if_else_supplier(
        data: Any,
        predicate: Callable[[Any], bool],
        true_consumer: Callable[[Any], Any],
        fallback: Callable[[Any], Any]
):
    return true_consumer(data) if predicate(data) else fallback(data)


def apply_operation_and_return_denominator_values(operation, columns, yoy_required_values_df):
    """
    Applies specified operations to the columns of a DataFrame and returns a list of calculated values.

    This function computes either the sum or difference of specified columns from the
    provided DataFrame (`yoy_required_values_df`). It handles specific row indices to
    return values for further calculations, replacing any zero values with NaN for
    better handling of missing or invalid data.

    Args:
        operation (str): The operation to apply. It should be either 'sum' or 'difference'.
        columns (list): A list of column names on which the operation will be performed.
        yoy_required_values_df (pd.DataFrame): The DataFrame containing the values for the operation.

    Returns:
        list: A list of calculated values resulting from the specified operation.
    """
    value_list = []

    if operation == 'sum':
        # Compute sum for specified indices and append to value_list
        value_list.append(apply_sum_operations(yoy_required_values_df, columns, 1))
        value_list.append(apply_sum_operations(yoy_required_values_df, columns, 2))
        value_list.append(apply_sum_operations(yoy_required_values_df, columns, 5))
        value_list.append(apply_sum_operations(yoy_required_values_df, columns, 7))
        value_list.append(apply_sum_operations(yoy_required_values_df, columns, 9))

    elif operation == 'difference':
        # Calculate the difference between the specified columns for specific indices
        value_list.append(yoy_required_values_df[columns[0]][1] - yoy_required_values_df[columns[1]][1])
        value_list.append(yoy_required_values_df[columns[0]][2] - yoy_required_values_df[columns[1]][2])
        value_list.append(yoy_required_values_df[columns[0]][5] - yoy_required_values_df[columns[1]][5])
        value_list.append(yoy_required_values_df[columns[0]][7] - yoy_required_values_df[columns[1]][7])
        value_list.append(yoy_required_values_df[columns[0]][9] - yoy_required_values_df[columns[1]][9])

    # Replace zero values with NaN for proper handling of invalid data
    for i in range(len(value_list)):
        if value_list[i] == 0:
            value_list[i] = np.nan

    return value_list


def apply_sum_operations(yoy_required_values_df, columns, index):
    sum_total = 0
    for column in columns:
        sum_total += yoy_required_values_df[column][index]

    return sum_total


def create_empty_df(df):
    """
    Create an empty DataFrame with the same columns as the input DataFrame.

    This function returns a new DataFrame that has the same structure (i.e.,
    column names and data types) as the provided DataFrame but contains no rows.

    Args:
        df (pd.DataFrame): The DataFrame from which to copy the structure.

    Returns:
        pd.DataFrame: An empty DataFrame with the same columns as the input DataFrame.
    """
    # Create a copy of the input DataFrame's structure without any rows
    copy = df.iloc[:0, :].copy(deep=True)

    return copy


def interlace_df(df1, df2):
    """
    Interlaces two DataFrames, inserting columns from the second DataFrame
    into the first DataFrame while ignoring the 'Date' column.

    This function assumes that the first column of the second DataFrame is labeled 'Date'
    and is not included in the interlacing. The resulting DataFrame can be useful
    for visualizing metrics with a secondary axis, allowing both weekly and monthly
    data to be displayed on the same graph.

    Args:
        df1 (pd.DataFrame): The first DataFrame to which columns will be added.
        df2 (pd.DataFrame): The second DataFrame from which columns will be interlaced
                            (ignores the first column).

    Returns:
        pd.DataFrame: A new DataFrame with interlaced columns from df2 into df1.
    """
    # Create deep copies of the input DataFrames to avoid modifying the originals
    left_df = df1.copy(deep=True)
    right_df = df2.copy(deep=True)

    # Initialize the index for inserting columns into the left DataFrame
    i = 0
    for column in right_df:
        # Skip the first column (assumed to be 'Date') by starting the insert at index 1
        if i > 0:
            left_df.insert(i, column, right_df[column])
        # Increment the index by 2 to interlace columns
        i += 2

    return left_df


def create_new_row(d, df):
    """
    Append a new row to the provided DataFrame with a specified date.

    This function creates a single-row DataFrame with the specified date and
    NaN values for all other columns. The new row will be added to the
    original DataFrame, which is then returned.

    Args:
        d:
        df (pd.DataFrame): The DataFrame to which the new row will be appended.

    Returns:
        pd.DataFrame: The DataFrame with the new row appended.

    Raises:
        ValueError: If the DataFrame does not have 'Date' or 'PY__Date' as the first column.
    """
    # Create a new DataFrame for the new row with the appropriate column name
    if df.columns[0] == 'Date':
        date_df = pd.DataFrame(columns=['Date'])
    else:
        date_df = pd.DataFrame(columns=['PY__Date'])

    # Add the specified date to the new DataFrame
    date_df.loc[0] = [d]

    # Exclude empty or all NaN rows (if applicable)
    date_df = exclude_empty_or_all_na(date_df)

    # Concatenate the original DataFrame with the new row DataFrame
    df = pd.concat([df, date_df])

    return df


def exclude_empty_or_all_na(df):
    return df.dropna(axis=1, how='all')


def create_trailing_six_weeks(df, week_ending, aggf):
    """
    Create a DataFrame summarized by week for the past 6 weeks.

    This function takes a DataFrame containing daily metrics data and a week ending datetime,
    returning a DataFrame with the weekly summary for the past 6 weeks. If there is insufficient
    daily data, the function pads the DataFrame with empty values to ensure it returns exactly 6 rows.

    Args:
        df (pd.DataFrame): The DataFrame containing daily metrics data with a 'Date' column.
        week_ending (timedelta): The datetime object representing the end of the week to summarize.
        aggf (dict): A dictionary defining aggregation functions for the metrics.

    Returns:
        pd.DataFrame: A DataFrame summarized by week for the past 6 weeks, padded with empty values if necessary.

    Raises:
        ValueError: If the 'Date' column is missing in the DataFrame.
    """
    # Check if the 'Date' column exists
    if 'Date' not in df.columns:
        raise ValueError("DataFrame must contain a 'Date' column.")

    # Dictionary to map weekdays to weekly resampling
    week_number_and_week_day = {1: 'W-Mon', 2: 'W-Tue', 3: 'W-Wed', 4: 'W-Thu', 5: 'W-Fri', 6: 'W-Sat', 7: 'W-Sun'}

    # Calculate the date six weeks ago
    six_weeks_ago = week_ending - datetime.timedelta(days=41)

    # Get daily data for the trailing 6 weeks
    trailing_six_weeks_daily = df.query('Date >= @six_weeks_ago and Date <= @week_ending')

    # Resample current year trailing six weeks daily data to weekly data
    trailing_six_weeks_weekly = (
        trailing_six_weeks_daily
        .resample(week_number_and_week_day[week_ending.isoweekday()], label='right', closed='right', on='Date')
        .agg(aggf)
        .reset_index()
        .sort_values(by='Date')
    )

    # Determine the earliest week date for padding
    if trailing_six_weeks_weekly.empty:
        earliest_week = week_ending + datetime.timedelta(days=7)
    else:
        earliest_week = trailing_six_weeks_weekly.loc[0].Date

    # Pad the DataFrame to ensure it has exactly 6 rows
    for _ in range(1, (6 - len(trailing_six_weeks_weekly) + 1)):
        earliest_week -= datetime.timedelta(days=7)
        trailing_six_weeks_weekly = create_new_row(earliest_week, trailing_six_weeks_weekly)

    # Sort by date and reset index
    trailing_six_weeks_weekly.sort_values(by=['Date'], inplace=True)
    trailing_six_weeks_weekly.reset_index(drop=True, inplace=True)

    return trailing_six_weeks_weekly


def is_last_day_of_month(d):
    """
    Check if the given date is the last day of its month.

    Args:
        d (datetime.date): The date to check.

    Returns:
        bool: True if the date is the last day of the month, False otherwise.
    """
    # Get the number of days in the month for the given date
    _, days_in_month = calendar.monthrange(d.year, d.month)

    # Return True if the day of the date is equal to the last day of the month
    return d.day == days_in_month


def create_trailing_twelve_months(df, week_ending, aggf):
    """
    Create a DataFrame summarizing monthly metrics for the past 12 months.

    Args:
        df (pd.DataFrame): DataFrame containing daily metrics data with a 'Date' column.
        week_ending (datetime.datetime): The week ending date to determine the last month to include.
        aggf (dict): Dictionary of aggregation functions to apply to each column.

    Returns:
        pd.DataFrame: DataFrame containing monthly metrics for the last 12 months, padded with NaN if necessary.
    """

    # Resample daily data to monthly data, with dates as the last day of the month
    monthly_data = (
        df.resample('ME', label='right', closed='right', on='Date')
        .agg(aggf)
        .reset_index()
        .sort_values(by='Date')
    )

    # Determine the last full month based on the week ending date
    if is_last_day_of_month(week_ending):
        # it's the last day of the month, so the last full month is current month
        end_date = week_ending
    else:
        # last full month is in the prior month
        end_date = week_ending.replace(day=1) - datetime.timedelta(days=1)

    # Define the beginning date for the trailing twelve months
    begin_date = end_date - dateutil.relativedelta.relativedelta(months=11)

    # Filter monthly data for the last twelve months
    trailing_twelve_months_monthly = (
        monthly_data.query('Date >= @begin_date and Date <= @end_date')
        .reset_index(drop=True)
        .sort_values(by="Date")
    )

    # Padding monthly if there is no data provided for those periods in the source file.
    # Note: this will not check of the week ending data is set into the future.
    if trailing_twelve_months_monthly.empty:
        # There was no data for this period so create 12 rows
        earliest_month = end_date
    else:
        earliest_month = trailing_twelve_months_monthly.loc[0].Date

    # Pad with empty rows if there are less than 12 months of data
    for i in range(1, (12 - len(trailing_twelve_months_monthly) + 1)):
        earliest_month = earliest_month.replace(day=1) - datetime.timedelta(days=1)
        trailing_twelve_months_monthly = create_new_row(earliest_month, trailing_twelve_months_monthly)

    # Resort by date and reindex
    trailing_twelve_months_monthly.sort_values(by=['Date'], inplace=True)
    trailing_twelve_months_monthly.reset_index(drop=True, inplace=True)

    return trailing_twelve_months_monthly


def create_axis_label(week_ending, week_number, number_of_months):
    """
    Create x-axis labels for a chart that includes week numbers and month abbreviations.

    The label sequence consists of the last six weeks, followed by an empty value,
    the last twelve months, followed by another empty value, and box totals.

    Args:
        week_ending (datetime.datetime): The date that marks the end of the week.
        week_number (int): The current week number of the year (1-52).
        number_of_months (int): The number of months to include in the label.

    Returns:
        list: A list of labels for the x-axis.
    """
    axis_label = []

    # Append last six-week labels
    for i in range(6, 0, -1):
        axis_label.append("wk " + str((week_number - i) % 52 + 1))

    # Append an empty space to separate weeks and months on the chart
    axis_label.append(" ")

    # Determine the last full month based on the week ending date
    if is_last_day_of_month(week_ending):
        # it's the last day of the month, so the last full month is current month
        last_full_month = week_ending
    else:
        # last full month is in the prior month
        last_full_month = week_ending.replace(day=1) - datetime.timedelta(days=1)

    # Calculate the first full month in the trailing twelve months
    first_full_month = last_full_month - dateutil.relativedelta.relativedelta(months=11)

    # Append month abbreviations for the trailing twelve months
    for i in range(number_of_months):
        month_date = first_full_month + dateutil.relativedelta.relativedelta(months=i)
        axis_label.append(calendar.month_abbr[month_date.month])

    return axis_label


def handle_function_metrics_for_extra_attribute(metric_name, metric_config, current_trailing_df, previous_trailing_df):
    """
    Perform calculations on specified metrics in the current and previous trailing dataframes.

    This function extracts metric configurations and performs specified operations (divide, sum,
    difference, product) on the corresponding columns in the provided dataframes.

    Args:
        metric_name (str): The name of the new metric to create.
        metric_config (dict): Configuration specifying the operation and metrics to be used.
        current_trailing_df (pd.DataFrame): DataFrame containing the current trailing metrics.
        previous_trailing_df (pd.DataFrame): DataFrame containing the previous trailing metrics.

    Returns:
        None: The function modifies the input DataFrames in place.
    """
    column_list = []
    operation = list(metric_config.keys())[0]
    metric_defs = list(metric_config.values())[0]

    # Extract column names from metric configurations
    for column_config in metric_defs:
        if 'metric' in column_config:
            if 'function' in column_config['metric']:
                handle_function_metrics_for_extra_attribute(
                    column_config['metric']['function'],
                    column_config['metric']['name'],
                    current_trailing_df,
                    previous_trailing_df
                )
                column_list.append(column_config['metric']['name'])
            else:
                column_list.append(column_config['metric']['name'])
        elif 'column' in column_config:
            column_list.append(column_config['column']['name'])

    # Select the necessary columns from both DataFrames
    current_trailing_data = current_trailing_df[column_list]
    previous_trailing_data = previous_trailing_df[column_list]

    # Define a mapping of operations to corresponding pandas functions
    operation_map = {
        'divide': current_trailing_data.iloc[:, 0].div,
        'sum': current_trailing_data.iloc[:, 0].add,
        'difference': current_trailing_data.iloc[:, 0].sub,
        'product': current_trailing_data.iloc[:, 0].mul
    }

    # Perform the operation if it's valid
    if operation in operation_map:
        current_trailing_df[metric_name] = operation_map[operation](current_trailing_data.iloc[:, 1])
        previous_trailing_df[metric_name] = operation_map[operation](previous_trailing_data.iloc[:, 1])
    else:
        raise ValueError(f"Unsupported operation: {operation}")


def create_data_subset_for_aggregation(daily_df, aggregation_dicts, base_metric):
    """
    Create a subset of the daily DataFrame based on aggregation criteria.

    This function filters and aggregates the daily DataFrame according to the specified
    aggregation criteria. If a query is provided, it filters the DataFrame accordingly;
    otherwise, it aggregates the specified base metric by date.

    Args:
        daily_df (pd.DataFrame): The daily DataFrame containing metrics.
        aggregation_dicts (dict): A dictionary containing aggregation parameters,
                                   including an optional query.
        base_metric (str): The name of the metric column to aggregate.

    Returns:
        pd.Series: A series containing the aggregated data for the specified metric.

    Raises:
        KeyError: If the specified base_metric is not found in the DataFrame.
        KeyError: If an invalid query is provided that references an unknown column.
    """

    aggregated_data_frame = pd.DataFrame(daily_df.iloc[:, 1:])

    # Extract the date series directly
    date_series = daily_df['Date']

    if "query" not in aggregation_dicts:
        # Aggregate the specified base metric by date
        aggregated_data_frame = pd.concat([date_series, aggregated_data_frame[base_metric]], axis=1)
        aggregated_data_frame = aggregated_data_frame.groupby('Date', as_index=False).aggregate('sum')
    else:
        query = aggregation_dicts['query']
        try:
            aggregated_data_frame = aggregated_data_frame.query(query)
        except pd.errors.UndefinedVariableError as e:
            raise KeyError(
                f"Invalid query provided: {query}. Unknown column found in the query for filter metric {base_metric} "
                f"at yaml line {aggregation_dicts.get('__line__', 'unknown')}, error: {e}"
            )

        # Re-aggregate the filtered DataFrame by date
        aggregated_data_frame = pd.concat([date_series, aggregated_data_frame], axis=1)

    # Check if base_metric exists in the aggregated data
    if base_metric not in aggregated_data_frame:
        raise KeyError(
            f"Column '{base_metric}' not found in the aggregated dataset while creating the filtered metric, "
            f"yaml line: {aggregation_dicts.get('__line__', 'unknown')}")

    return aggregated_data_frame[base_metric]


def aggregate_and_append_series_to_main_data_frame(daily_df, metrics_name, metric_config):
    """
    Aggregate a base metric from the daily DataFrame and append it as a new series.

    This function aggregates a specified metric from the daily DataFrame based on the
    provided configuration and appends it to the original DataFrame as a new column.

    Args:
        daily_df (pd.DataFrame): The daily DataFrame containing metrics.
        metrics_name (str): The name of the new column to be added to the DataFrame.
        metric_config (dict): A dictionary containing the configuration for aggregation,
                              including 'base_column' to specify the metric to aggregate.

    Returns:
        pd.Series: A series containing the aggregated values for the specified metric.

    Raises:
        KeyError: If the base column specified in metric_config is not found in daily_df.
    """

    base_metric = metric_config['base_column']
    series_list = create_data_subset_for_aggregation(daily_df, metric_config, base_metric).to_list()
    daily_df = pd.concat([daily_df, pd.DataFrame.from_dict({metrics_name: series_list})], axis=1)
    return daily_df[metrics_name]


def create_dynamic_data_frame(daily_df, metrics_config):
    """
    Generate a dynamic DataFrame for a given set of metrics.

    This function creates a DataFrame based on the provided configuration,
    aggregating metrics as necessary.

    Args:
        daily_df (pd.DataFrame): The DataFrame containing daily metrics.
        metrics_config (dict): Configuration dictionary defining metrics.

    Returns:
        pd.DataFrame: A DataFrame containing the specified metrics.

    Raises:
        KeyError: If a specified column or metric is not found.
        Exception: For aggregation-related errors.
    """

    main_dataframe = pd.DataFrame()
    main_dataframe = pd.concat([main_dataframe, daily_df['Date']], axis=1)  # Initialize the main DataFrame with 'Date'
    main_dataframe = main_dataframe.drop_duplicates()  # Remove duplicate dates
    main_dataframe = main_dataframe.reset_index(drop=True)  # Reset index after dropping duplicates

    for metrics_name, metric_config in metrics_config.items():
        if metrics_name == '__line__':
            continue  # Skip the line indicator

        aggregator_dataframe = pd.DataFrame()  # Create a temporary DataFrame for aggregation
        if 'column' in metric_config:
            aggregator_dataframe['Date'] = daily_df['Date']  # Initialize with the 'Date' column
            try:
                aggregator_dataframe[metrics_name] = daily_df[metric_config['column']]  # Assign specified column
            except KeyError:
                raise KeyError(f"Column {metrics_name} not found in the dataset while calculating the metric, yaml line"
                               f": {metric_config['__line__']}")

            # Aggregate based on the specified aggregation function
            if metric_config['aggf'] == 'sum':
                aggregator_dataframe = aggregator_dataframe.groupby('Date', as_index=False).aggregate(
                    metric_config['aggf'], min_count=1)
            else:
                try:
                    aggregator_dataframe = aggregator_dataframe.groupby('Date', as_index=False).aggregate(
                        metric_config['aggf'])
                except Exception as exp_err:
                    raise Exception(exp_err.__str__().replace("for 'DataFrameGroupBy' object", ' for ')
                                    + metrics_name + " metric")
            main_dataframe = pd.concat([main_dataframe, aggregator_dataframe[metrics_name]],
                                       axis=1)  # Concatenate results to main DataFrame

        elif 'metric' in metric_config:
            main_dataframe[metrics_name] = daily_df[metric_config['column']]  # Assign metric column directly
            main_dataframe = main_dataframe.groupby('Date', as_index=False).aggregate('sum')  # Aggregate by date

        elif 'filter' in metric_config:
            aggregator_dataframe['Date'] = daily_df['Date']  # Initialize with 'Date'
            # Aggregate and append the series based on filtering criteria
            aggregator_dataframe = pd.concat([
                aggregator_dataframe,
                aggregate_and_append_series_to_main_data_frame(daily_df, metrics_name, metric_config['filter'])
            ], axis=1)
            if metric_config['aggf'] == 'sum':
                aggregator_dataframe = aggregator_dataframe.groupby('Date', as_index=False).aggregate(
                    metric_config['aggf'], min_count=1
                )
            else:
                aggregator_dataframe = aggregator_dataframe.groupby('Date', as_index=False).aggregate(
                    metric_config['aggf']
                )
            main_dataframe = pd.concat([main_dataframe, aggregator_dataframe[metrics_name]],
                                       axis=1)  # Concatenate results

        elif 'function' in metric_config:
            pass  # Placeholder for handling functional metrics

        else:
            raise KeyError(
                f"Could not create metric {metrics_name} as no column/metric/aggregation/function is specified"
            )

    return main_dataframe  # Return the final aggregated DataFrame
