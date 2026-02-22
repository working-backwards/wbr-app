# SPDX-License-Identifier: Apache-2.0
from datetime import datetime, timedelta, date
from itertools import groupby

import fiscalyear
import numpy as np
import pandas as pd
from dateutil import relativedelta

from src.constants import (
    BPS_MULTIPLIER,
    BOX_IDX_LAST_WK,
    BOX_IDX_MTD,
    BOX_IDX_QTD,
    BOX_IDX_WOW,
    BOX_IDX_YOY_MTD,
    BOX_IDX_YOY_QTD,
    BOX_IDX_YOY_WK,
    BOX_IDX_YOY_YTD,
    BOX_IDX_YTD,
    MONTHLY_DATA_START_INDEX,
    NUM_BOX_TOTAL_ROWS,
    NUM_TRAILING_WEEKS,
    PCT_MULTIPLIER,
    PY_WEEKLY_OFFSET_DAYS,
    YOY_IDX_CY_MTD,
    YOY_IDX_CY_QTD,
    YOY_IDX_CY_WK5,
    YOY_IDX_CY_WK6,
    YOY_IDX_CY_YTD,
    YOY_IDX_PY_MTD,
    YOY_IDX_PY_QTD,
    YOY_IDX_PY_WK6,
    YOY_IDX_PY_YTD,
)
import src.wbr_utility as wbr_util

_BOX_POSITIONS = (
    (BOX_IDX_WOW,     YOY_IDX_CY_WK6, YOY_IDX_CY_WK5),
    (BOX_IDX_YOY_WK,  YOY_IDX_CY_WK6, YOY_IDX_PY_WK6),
    (BOX_IDX_YOY_MTD, YOY_IDX_CY_MTD, YOY_IDX_PY_MTD),
    (BOX_IDX_YOY_QTD, YOY_IDX_CY_QTD, YOY_IDX_PY_QTD),
    (BOX_IDX_YOY_YTD, YOY_IDX_CY_YTD, YOY_IDX_PY_YTD),
)


def build_agg(item):
    if 'function' in item[1]:
        return None
    if item[1]['aggf'] == 'sum':
        return item[0], lambda x: x.sum(skipna=False)
    else:
        return item[0], item[1]["aggf"]


def bps_or_pct_change_collector(entry):
    metric_config = entry[1]
    return "metric_comparison_method" in metric_config and metric_config["metric_comparison_method"] == "bps"


def function_exists_collector(entry):
    return "function" if "function" in entry[1] else "non_function"


def get_bps_and_pct_change_metrics(metrics_configs):
    # holds key as true or false and value as the list of (metric, metric config)
    bps_and_pct_change_metric_map = {k: list(v) for k, v in groupby(sorted(metrics_configs.items(),
                                                                           key=bps_or_pct_change_collector),
                                                                    key=bps_or_pct_change_collector)}

    # Further grouping by
    bps_metric_map = {k: list(v) for k, v in groupby(sorted(bps_and_pct_change_metric_map[True],
                                                            key=function_exists_collector),
                                                     key=function_exists_collector)} \
        if True in bps_and_pct_change_metric_map else {}

    pct_change_metric_map = {k: list(v) for k, v in groupby(sorted(bps_and_pct_change_metric_map[False],
                                                                   key=function_exists_collector),
                                                            key=function_exists_collector)} \
        if False in bps_and_pct_change_metric_map else {}

    # Extract lists of metrics
    fn_bps_metrics = [entry[0] for entry in bps_metric_map["function"]] if "function" in bps_metric_map else []
    bps_metrics = [entry[0] for entry in bps_metric_map["non_function"]] if "non_function" in bps_metric_map else []
    fn_pct_change_metrics = [entry[0] for entry in pct_change_metric_map["function"]] \
        if "function" in pct_change_metric_map else []
    pct_change_metrics = [entry[0] for entry in pct_change_metric_map["non_function"]] \
        if "non_function" in pct_change_metric_map else []
    return fn_bps_metrics, bps_metrics, fn_pct_change_metrics, pct_change_metrics


def get_function_metrics_configs(metrics_configs: dict):
    return dict(filter(lambda item: "function" in item[1], metrics_configs.items()))


class WBR:
    """
        Represents the WBR (Weekly Business Review) class.

        Attributes:
            daily_df (pandas.DataFrame): The daily data frame.
            cfg (dict): The configuration dictionary.
            cy_week_ending (datetime.datetime): The week ending date for the current year.
            week_number (int): The week number.
            fiscal_month (str): The fiscal year end month.
            metrics_configs (dict): The metrics configuration dictionary.
            metric_aggregation (dict): The metric aggregation dictionary.
            daily_metrics (pandas.DataFrame): The daily data frame with columns for all configured metrics.
            cy_trailing_six_weeks (pandas.DataFrame): The trailing six weeks data frame for the current year.
            py_trailing_six_weeks (pandas.DataFrame): The trailing six weeks data frame for the previous year.
            cy_monthly (pandas.DataFrame): The monthly data frame for the current year.
            py_monthly (pandas.DataFrame): The monthly data frame for the previous year.
            bps_metrics (list): The list of metrics for basis point comparison.
            function_bps_metrics (list): The list of metrics with function for basis point comparison.
            pct_change_metrics (list): The list of metrics for percent-change comparison.
            fn_pct_change_metrics (list): The list of metrics with function for percent-change comparison.
            graph_axis_label (str): The graph axis label.
        """
    def __init__(self, cfg, daily_df=None, csv=None):
        if daily_df is None:
            # This case should ideally not happen if WBRValidator always provides daily_df.
            # If it can, we need a strategy: error, or expect 'csv' path in cfg for fallback.
            # For now, let's assume daily_df is always provided.
            raise ValueError("WBR class initialized without daily_df. This should be provided by WBRValidator.")
        self.daily_df = daily_df
        self.cfg = cfg
        self.cy_week_ending = datetime.strptime(self.cfg['setup']['week_ending'], '%d-%b-%Y')
        self.week_number = self.cfg['setup']['week_number']
        self.fiscal_month = self.cfg['setup']['fiscal_year_end_month'] if 'fiscal_year_end_month' in self.cfg['setup']\
            else "DEC"
        self.metrics_configs = self.cfg['metrics']

        self.metrics_configs.__delitem__("__line__")

        self.metric_aggregation = dict(filter(None, list(map(build_agg, self.metrics_configs.items()))))
        self.daily_metrics = wbr_util.create_dynamic_data_frame(self.daily_df, self.metrics_configs)

        self.cy_trailing_six_weeks = wbr_util.create_trailing_six_weeks(
            self.daily_metrics,
            self.cy_week_ending,
            self.metric_aggregation
        )

        self.py_trailing_six_weeks = wbr_util.create_trailing_six_weeks(
            self.daily_metrics,
            self.cy_week_ending - timedelta(days=PY_WEEKLY_OFFSET_DAYS),
            self.metric_aggregation
        ).add_prefix('PY__')

        self.cy_monthly = wbr_util.create_trailing_twelve_months(
            self.daily_metrics,
            self.cy_week_ending,
            self.metric_aggregation
        )

        self.py_monthly = wbr_util.create_trailing_twelve_months(
            self.daily_metrics,
            self.cy_week_ending - relativedelta.relativedelta(years=1),
            self.metric_aggregation
        ).add_prefix('PY__')

        self.function_bps_metrics, self.bps_metrics, self.fn_pct_change_metrics, self.pct_change_metrics =\
            get_bps_and_pct_change_metrics(self.metrics_configs)

        self.box_totals, self.py_box_total, self.period_summary = self.calculate_box_totals()
        self.compute_extra_months()
        self.compute_functional_metrics()
        self.graph_axis_label = wbr_util.create_axis_label(self.cy_week_ending, self.week_number,
                                                           len(self.cy_monthly['Date']))
        self.metrics = self.create_wbr_metrics()
        # init end

    def create_wbr_metrics(self):
        """
        We are going to create 4 dataframes
            1. cy_wbr_graph_data_with_weekly
            2. py_wbr_graph_data_with_weekly
        Then we will create the final dataframe by interlacing the two merged dataframes
        so that the metric and PY_metric are in adjacent columns
        :return: metric dataframe
        """

        # Create #1 -cy_wbr_graph_data_with_weekly
        cy_wbr_graph_data_with_weekly = pd.DataFrame()
        cy_wbr_graph_data_with_weekly = pd.concat(
            [cy_wbr_graph_data_with_weekly, self.cy_trailing_six_weeks], ignore_index=True
        )
        cy_wbr_graph_data_with_weekly = wbr_util.create_new_row(None, cy_wbr_graph_data_with_weekly)
        cy_wbr_graph_data_with_weekly.reset_index(drop=True, inplace=True)
        cy_wbr_graph_data_with_weekly = pd.concat(
            [cy_wbr_graph_data_with_weekly, self.cy_monthly], ignore_index=True
        )

        # Create #2 -py_wbr_graph_data_with_weekly
        py_wbr_graph_data_with_weekly = pd.DataFrame()
        py_wbr_graph_data_with_weekly = pd.concat(
            [py_wbr_graph_data_with_weekly, self.py_trailing_six_weeks], ignore_index=True
        )
        py_wbr_graph_data_with_weekly = wbr_util.create_new_row(None, py_wbr_graph_data_with_weekly)
        py_wbr_graph_data_with_weekly.reset_index(drop=True, inplace=True)
        py_wbr_graph_data_with_weekly = pd.concat(
            [py_wbr_graph_data_with_weekly, self.py_monthly], ignore_index=True
        )

        # Now we interlace cy_wbr_graph_data_with_weekly with py_wbr_graph_data_with_weekly
        # so the columns that will appear on a WBR chart are near each other.
        # The date column from py_wbr_graph_data_with_weekly will be ignored.
        metrics = pd.concat([cy_wbr_graph_data_with_weekly, py_wbr_graph_data_with_weekly], axis=1)
        # metrics = wbr_util.interlace_df(cy_wbr_graph_data_with_weekly, py_wbr_graph_data_with_weekly)

        axis_series = pd.Series(self.graph_axis_label, name='Axis')

        columns = [metrics[col] for col in metrics.columns]
        insert_position = 1
        columns.insert(insert_position, axis_series)
        # Add the axis label to the metrics dataframe
        metrics = pd.concat(columns, axis=1)
        # metrics.insert(1, 'Axis', pd.Series(self.graph_axis_label))

        # append wow, mom, yoy values for all the metrics provided in the yaml
        metrics = self.append_yoy_values(cy_wbr_graph_data_with_weekly, py_wbr_graph_data_with_weekly, metrics)
        metrics = self.append_wow_values(metrics)
        metrics = self.append_mom_values(metrics)

        self.cy_trailing_six_weeks.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.py_trailing_six_weeks.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.cy_monthly.replace([np.inf, -np.inf], np.nan, inplace=True)
        self.py_monthly.replace([np.inf, -np.inf], np.nan, inplace=True)

        self.box_totals.replace([np.inf, -np.inf], "N/A", inplace=True)
        self.py_box_total.replace([np.inf, -np.inf], "N/A", inplace=True)

        self.box_totals = self.box_totals.fillna("N/A")
        self.py_box_total = self.py_box_total.fillna("N/A")

        metrics.replace([np.inf, -np.inf], np.nan, inplace=True)

        return metrics

    def append_wow_values(self, metric_df):
        """
        Appends Week-over-Week (WOW) values to the given metric DataFrame.

        This method calculates the trailing six-week metrics for the current and previous week,
        processes the metrics according to their configurations, and appends the resulting WOW values
        to the provided metric DataFrame. It also updates the box totals.

        Args:
            metric_df (pd.DataFrame): The DataFrame containing the current metrics to which WOW values will be appended.

        Returns:
            pd.DataFrame: The updated metric DataFrame with WOW values appended.
        """
        # Calculate the current trailing six weeks metrics
        current_trailing_six_weeks = wbr_util.create_trailing_six_weeks(
            self.daily_metrics, self.cy_week_ending, self.metric_aggregation
        )

        # Calculate the previous week's trailing six weeks metrics
        previous_week_trailing_data = wbr_util.create_trailing_six_weeks(
            self.daily_metrics, self.cy_week_ending - timedelta(7), self.metric_aggregation
        )

        # Process each metric based on its configuration
        for metric, metric_configs in self.metrics_configs.items():
            # If the metric has a function and is not in the current trailing data, handle it accordingly
            if 'function' in metric_configs and metric not in current_trailing_six_weeks:
                wbr_util.handle_function_metrics_for_extra_attribute(
                    metric, metric_configs['function'], current_trailing_six_weeks, previous_week_trailing_data
                )

        # Drop the 'Date' column from both DataFrames for further processing
        current_trailing_six_weeks = current_trailing_six_weeks.drop(columns='Date')
        previous_week_trailing_data = previous_week_trailing_data.drop(columns='Date')

        # Calculate the Month-over-Month (MoM), WOW, Year-over-Year (YoY), basis points, or percent values
        operated_data_frame = self.calculate_mom_wow_yoy_bps_or_percent_values(
            current_trailing_six_weeks, previous_week_trailing_data, False
        )

        # Rename columns to indicate WOW values
        operated_data_frame = operated_data_frame.rename(
            columns={col: col + 'WOW' for col in operated_data_frame.columns})

        rows_to_add = len(metric_df) - len(operated_data_frame)

        # Append None values to align the index with metric_df
        nan_rows = pd.DataFrame(np.nan, index=range(rows_to_add), columns=operated_data_frame.columns)
        operated_data_frame = pd.concat([operated_data_frame, nan_rows], ignore_index=True)

        # Concatenate the operated data frame with the original metric DataFrame
        metric_df = pd.concat([metric_df, operated_data_frame.reset_index(drop=True)], axis=1)

        # Create a DataFrame with 'N/A' values for all columns
        box_totals_wow_df = pd.DataFrame([['N/A'] * len(operated_data_frame.columns)],
                                         columns=operated_data_frame.columns)

        # Repeat the row to match box totals structure
        box_totals_wow_df = box_totals_wow_df.loc[
            box_totals_wow_df.index.repeat(NUM_BOX_TOTAL_ROWS)
        ].reset_index(drop=True)

        # Concatenate the new DataFrame with the existing one
        self.box_totals = pd.concat([self.box_totals, box_totals_wow_df], axis=1)

        return metric_df

    def append_mom_values(self, metric_df):
        """
        Appends Month-over-Month (MoM) values to the given metric DataFrame.

        This method calculates the trailing twelve-month metrics for the current date and the previous month,
        processes the metrics according to their configurations, and appends the resulting MoM values
        to the provided metric DataFrame. It also updates the box totals.

        Args:
            metric_df (pd.DataFrame): The DataFrame containing the current metrics to which MoM values will be appended.

        Returns:
            pd.DataFrame: The updated metric DataFrame with MoM values appended.
        """
        # Define the current date and the date for the previous month
        current_date = self.cy_week_ending
        previous_month_date = (current_date + relativedelta.relativedelta(months=-1))

        # Calculate the current and previous trailing twelve months metrics
        current_trailing_twelve_months = wbr_util.create_trailing_twelve_months(
            self.daily_metrics, current_date, self.metric_aggregation
        )

        previous_trailing_twelve_months = wbr_util.create_trailing_twelve_months(
            self.daily_metrics, previous_month_date, self.metric_aggregation
        )

        # Process each metric based on its configuration
        for metric, metric_configs in self.metrics_configs.items():
            # If the metric has a function and is not in the current trailing data, handle it accordingly
            if 'function' in metric_configs and metric not in current_trailing_twelve_months:
                wbr_util.handle_function_metrics_for_extra_attribute(
                    metric,
                    metric_configs['function'],
                    current_trailing_twelve_months,
                    previous_trailing_twelve_months
                )

        # Drop the 'Date' column for further processing
        current_trailing_six_weeks = current_trailing_twelve_months.drop(columns='Date')
        previous_week_trailing_data = previous_trailing_twelve_months.drop(columns='Date')

        # Calculate the MoM values
        operated_data_frame = self.calculate_mom_wow_yoy_bps_or_percent_values(
            current_trailing_six_weeks, previous_week_trailing_data, False
        )

        # Rename columns to indicate MoM values
        operated_data_frame = operated_data_frame.rename(
            columns={col: col + 'MOM' for col in operated_data_frame.columns})

        # Append None values to align the index with metric_df
        nan_rows = pd.DataFrame(np.nan, index=range(MONTHLY_DATA_START_INDEX), columns=operated_data_frame.columns)
        operated_data_frame = pd.concat([nan_rows, operated_data_frame], ignore_index=True)

        # Concatenate the operated data frame with the original metric DataFrame
        metric_df = pd.concat([metric_df, operated_data_frame.reset_index(drop=True)], axis=1)

        # Create a DataFrame with 'N/A' values for all columns
        box_total_mom_df = pd.DataFrame([['N/A'] * len(operated_data_frame.columns)],
                                        columns=operated_data_frame.columns)

        # Repeat the row to match box totals structure
        box_total_mom_df = box_total_mom_df.loc[
            box_total_mom_df.index.repeat(NUM_BOX_TOTAL_ROWS)
        ].reset_index(drop=True)

        # Concatenate the new DataFrame with the existing one
        self.box_totals = pd.concat([self.box_totals, box_total_mom_df], axis=1)

        return metric_df

    def calculate_mom_wow_yoy_bps_or_percent_values(self, current_trailing_six_weeks, previous_week_trailing_data,
                                                    do_multiply):
        """
        Calculates Month-over-Month (MoM), Week-over-Week (WoW), Year-over-Year (YoY),
        Basis Points (bps), or Percent values based on the provided data.

        The method compares current trailing six-week metrics with the previous week's data
        and computes differences, ratios, or percentages as specified by the provided metric categories.

        Args:
            current_trailing_six_weeks (pd.DataFrame): DataFrame containing the current trailing six weeks metrics.
            previous_week_trailing_data (pd.DataFrame): DataFrame containing the previous week's metrics for comparison.
            do_multiply (bool): If True, multiplies the results by 10,000 for bps metrics or by 100 for percentage
            metrics.

        Returns:
            pd.DataFrame: DataFrame containing the calculated metrics.
        """
        operated_data_frame = pd.DataFrame()  # Initialize an empty DataFrame for results

        # Calculate differences for basis points metrics
        if len(self.bps_metrics) > 0:
            operated_data_frame = pd.concat(
                [
                    operated_data_frame,
                    (current_trailing_six_weeks[self.bps_metrics]
                     .subtract(previous_week_trailing_data[self.bps_metrics]))
                ],
                axis=1
            )
            # Multiply by BPS_MULTIPLIER if required for basis points
            if do_multiply:
                operated_data_frame = operated_data_frame.mul(BPS_MULTIPLIER)

        # Calculate differences for function basis points metrics
        if len(self.function_bps_metrics) > 0:
            operated_data_frame = pd.concat(
                [
                    operated_data_frame,
                    (current_trailing_six_weeks[self.function_bps_metrics]
                     .subtract(previous_week_trailing_data[self.function_bps_metrics]))
                ],
                axis=1
            )
            # Multiply by BPS_MULTIPLIER if required for basis points
            if do_multiply:
                operated_data_frame = operated_data_frame.mul(BPS_MULTIPLIER)

        # Calculate percentage changes for percentile metrics
        if len(self.pct_change_metrics) > 0:
            operated_data_frame = pd.concat(
                [
                    operated_data_frame,
                    (current_trailing_six_weeks[self.pct_change_metrics]
                     .div(previous_week_trailing_data[self.pct_change_metrics]) - 1)
                ],
                axis=1
            )
            # Multiply by PCT_MULTIPLIER if required for percentage
            if do_multiply:
                operated_data_frame = operated_data_frame.mul(PCT_MULTIPLIER)

        # Calculate percentage changes for function percentile metrics
        if len(self.fn_pct_change_metrics) > 0:
            operated_data_frame = pd.concat(
                [
                    operated_data_frame,
                    (current_trailing_six_weeks[self.fn_pct_change_metrics]
                     .div(previous_week_trailing_data[self.fn_pct_change_metrics]) - 1)
                ],
                axis=1
            )
            # Multiply by PCT_MULTIPLIER if required for percentage
            if do_multiply:
                operated_data_frame = operated_data_frame.mul(PCT_MULTIPLIER)

        return operated_data_frame  # Return the DataFrame with calculated values

    def append_yoy_values(self, cy_weekly_and_monthly_data, py_weekly_and_monthly_data, metric_df):
        """
        Appends Year-over-Year (YoY) values to the metric DataFrame by comparing
        current year (CY) and previous year (PY) weekly and monthly data.

        This method calculates YoY differences based on the provided current and previous year data,
        appends the resulting metrics to the existing DataFrame, and updates the box totals.

        Args:
            cy_weekly_and_monthly_data (pd.DataFrame): Current year weekly and monthly data.
            py_weekly_and_monthly_data (pd.DataFrame): Previous year weekly and monthly data.
            metric_df (pd.DataFrame): DataFrame to which YoY values will be appended.

        Returns:
            pd.DataFrame: Updated metric DataFrame with appended YoY values.
        """
        # Drop the 'Date' column from the current year data
        cy_weekly_and_monthly_data = cy_weekly_and_monthly_data.drop(columns='Date')

        # Drop the 'PY__Date' column from the previous year data and clean up column names
        py_weekly_and_monthly_data = py_weekly_and_monthly_data.drop(columns='PY__Date')
        py_weekly_and_monthly_data.columns = py_weekly_and_monthly_data.columns.str.replace('PY__', '')

        # Calculate YoY differences
        operated_data_frame = self.calculate_mom_wow_yoy_bps_or_percent_values(cy_weekly_and_monthly_data,
                                                                               py_weekly_and_monthly_data, False)

        # Extract week 6 and week 5 data for further calculations
        week_6_df = pd.DataFrame(operated_data_frame.iloc[NUM_TRAILING_WEEKS - 1]).T.reset_index(drop=True)
        week_5_df = pd.DataFrame(operated_data_frame.iloc[NUM_TRAILING_WEEKS - 2]).T.reset_index(drop=True)

        # Calculate WoW for the extracted weeks
        wow_dataframe = self.calculate_mom_wow_yoy_bps_or_percent_values(week_6_df, week_5_df, True)

        # Rename columns for the YoY DataFrame
        operated_data_frame = operated_data_frame.rename(
            columns={col: col + 'YOY' for col in operated_data_frame.columns})

        # Append the YoY data to the metric DataFrame
        metric_df = pd.concat([metric_df, operated_data_frame], axis=1)

        # Calculate YoY values for box totals
        box_data_frame = self.calculate_mom_wow_yoy_bps_or_percent_values(
            self.box_totals.drop(columns=['Date', 'Axis']),
            self.py_box_total.drop(columns=['Date', 'Axis']),
            False
        )

        # Fill missing values and update with WoW values
        for j in range(len(box_data_frame.columns)):
            column_name = box_data_frame.columns[j]
            box_data_frame.loc[BOX_IDX_WOW, column_name] = wow_dataframe.loc[0, column_name]

        # Fill missing values in absolute-value rows (not comparison rows)
        box_data_frame.loc[BOX_IDX_LAST_WK] = box_data_frame.loc[BOX_IDX_LAST_WK].fillna(0)
        box_data_frame.loc[BOX_IDX_MTD] = box_data_frame.loc[BOX_IDX_MTD].fillna(0)
        box_data_frame.loc[BOX_IDX_QTD] = box_data_frame.loc[BOX_IDX_QTD].fillna(0)
        box_data_frame.loc[BOX_IDX_YTD] = box_data_frame.loc[BOX_IDX_YTD].fillna(0)

        # Rename columns for the box totals DataFrame
        box_data_frame = box_data_frame.rename(columns={col: col + 'YOY' for col in box_data_frame.columns})

        # Append the updated box totals DataFrame to the existing box totals
        self.box_totals = pd.concat([self.box_totals, box_data_frame.fillna('N/A')], axis=1)

        return metric_df  # Return the updated metric DataFrame

    def compute_functional_metrics(self):
        """
        Evaluates all function metrics using iterative DFS with cycle detection.

        Metrics are evaluated in dependency order: if metric A depends on metric B,
        B is evaluated first. A circular dependency raises ValueError.
        """
        function_metrics = get_function_metrics_configs(self.metrics_configs)
        computed = set()
        pending = set()

        for metric_name, config in function_metrics.items():
            if metric_name in computed:
                continue

            stack = [(metric_name, config["function"], False)]

            while stack:
                name, func_config, deps_resolved = stack.pop()

                if name in computed:
                    continue

                if deps_resolved:
                    self._evaluate_function_metric(name, func_config)
                    computed.add(name)
                    pending.discard(name)
                    continue

                if name in pending:
                    raise ValueError(
                        f"Circular dependency detected for metric '{name}' "
                        f"at line {func_config.get('__line__', '?')} in yaml"
                    )

                pending.add(name)
                stack.append((name, func_config, True))

                # Push uncomputed function-metric dependencies (reversed to preserve order)
                operation = list(func_config.keys())[0]
                operands = list(func_config.values())[0]
                deps = [
                    (op['metric']['name'], op['metric']['function'])
                    for op in operands
                    if 'metric' in op and 'function' in op['metric']
                ]
                for dep_name, dep_config in reversed(deps):
                    if dep_name not in computed:
                        stack.append((dep_name, dep_config, False))

    def _evaluate_function_metric(self, metric_name, func_config):
        """Compute a single function metric, assuming all its dependencies are already computed."""
        operation = list(func_config.keys())[0]

        grouped = {
            key: list(group) for key, group in
            groupby(list(func_config.values())[0],
                    key=lambda x: 'metric' if 'metric' in x else 'column')
        }

        column_list = [cfg['metric']['name'] for cfg in grouped.get("metric", [])]
        column_list.extend(
            cfg["column"]["name"] for cfg in grouped.get("column", [])
        )

        py_column_list = ['PY__' + name for name in column_list]

        try:
            self._apply_function_to_all_series(column_list, py_column_list, metric_name, operation)
        except KeyError as e:
            raise KeyError(
                f"Unknown metric found at line: {func_config['__line__']} in yaml. "
                f"Please check if you have defined this in metric section {e}"
            )

    def _apply_function_to_all_series(self, column_list, py_column_list, metric_name, operation):
        def apply_op(df):
            if operation == 'sum':
                return df.iloc[:].sum(axis=1)
            op_method = {'product': 'mul', 'difference': 'sub', 'divide': 'div'}[operation]
            return getattr(df.iloc[:, 0], op_method)(df.iloc[:, 1])

        self.cy_trailing_six_weeks[metric_name] = apply_op(self.cy_trailing_six_weeks[column_list])
        self.py_trailing_six_weeks['PY__' + metric_name] = apply_op(self.py_trailing_six_weeks[py_column_list])
        self.cy_monthly[metric_name] = apply_op(self.cy_monthly[column_list])
        self.py_monthly['PY__' + metric_name] = apply_op(self.py_monthly[py_column_list])

        box_totals = apply_op(self.box_totals[column_list])
        self._compute_box_total_yoy(metric_name, column_list, box_totals, operation)
        self.box_totals[metric_name] = box_totals
        self.py_box_total[metric_name] = apply_op(self.py_box_total[column_list])

    def _compute_box_total_yoy(self, metric_name, columns, box_totals, operation):
        if operation in ('divide', 'product'):
            if operation == 'divide':
                self.period_summary[metric_name] = (
                    self.period_summary[columns[0]] / self.period_summary[columns[1]]
                )
            else:
                self.period_summary[metric_name] = (
                    self.period_summary[columns[0]] * self.period_summary[columns[1]]
                )

            yoy = self.period_summary

            for box_idx, cy_idx, py_idx in _BOX_POSITIONS:
                if operation == 'divide':
                    cy_val = yoy[columns[0]][cy_idx] / yoy[columns[1]][cy_idx]
                    py_val = yoy[columns[0]][py_idx] / yoy[columns[1]][py_idx]
                else:
                    cy_val = yoy[columns[0]][cy_idx] * yoy[columns[1]][cy_idx]
                    py_val = yoy[columns[0]][py_idx] * yoy[columns[1]][py_idx]
                box_totals[box_idx] = self.calculate_yoy_box_total(cy_val, py_val, metric_name)
        else:
            if operation == 'sum':
                self.period_summary[metric_name] = self.period_summary.iloc[:].sum(axis=1)
            else:
                self.period_summary[metric_name] = (
                    self.period_summary[columns[0]] - self.period_summary[columns[1]]
                )

            yoy_field_values = pd.DataFrame()
            yoy_field_values = pd.concat([yoy_field_values, self.period_summary], axis=1)
            yoy_field_values = yoy_field_values.replace(np.nan, 0)

            value_list = wbr_util.apply_operation_and_return_denominator_values(
                operation, columns, yoy_field_values
            )

            for i, (box_idx, cy_idx, _py_idx) in enumerate(_BOX_POSITIONS):
                if operation == 'sum':
                    cy_value = wbr_util.apply_sum_operations(yoy_field_values, columns, cy_idx)
                else:
                    cy_value = (yoy_field_values[columns[0]][cy_idx]
                                - yoy_field_values[columns[1]][cy_idx])
                box_totals[box_idx] = self.calculate_yoy_box_total(
                    cy_value, value_list[i], metric_name
                )

    def calculate_yoy_box_total(self, operand_1, operand_2, metric_name):
        return (operand_1 - operand_2) * BPS_MULTIPLIER if metric_name in self.function_bps_metrics else (
                ((operand_1/operand_2) - 1) * PCT_MULTIPLIER)

    def compute_extra_months(self):
        if not wbr_util.is_last_day_of_month(self.cy_week_ending):
            self.aggregate_week_ending_month()
        if self.fiscal_month.lower() != self.cy_week_ending.strftime("%b").lower():
            self.aggregate_months_to_fiscal_year_end()

    def aggregate_months_to_fiscal_year_end(self):
        """
        Aggregates monthly data to fiscal year-end based on the provided fiscal month.

        This method resamples the data to a monthly frequency and performs aggregation
        using the specified metrics. It calculates the fiscal year-end dates and
        corresponding dates for the previous year. The resulting monthly aggregates
        are filtered for the current and previous fiscal years, and concatenated
        to existing trailing twelve-month data.

        Returns:
            None: The method updates the instance variables directly.
        """
        # Resample data to monthly frequency and perform aggregation
        monthly_data = (
            self.daily_metrics.resample('ME', label='right', closed='right', on='Date')
            .agg(self.metric_aggregation, skipna=False)  # Aggregate using predefined metrics
            .reset_index()
            .sort_values(by='Date')
        )

        # Set up fiscal year and calculate relevant dates
        fiscal_end_month = datetime.strptime(self.fiscal_month, "%b")  # Convert fiscal month to datetime
        fiscalyear.setup_fiscal_calendar(start_month=(fiscal_end_month.month + 1) % 12)  # Setup fiscal calendar
        fy = fiscalyear.FiscalYear(self.get_start_year())  # Get the fiscal year object
        month_next_to_last_week = self.cy_week_ending.month + 1  # Determine the next month after the current week
        first_day_of_month = date(self.cy_week_ending.year, month_next_to_last_week, 1).strftime(
            "%d-%b-%Y")  # First day of the next month
        last_day_of_fiscal_year = fy.end.strftime("%d-%b-%Y")  # Last day of the fiscal year

        # Calculate previous year's corresponding dates
        py_first_day_of_month = (datetime.strptime(first_day_of_month, "%d-%b-%Y")
                                 - relativedelta.relativedelta(years=1))  # Previous year's first day of month
        py_last_of_fiscal_year = (datetime.strptime(last_day_of_fiscal_year, "%d-%b-%Y")
                                  - relativedelta.relativedelta(years=1))  # Previous year's last day of fiscal year

        # Filter data for the future and previous year's months
        future_month_aggregate_data = (
            monthly_data
            .query('Date >= @first_day_of_month and Date <= @last_day_of_fiscal_year')  # Filter for current year
            .reset_index(drop=True)
            .sort_values(by="Date")
            .replace(0, np.nan)  # Replace 0 values with NaN
        )
        py_future_month_aggregate_data = (
            monthly_data
            .query('Date >= @py_first_day_of_month and Date <= @py_last_of_fiscal_year')  # Filter for previous year
            .reset_index(drop=True)
            .sort_values(by="Date")
            .replace(0, np.nan)  # Replace 0 values with NaN
            .add_prefix('PY__')  # Prefix columns for previous year
        )

        # Concatenate current year and previous year data to trailing twelve months
        self.cy_monthly = pd.concat(
            [self.cy_monthly, future_month_aggregate_data]
        ).reset_index(drop=True)

        self.py_monthly = pd.concat(
            [self.py_monthly, py_future_month_aggregate_data]
        ).reset_index(drop=True)

    def aggregate_week_ending_month(self):
        """
        Aggregates daily data into monthly metrics based on the current week ending date.

        This method computes the first and last day of the month corresponding to
        the current week ending date. It then filters daily data within that month,
        aggregates the metrics according to the specified aggregation methods,
        and appends the results to the trailing twelve months data for both
        current and previous years.

        Returns:
            None: The method updates the instance variables directly.
        """
        # Get the first day of the current month
        first_day_of_month = date(
            self.cy_week_ending.year, self.cy_week_ending.month, 1
        ).strftime("%d-%b-%Y")

        # Get the last day of the current month
        last_day_of_month = date(
            self.cy_week_ending.year + self.cy_week_ending.month // 12,
            self.cy_week_ending.month % 12 + 1, 1
        ) - timedelta(1)

        # Filter daily data for the current month
        month_daily_data = self.daily_metrics.query(
            'Date >= @first_day_of_month and Date <= @last_day_of_month'
        ).reset_index(drop=True).sort_values(by="Date")

        # Initialize a DataFrame to hold the aggregated results
        agg_series = pd.DataFrame({"Date": [last_day_of_month.strftime("%Y-%m-%d %H:%M:%S")]})

        # Perform aggregation for each metric
        for metric in month_daily_data:
            if metric == 'Date':
                continue  # Skip the Date column

            # Check if the count of non-null values matches
            if month_daily_data['Date'].count() != month_daily_data[metric].count():
                agg_series[metric] = np.nan  # Assign NaN if counts do not match
            elif self.metric_aggregation[metric] == 'last':
                # Get the last value for the metric
                agg_series[metric] = month_daily_data.tail(1)[metric].reset_index(drop=True).get(0)
            elif self.metric_aggregation[metric] == 'first':
                # Get the first value for the metric
                agg_series[metric] = month_daily_data.head(1)[metric].reset_index(drop=True).get(0)
            else:
                # Aggregate using the specified method
                agg_result = month_daily_data[metric].agg(self.metric_aggregation[metric])
                agg_series = pd.concat([agg_series, pd.DataFrame.from_dict(
                    {metric: [agg_result]}
                )], axis=1)

        # Append the aggregated results to the current year trailing twelve months data
        self.cy_monthly = pd.concat([self.cy_monthly, agg_series]).reset_index(drop=True)

        # Calculate previous year's corresponding dates
        py_first_day_of_month = (
                datetime.strptime(first_day_of_month, "%d-%b-%Y") -
                relativedelta.relativedelta(years=1)
        )

        py_last_day_of_month = last_day_of_month - relativedelta.relativedelta(years=1)

        # Filter daily data for the previous year
        py_month_agg_data = self.daily_metrics.query(
            'Date >= @py_first_day_of_month and Date <= @py_last_day_of_month'
        ).reset_index(drop=True).sort_values(by="Date").resample(
            'ME', label='right', closed='right', on='Date'
        ).agg(self.metric_aggregation, skipna=False).reset_index().sort_values(by='Date').add_prefix('PY__')

        # Append the previous year's aggregated data to the trailing twelve months
        self.py_monthly = pd.concat(
            [self.py_monthly, py_month_agg_data]
        ).reset_index(drop=True)

    def calculate_box_totals(self):
        """
        calculate_box_totals takes a dataframe containing daily data and a date.
        It creates a data frame with the columns of the original data frame.
        The date column is the period end date for either the current period
        or the comp period (prior week or prior year).

        It will contain 9 rows with following axis labels for WBR box total.

        LastWk, WOW, YOY, MTD, YOY, QTD, YOY, YTD, YOY

        The box_totals member will be appended at the end of the trailing 6-week
        and 12-month WBR frame or can be returned as a standalone data frame.

        Method to calculate various totals and metrics for different time periods
        :return: box_total, py_box_total and period_summary dataframe
        """
        # Initialize empty DataFrames for box totals and year-over-year (YoY) box totals
        box_totals = pd.DataFrame()
        py_box_totals = pd.DataFrame()

        # Extract specific rows from current year (cy) and previous year (py) trailing six weeks dataframes
        cy_wk6, cy_wk5, py_wk6, py_wk5 = (
            self.cy_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 1]],
            self.cy_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 2]],
            self.py_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 1]],
            self.py_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 2]],
        )

        # Remove 'PY__' prefix from column names for py_wk6 and py_wk5
        py_wk6.columns, py_wk5.columns = py_wk6.columns.str.replace('PY__', ''), py_wk5.columns.str.replace('PY__', '')

        # Reset the indices to 0 for the selected rows
        dataframe_list = [cy_wk6, cy_wk5, py_wk6, py_wk5]
        [x.reset_index(drop=True, inplace=True) for x in dataframe_list]

        # Extract common dates for year-over-year comparison
        cy_last_day = pd.to_datetime(self.cy_week_ending)
        py_last_day = pd.to_datetime(cy_last_day) - relativedelta.relativedelta(years=1)

        # Calculate start dates for MTD, QTD, and YTD
        cy_first_day_mtd = cy_last_day.replace(day=1)
        py_first_day_mtd = py_last_day.replace(day=1)

        try:
            cy_first_day_qtd = cy_last_day.to_period('Q-' + self.fiscal_month).to_timestamp()
            py_first_day_qtd = py_last_day.to_period('Q-' + self.fiscal_month).to_timestamp()
            cy_first_day_ytd = cy_last_day.to_period('Y-' + self.fiscal_month).to_timestamp()
            py_first_day_ytd = py_last_day.to_period('Y-' + self.fiscal_month).to_timestamp()
        except ValueError:
            raise ValueError(f"fiscal_year_end_month' value is in incorrect format from setup section "
                             f"at line: {self.cfg['setup']['__line__']}")

        # Loop through different time periods (MTD, QTD, YTD)
        for period, period_range in [
            ('MTD', [('cy_first_day_mtd', 'cy_last_day'), ('py_first_day_mtd', 'py_last_day')]),
            ('QTD', [('cy_first_day_qtd', 'cy_last_day'), ('py_first_day_qtd', 'py_last_day')]),
            ('YTD', [('cy_first_day_ytd', 'cy_last_day'), ('py_first_day_ytd', 'py_last_day')])
        ]:
            # Filter data for the specified period
            cy_data = self.daily_metrics.query(f'Date >= @{period_range[0][0]} and Date <= @{period_range[0][1]}')
            py_data = self.daily_metrics.query(f'Date >= @{period_range[1][0]} and Date <= @{period_range[1][1]}')

            # Resample data annually based on fiscal month and calculate aggregated metric
            cy_total = cy_data.resample('YE-' + self.fiscal_month, label='right', closed='right', on='Date').agg(
                self.metric_aggregation).reset_index().sort_values(by='Date')
            py_total = py_data.resample('YE-' + self.fiscal_month, label='right', closed='right', on='Date').agg(
                self.metric_aggregation).reset_index().sort_values(by='Date')

            # If the resulting dataframe is empty, create a new row
            if cy_total.empty:
                cy_total = wbr_util.create_new_row(None, cy_total)

            if py_total.empty:
                py_total = wbr_util.create_new_row(None, py_total)

            # Add the calculated totals to the list of dataframes
            dataframe_list.extend([cy_total, py_total])

        # Remove the 'Date' column and replace 0 values with NaN
        for i in range(len(dataframe_list)):
            dataframe_list[i] = dataframe_list[i].drop(columns='Date').replace([0], np.nan)

        period_summary = pd.concat(dataframe_list, ignore_index=True)

        # Initialize variables for week-over-week (WOW) and YoY calculations for business points (bps) and percentiles
        cy_wk6_wow = pd.DataFrame()
        cy_wk6_yoy = pd.DataFrame()
        cy_mtd_yoy = pd.DataFrame()
        cy_qtd_yoy = pd.DataFrame()
        cy_ytd_yoy = pd.DataFrame()

        # Separate dataframes for bps and percentiles
        list_bps_df = []
        list_pct_change_df = []

        # Extract bps and percentiles data for different time periods
        for df in dataframe_list:
            if len(self.bps_metrics) > 0:
                bps_metric_df = df[self.bps_metrics]
                list_bps_df.append(bps_metric_df)
            if len(self.pct_change_metrics) > 0:
                pct_change_metric_df = df[self.pct_change_metrics]
                list_pct_change_df.append(pct_change_metric_df)

        # Calculate WOW and YoY for bps
        if len(list_bps_df) > 0:
            cy_wk6_wow = pd.concat([cy_wk6_wow, pd.DataFrame(
                list_bps_df[YOY_IDX_CY_WK6].subtract(list_bps_df[YOY_IDX_CY_WK5])).mul(BPS_MULTIPLIER)], axis=1)
            cy_wk6_yoy = pd.concat([cy_wk6_yoy, pd.DataFrame(
                list_bps_df[YOY_IDX_CY_WK6].subtract(list_bps_df[YOY_IDX_PY_WK6])).mul(BPS_MULTIPLIER)], axis=1)
            cy_mtd_yoy = pd.concat([cy_mtd_yoy, pd.DataFrame(
                list_bps_df[YOY_IDX_CY_MTD].subtract(list_bps_df[YOY_IDX_PY_MTD])).mul(BPS_MULTIPLIER)], axis=1)
            cy_qtd_yoy = pd.concat([cy_qtd_yoy, pd.DataFrame(
                list_bps_df[YOY_IDX_CY_QTD].subtract(list_bps_df[YOY_IDX_PY_QTD])).mul(BPS_MULTIPLIER)], axis=1)
            cy_ytd_yoy = pd.concat([cy_ytd_yoy, pd.DataFrame(
                list_bps_df[YOY_IDX_CY_YTD].subtract(list_bps_df[YOY_IDX_PY_YTD])).mul(BPS_MULTIPLIER)], axis=1)

        # Calculate WOW and YoY for percentiles
        if len(list_pct_change_df) > 0:
            cy_wk6_wow = pd.concat([cy_wk6_wow, pd.DataFrame(
                list_pct_change_df[YOY_IDX_CY_WK6].div(list_pct_change_df[YOY_IDX_CY_WK5]) - 1
            ).mul(PCT_MULTIPLIER)], axis=1)
            cy_wk6_yoy = pd.concat([cy_wk6_yoy, pd.DataFrame(
                list_pct_change_df[YOY_IDX_CY_WK6].div(list_pct_change_df[YOY_IDX_PY_WK6]) - 1
            ).mul(PCT_MULTIPLIER)], axis=1)
            cy_mtd_yoy = pd.concat([cy_mtd_yoy, pd.DataFrame(
                list_pct_change_df[YOY_IDX_CY_MTD].div(list_pct_change_df[YOY_IDX_PY_MTD]) - 1
            ).mul(PCT_MULTIPLIER)], axis=1)
            cy_qtd_yoy = pd.concat([cy_qtd_yoy, pd.DataFrame(
                list_pct_change_df[YOY_IDX_CY_QTD].div(list_pct_change_df[YOY_IDX_PY_QTD]) - 1
            ).mul(PCT_MULTIPLIER)], axis=1)
            cy_ytd_yoy = pd.concat([cy_ytd_yoy, pd.DataFrame(
                list_pct_change_df[YOY_IDX_CY_YTD].div(list_pct_change_df[YOY_IDX_PY_YTD]) - 1
            ).mul(PCT_MULTIPLIER)], axis=1)

        # Combine calculated metrics into box totals dataframe
        box_totals_df = [box_totals,
                         dataframe_list[YOY_IDX_CY_WK6], cy_wk6_wow, cy_wk6_yoy,
                         dataframe_list[YOY_IDX_CY_MTD], cy_mtd_yoy,
                         dataframe_list[YOY_IDX_CY_QTD], cy_qtd_yoy,
                         dataframe_list[YOY_IDX_CY_YTD], cy_ytd_yoy]

        # Concatenate the dataframes in the box_totals_df list
        box_totals = pd.concat([x.fillna(0) for x in box_totals_df], axis=0)

        # Reset index of the resulting dataframe
        box_totals = box_totals.reset_index(drop=True)

        # Extract py data for py_box_totals
        py_box_totals = pd.concat([py_box_totals, dataframe_list[YOY_IDX_PY_WK6]])

        # Add null columns for py_box_totals
        py_box_totals = pd.concat(
            [py_box_totals.T, pd.Series(None, dtype='float64')], ignore_index=True, axis=1
        ).T
        py_box_totals = pd.concat(
            [py_box_totals.T, pd.Series(None, dtype='float64')], ignore_index=True, axis=1
        ).T
        py_box_totals = pd.concat([py_box_totals, dataframe_list[YOY_IDX_PY_MTD]])
        py_box_totals = pd.concat(
            [py_box_totals.T, pd.Series(None, dtype='float64')], ignore_index=True, axis=1
        ).T
        py_box_totals = pd.concat((py_box_totals, dataframe_list[YOY_IDX_PY_QTD]), axis=0)
        py_box_totals = pd.concat(
            [py_box_totals.T, pd.Series(None, dtype='float64')], ignore_index=True, axis=1
        ).T
        py_box_totals = pd.concat((py_box_totals, dataframe_list[YOY_IDX_PY_YTD]), axis=0)
        py_box_totals = pd.concat(
            [py_box_totals.T, pd.Series(None, dtype='float64')], ignore_index=True, axis=1
        ).T

        # Create axis labels and dates to associate with box totals
        col_list = [cy_last_day, cy_last_day - timedelta(days=7), py_last_day, cy_last_day, py_last_day,
                    cy_last_day, py_last_day, cy_last_day, py_last_day]
        box_totals.insert(0, 'Date', pd.Series(col_list), allow_duplicates=True)
        py_box_totals.insert(0, 'Date', pd.Series(col_list), allow_duplicates=True)
        summary_labels = ['LastWk', 'WOW', 'YOY', 'MTD', 'YOY', 'QTD', 'YOY', 'YTD', 'YOY']

        # Add axis labels to box_totals dataframe
        box_totals.insert(1, 'Axis', pd.Series(summary_labels), allow_duplicates=True)

        # Add axis labels to py_box_totals dataframe
        py_box_totals.insert(1, 'Axis', pd.Series(summary_labels), allow_duplicates=True)

        # Set the calculated box_totals and py_box_totals to class attributes
        return box_totals, py_box_totals, period_summary

    def get_start_year(self):
        if self.fiscal_month == 'DEC':
            return self.cy_week_ending.year + 1
        else:
            week_ending_month = self.cy_week_ending.month
            for i in range(week_ending_month, 13):
                if i == datetime.strptime(self.fiscal_month, "%b").month:
                    return self.cy_week_ending.year
                if self.cy_week_ending.year + i // 12 > self.cy_week_ending.year:
                    return self.cy_week_ending.year + 1

    def __str__(self):
        return (f'Current YearTrailing 6 Weeks: \n {self.cy_trailing_six_weeks} \n'
                f'Prior Year Trailing 6 Weeks: \n {self.py_trailing_six_weeks} \n'
                f'Current Year Trailing 12 months \n {self.cy_monthly} \n'
                f'Prior Year Trailing 12 months \n {self.py_monthly} \n'
                f'x-axis \n {self.graph_axis_label} \n Box Totals \n {self.graph_axis_label} \n metrics {self.metrics}')
