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
    """Build an aggregation entry for one metric from its YAML config.

    Returns None for function metrics (they're computed later, not aggregated).
    For sum aggregation, uses skipna=False so that a week with missing days produces
    NaN rather than a misleading partial sum — the WBR would show incomplete data
    as if it were a real total.
    """
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
    """Builds all data needed for an Amazon-style Weekly Business Review (WBR) deck.

    A WBR is a standardized weekly report used to track business metrics over time.
    Each metric is shown as a 6-12 chart: the last 6 weeks of data plus up to 12
    months of trailing monthly data, with both current year (CY) and prior year (PY)
    values for comparison. Below each chart is a "box total" summary with 9 rows:
    LastWk, WOW (week-over-week), YOY (year-over-year for the week), MTD, YOY,
    QTD, YOY, YTD, YOY.

    Metrics fall into two comparison categories configured in the YAML:
    - **bps (basis points):** Rate metrics (e.g., conversion rate) compared by
      subtraction: (CY - PY) * 10,000. A 0.5% improvement = 50 bps.
    - **pct_change (percent change):** Volume metrics (e.g., page views) compared
      by division: ((CY / PY) - 1) * 100. Doubling = +100%.

    Some metrics are "function metrics" — derived from other metrics via arithmetic
    (sum, difference, product, divide). These are defined in the YAML with a
    `function:` block and evaluated after base metrics are computed.

    Pipeline data-flow through __init__:

        cfg, daily_df                         <- inputs
          |
          +-- cfg ----------> cy_week_ending, week_number, fiscal_month, metrics_configs
          +-- metrics_configs --> metric_aggregation      (via build_agg)
          +-- daily_df + metrics_configs --> daily_metrics     (via create_dynamic_data_frame)
          |
          +-- daily_metrics + cy_week_ending + metric_aggregation
          |     --> cy_trailing_six_weeks     (CY last 6 weeks, aggregated per metric)
          |     --> py_trailing_six_weeks     (PY last 6 weeks, 364 days back)
          |     --> cy_monthly               (CY trailing 12 months)
          |     --> py_monthly               (PY trailing 12 months)
          |
          +-- metrics_configs --> bps_metrics, function_bps_metrics,
          |                       pct_change_metrics, fn_pct_change_metrics
          |
          +-- cy/py_trailing_six_weeks + daily_metrics + cy_week_ending
          |     + fiscal_month + metric_aggregation
          |     --> box_totals, py_box_total, period_summary
          |                                        (via calculate_box_totals)
          |
          +-- MUTATES cy_monthly, py_monthly       (via compute_extra_months)
          |     Appends partial-month and forecast months through fiscal year end.
          |
          +-- MUTATES cy/py_trailing_six_weeks, cy/py_monthly,
          |     box_totals, py_box_total, period_summary
          |                                        (via compute_functional_metrics)
          |     Evaluates derived metrics (sum, difference, product, divide) across
          |     all DataFrames in dependency order.
          |
          +-- cy_week_ending + week_number + len(cy_monthly) --> graph_axis_label
          |
          +-- ALL OF THE ABOVE --> metrics          (via create_wbr_metrics)
                Assembles the final interlaced CY/PY DataFrame with WOW, MOM, YOY
                columns. Also MUTATES 6 DataFrames for display cleanup (inf->NaN,
                NaN->"N/A").
    """

    def __init__(self, cfg, daily_df=None, csv=None):
        if daily_df is None:
            raise ValueError("WBR class initialized without daily_df. This should be provided by WBRValidator.")
        self.daily_df = daily_df
        self.cfg = cfg

        # --- Extract setup parameters from YAML config ---
        self.cy_week_ending = datetime.strptime(self.cfg['setup']['week_ending'], '%d-%b-%Y')
        self.week_number = self.cfg['setup']['week_number']
        self.fiscal_month = self.cfg['setup']['fiscal_year_end_month'] if 'fiscal_year_end_month' in self.cfg['setup']\
            else "DEC"
        self.metrics_configs = self.cfg['metrics']
        self.metrics_configs.__delitem__("__line__")

        # Build the aggregation map: metric_name -> aggregation function.
        # Function metrics (derived via sum/difference/etc.) get None here and are
        # computed later by compute_functional_metrics.
        self.metric_aggregation = dict(filter(None, list(map(build_agg, self.metrics_configs.items()))))

        # --- Build base DataFrames for all non-function metrics ---

        # daily_metrics: the raw daily data filtered to only the columns defined in YAML
        self.daily_metrics = wbr_util.create_dynamic_data_frame(self.daily_df, self.metrics_configs)

        # Trailing 6 weeks: weekly aggregates for the chart's left half (the "6" in 6-12)
        self.cy_trailing_six_weeks = wbr_util.create_trailing_six_weeks(
            self.daily_metrics,
            self.cy_week_ending,
            self.metric_aggregation
        )
        # PY offset is 364 days (52 weeks exactly) — NOT 365 — so that weekdays align
        # for an apples-to-apples weekly comparison (e.g., Mon-Sun vs Mon-Sun).
        self.py_trailing_six_weeks = wbr_util.create_trailing_six_weeks(
            self.daily_metrics,
            self.cy_week_ending - timedelta(days=PY_WEEKLY_OFFSET_DAYS),
            self.metric_aggregation
        ).add_prefix('PY__')

        # Trailing 12 months: monthly aggregates for the chart's right half (the "12" in 6-12)
        self.cy_monthly = wbr_util.create_trailing_twelve_months(
            self.daily_metrics,
            self.cy_week_ending,
            self.metric_aggregation
        )
        # PY monthly uses a calendar-year offset (relativedelta) rather than 364 days,
        # because monthly periods align by calendar month, not by weekday.
        self.py_monthly = wbr_util.create_trailing_twelve_months(
            self.daily_metrics,
            self.cy_week_ending - relativedelta.relativedelta(years=1),
            self.metric_aggregation
        ).add_prefix('PY__')

        # --- Classify metrics by comparison method ---
        # Each metric is compared either by basis points (bps) or percent change.
        # Function metrics (derived from other metrics) are tracked separately because
        # their YOY box-total calculation must use the derived values, not raw inputs.
        self.function_bps_metrics, self.bps_metrics, self.fn_pct_change_metrics, self.pct_change_metrics =\
            get_bps_and_pct_change_metrics(self.metrics_configs)

        # --- Box totals: the 9-row summary below each chart ---
        # See constants.py BOX_IDX_* for the row layout.
        self.box_totals, self.py_box_total, self.period_summary = self.calculate_box_totals()

        # Append months beyond the trailing 12 through fiscal year end.
        # The CY partial month is aggregated manually (per-metric) with a count-mismatch
        # guard: if any day's data is missing, the whole month becomes NaN rather than
        # showing a misleading partial sum. PY uses resample since it has complete data.
        self.compute_extra_months()

        # Evaluate derived (function) metrics across all DataFrames.
        # Must run after base metrics and box totals are built, since function metrics
        # reference base metrics as operands.
        self.compute_functional_metrics()

        self.graph_axis_label = wbr_util.create_axis_label(self.cy_week_ending, self.week_number,
                                                           len(self.cy_monthly['Date']))

        # Assemble the final interlaced CY/PY metric DataFrame with WOW, MOM, YOY
        # comparison columns. Also cleans up inf/NaN values for display.
        self.metrics = self.create_wbr_metrics()

    def create_wbr_metrics(self):
        """Assemble the final WBR metrics DataFrame for the deck builder.

        Combines weekly (6 rows) and monthly (12+ rows) data for both CY and PY
        into a single interlaced DataFrame where each metric's CY and PY columns
        are adjacent (e.g., PageViews, PY__PageViews). Appends WOW, MOM, and YOY
        comparison columns, then cleans up inf/NaN values for display.

        Mutates (for display cleanup):
            self.cy/py_trailing_six_weeks, self.cy/py_monthly — inf -> NaN
            self.box_totals, self.py_box_total — inf -> "N/A", NaN -> "N/A"
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
        """Compare CY vs PY data using each metric's configured comparison method.

        This is the core comparison engine used by WOW, MOM, and YOY calculations.
        Each metric is compared using one of two methods (configured in YAML):

        - **bps metrics:** CY - PY (subtraction). Used for rate metrics like conversion
          rate where the meaningful comparison is the difference in rates.
          When do_multiply=True, scaled by 10,000 to express as basis points.
        - **pct_change metrics:** (CY / PY) - 1 (percent change). Used for volume
          metrics like page views where the meaningful comparison is relative growth.
          When do_multiply=True, scaled by 100 to express as a percentage.

        Args:
            current_trailing_six_weeks: CY period data (despite the name, can be any period).
            previous_week_trailing_data: PY/comparison period data.
            do_multiply: If True, apply BPS_MULTIPLIER (10,000) or PCT_MULTIPLIER (100).
                Used for box-total comparisons; False for raw chart-level comparisons.
        """
        operated_data_frame = pd.DataFrame()

        # bps metrics: subtraction (CY - PY). Rate metrics are compared by absolute
        # difference because a 2% conversion rate vs 1.5% is "50 bps better", not "33% better".
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

        # pct_change metrics: division ((CY / PY) - 1). Volume metrics are compared by
        # relative change because "20M vs 10M page views" is meaningful as "+100%".
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
        """Apply a function metric's operation across all 6 DataFrames.

        A function metric derives its value from other metrics using one of four
        operations defined in the YAML: sum (N-ary), difference, product, or divide
        (all binary). This method computes the result for each time series (CY/PY
        weekly, CY/PY monthly, box_totals, py_box_total) and also triggers the
        box-total YOY computation via _compute_box_total_yoy.

        Mutates: self.cy/py_trailing_six_weeks, self.cy/py_monthly,
                 self.box_totals, self.py_box_total, self.period_summary
        """
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
        """Compute YOY box-total rows for a function metric.

        Two paths based on operation type:
        - divide/product: Compute the derived metric directly from period_summary
          values (e.g., revenue / units = price), then compare CY vs PY.
        - sum/difference: Replace NaN with 0 (partial periods should sum as zero,
          not propagate NaN), then use wbr_util helpers for aggregation.

        Mutates: self.period_summary (adds the derived metric column),
                 box_totals (fills in the 5 YOY comparison rows)
        """
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

            # Copy period_summary and replace NaN with 0 for sum/difference operations.
            # Partial periods (e.g., incomplete QTD) have NaN for missing months;
            # when summing, those should contribute 0 rather than making the total NaN.
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
        """Compute a single YOY comparison value for a function metric's box total.

        Uses subtraction for bps metrics (rate comparison) or division for
        pct_change metrics (volume comparison). See class docstring for why.
        """
        return (operand_1 - operand_2) * BPS_MULTIPLIER if metric_name in self.function_bps_metrics else (
                ((operand_1/operand_2) - 1) * PCT_MULTIPLIER)

    def compute_extra_months(self):
        """Extend cy_monthly/py_monthly beyond the trailing 12 months.

        Two cases require extra months appended to the monthly DataFrames:
        1. Partial month: if the week ends mid-month, manually aggregate the
           partial CY month (with missing-day guard) and full PY month.
        2. Fiscal year extension: if the fiscal year doesn't end this month,
           append forecast months through fiscal year end (with 0 -> NaN).

        Mutates: self.cy_monthly, self.py_monthly
        """
        if not wbr_util.is_last_day_of_month(self.cy_week_ending):
            self.aggregate_week_ending_month()
        if self.fiscal_month.lower() != self.cy_week_ending.strftime("%b").lower():
            self.aggregate_months_to_fiscal_year_end()

    def aggregate_months_to_fiscal_year_end(self):
        """Append forecast months from the current month through fiscal year end.

        When the fiscal year doesn't end in December (or the current month isn't the
        last month of the fiscal year), the 6-12 chart needs to show future months
        through fiscal year end. This method appends those months so the chart's
        right half extends to the fiscal year boundary.

        Future months with zero values are replaced with NaN because zeros in future
        months represent missing data (no actuals yet), not actual zero values. NaN
        renders as blank on the chart rather than a misleading zero line.

        Mutates: self.cy_monthly, self.py_monthly
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
        """Build a partial-month aggregate when the week doesn't end on month-end.

        When cy_week_ending falls mid-month, the trailing-12-months data won't
        include the current month (resample only produces complete months). This
        method manually aggregates daily data for the partial CY month and appends
        it to cy_monthly/py_monthly.

        The CY partial month is aggregated per-metric with a count-mismatch guard:
        if any day's data is missing for a metric, the whole month becomes NaN. This
        prevents the WBR from showing a misleading partial sum as if it were a
        complete month. PY uses resample since it has complete data for that month.

        Mutates: self.cy_monthly, self.py_monthly
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

    def calculate_box_totals(self, cy_trailing_six_weeks=None, py_trailing_six_weeks=None,
                             cy_week_ending=None, fiscal_month=None, daily_metrics=None,
                             metric_aggregation=None, bps_metrics=None, pct_change_metrics=None):
        """Build the 9-row summary table shown below each WBR chart.

        The box totals provide at-a-glance period comparisons:
            Row 0: LastWk  — most recent full CY week's value
            Row 1: WOW     — week-over-week change (CY wk6 vs CY wk5)
            Row 2: YOY     — year-over-year change for the week (CY wk6 vs PY wk6)
            Row 3: MTD     — month-to-date total
            Row 4: YOY     — year-over-year change for MTD
            Row 5: QTD     — quarter-to-date total (aligned to fiscal calendar)
            Row 6: YOY     — year-over-year change for QTD
            Row 7: YTD     — year-to-date total (aligned to fiscal calendar)
            Row 8: YOY     — year-over-year change for YTD

        All parameters default to self.* attributes when None, so existing callers
        work unchanged. Tests can pass explicit DataFrames to exercise this method
        without constructing a full WBR pipeline.

        Returns three DataFrames:
            box_totals:     CY summary values + comparison rows (WOW/YOY)
            py_box_total:   PY absolute values (used later for YOY in append_yoy_values)
            period_summary: Raw CY/PY period aggregates (10 rows, see constants.py
                            YOY_IDX_*) used by compute_functional_metrics to derive
                            function-metric box totals
        """
        cy_trailing_six_weeks = cy_trailing_six_weeks if cy_trailing_six_weeks is not None else self.cy_trailing_six_weeks
        py_trailing_six_weeks = py_trailing_six_weeks if py_trailing_six_weeks is not None else self.py_trailing_six_weeks
        cy_week_ending = cy_week_ending if cy_week_ending is not None else self.cy_week_ending
        fiscal_month = fiscal_month if fiscal_month is not None else self.fiscal_month
        daily_metrics = daily_metrics if daily_metrics is not None else self.daily_metrics
        metric_aggregation = metric_aggregation if metric_aggregation is not None else self.metric_aggregation
        bps_metrics = bps_metrics if bps_metrics is not None else self.bps_metrics
        pct_change_metrics = pct_change_metrics if pct_change_metrics is not None else self.pct_change_metrics

        # Initialize empty DataFrames for box totals and year-over-year (YoY) box totals
        box_totals = pd.DataFrame()
        py_box_totals = pd.DataFrame()

        # Extract specific rows from current year (cy) and previous year (py) trailing six weeks dataframes
        cy_wk6, cy_wk5, py_wk6, py_wk5 = (
            cy_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 1]],
            cy_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 2]],
            py_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 1]],
            py_trailing_six_weeks.iloc[[NUM_TRAILING_WEEKS - 2]],
        )

        # Remove 'PY__' prefix from column names for py_wk6 and py_wk5
        py_wk6.columns, py_wk5.columns = py_wk6.columns.str.replace('PY__', ''), py_wk5.columns.str.replace('PY__', '')

        # Reset the indices to 0 for the selected rows
        dataframe_list = [cy_wk6, cy_wk5, py_wk6, py_wk5]
        [x.reset_index(drop=True, inplace=True) for x in dataframe_list]

        # Extract common dates for year-over-year comparison
        cy_last_day = pd.to_datetime(cy_week_ending)
        py_last_day = pd.to_datetime(cy_last_day) - relativedelta.relativedelta(years=1)

        # Calculate start dates for MTD, QTD, and YTD
        cy_first_day_mtd = cy_last_day.replace(day=1)
        py_first_day_mtd = py_last_day.replace(day=1)

        try:
            cy_first_day_qtd = cy_last_day.to_period('Q-' + fiscal_month).to_timestamp()
            py_first_day_qtd = py_last_day.to_period('Q-' + fiscal_month).to_timestamp()
            cy_first_day_ytd = cy_last_day.to_period('Y-' + fiscal_month).to_timestamp()
            py_first_day_ytd = py_last_day.to_period('Y-' + fiscal_month).to_timestamp()
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
            cy_data = daily_metrics.query(f'Date >= @{period_range[0][0]} and Date <= @{period_range[0][1]}')
            py_data = daily_metrics.query(f'Date >= @{period_range[1][0]} and Date <= @{period_range[1][1]}')

            # Resample data annually based on fiscal month and calculate aggregated metric
            cy_total = cy_data.resample('YE-' + fiscal_month, label='right', closed='right', on='Date').agg(
                metric_aggregation).reset_index().sort_values(by='Date')
            py_total = py_data.resample('YE-' + fiscal_month, label='right', closed='right', on='Date').agg(
                metric_aggregation).reset_index().sort_values(by='Date')

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
            if len(bps_metrics) > 0:
                bps_metric_df = df[bps_metrics]
                list_bps_df.append(bps_metric_df)
            if len(pct_change_metrics) > 0:
                pct_change_metric_df = df[pct_change_metrics]
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
