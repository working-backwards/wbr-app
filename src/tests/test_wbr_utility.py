# SPDX-License-Identifier: Apache-2.0
"""
Behavioral tests for utility functions in wbr_utility.py.

These tests exercise the standalone functions that build the intermediate
data structures used by the WBR computation engine. Each test uses small,
hand-crafted DataFrames to isolate a single behavior.
"""
import datetime

import numpy as np
import pandas as pd
import pytest

from src.wbr_utility import (
    create_axis_label,
    create_dynamic_data_frame,
    create_trailing_six_weeks,
    create_trailing_twelve_months,
    handle_function_metrics_for_extra_attribute,
    is_last_day_of_month,
)


# ---------------------------------------------------------------------------
# is_last_day_of_month
# ---------------------------------------------------------------------------

class TestIsLastDayOfMonth:
    def test_last_day_jan(self):
        assert is_last_day_of_month(datetime.datetime(2023, 1, 31)) is True

    def test_not_last_day(self):
        assert is_last_day_of_month(datetime.datetime(2023, 1, 30)) is False

    def test_feb_non_leap(self):
        assert is_last_day_of_month(datetime.datetime(2023, 2, 28)) is True

    def test_feb_leap_year_28th(self):
        """Feb 28 in a leap year is NOT the last day."""
        assert is_last_day_of_month(datetime.datetime(2024, 2, 28)) is False

    def test_feb_leap_year_29th(self):
        assert is_last_day_of_month(datetime.datetime(2024, 2, 29)) is True

    def test_dec_31(self):
        assert is_last_day_of_month(datetime.datetime(2023, 12, 31)) is True

    def test_apr_30(self):
        assert is_last_day_of_month(datetime.datetime(2023, 4, 30)) is True

    def test_apr_29(self):
        assert is_last_day_of_month(datetime.datetime(2023, 4, 29)) is False


# ---------------------------------------------------------------------------
# create_trailing_six_weeks
# ---------------------------------------------------------------------------

def _make_daily_df(start, days, metrics=None):
    """Helper: create a daily DataFrame with Date + metric columns."""
    dates = pd.date_range(start, periods=days, freq="D")
    df = pd.DataFrame({"Date": dates})
    if metrics:
        for name, values in metrics.items():
            df[name] = values
    return df


class TestCreateTrailingSixWeeks:
    def test_basic_six_rows(self):
        """Should return exactly 6 weekly rows for a full dataset."""
        # 50 days of data ensures at least 6 full weeks
        df = _make_daily_df("2021-08-07", 50, {"Sales": range(50)})
        aggf = {"Sales": "sum"}
        week_ending = datetime.datetime(2021, 9, 25)

        result = create_trailing_six_weeks(df, week_ending, aggf)
        assert len(result) == 6
        assert "Date" in result.columns
        assert "Sales" in result.columns

    def test_weekday_alignment(self):
        """Resampling anchor matches the weekday of week_ending (Saturday)."""
        df = _make_daily_df("2021-08-07", 50, {"Sales": [1.0] * 50})
        aggf = {"Sales": "sum"}
        week_ending = datetime.datetime(2021, 9, 25)  # Saturday

        result = create_trailing_six_weeks(df, week_ending, aggf)
        # The last date in the result should be the week_ending date (Saturday)
        last_date = pd.Timestamp(result.iloc[-1]["Date"])
        assert last_date.isoweekday() == week_ending.isoweekday()

    def test_padding_with_insufficient_data(self):
        """If less than 6 weeks of data exist, pad with NaN to 6 rows."""
        # Only 14 days = 2 full weeks
        df = _make_daily_df("2021-09-12", 14, {"Sales": [10.0] * 14})
        aggf = {"Sales": "sum"}
        week_ending = datetime.datetime(2021, 9, 25)

        result = create_trailing_six_weeks(df, week_ending, aggf)
        assert len(result) == 6
        # First 4 rows should be NaN (padded), last 2 should have data
        assert pd.isna(result.iloc[0]["Sales"])

    def test_skipna_false_for_sum(self):
        """Sum aggregation with skipna=False: a week with missing days
        should propagate NaN rather than produce a partial sum.

        This is critical in WBR: showing a partial weekly total would be
        misleading — it's better to show nothing than an incomplete number.
        """
        dates = pd.date_range("2021-09-19", periods=7, freq="D")
        sales = [100.0, 200.0, np.nan, 400.0, 500.0, 600.0, 700.0]
        df = pd.DataFrame({"Date": dates, "Sales": sales})
        # Build a wider df spanning 6 weeks so we get enough rows
        full_df = _make_daily_df("2021-08-07", 43, {"Sales": [10.0] * 43})
        full_df = pd.concat([full_df, df], ignore_index=True).drop_duplicates(subset="Date")
        full_df = full_df.sort_values("Date").reset_index(drop=True)

        aggf = {"Sales": lambda x: x.sum(skipna=False)}
        week_ending = datetime.datetime(2021, 9, 25)

        result = create_trailing_six_weeks(full_df, week_ending, aggf)
        # The last week (Sep 19-25) has a NaN, so its sum should be NaN
        last_week_sum = result.iloc[-1]["Sales"]
        assert pd.isna(last_week_sum), f"Expected NaN but got {last_week_sum}"

    def test_missing_date_column_raises(self):
        df = pd.DataFrame({"Sales": [1, 2, 3]})
        with pytest.raises(ValueError, match="Date"):
            create_trailing_six_weeks(df, datetime.datetime(2021, 9, 25), {"Sales": "sum"})


# ---------------------------------------------------------------------------
# create_trailing_twelve_months
# ---------------------------------------------------------------------------

class TestCreateTrailingTwelveMonths:
    def _make_monthly_df(self, start_year, start_month, num_months):
        """Create daily data spanning multiple months."""
        start = datetime.date(start_year, start_month, 1)
        # Create enough daily rows to span num_months
        end = start + datetime.timedelta(days=num_months * 31)
        dates = pd.date_range(start, end, freq="D")
        df = pd.DataFrame({"Date": dates, "Revenue": [100.0] * len(dates)})
        return df

    def test_basic_twelve_rows(self):
        """Should return exactly 12 monthly rows for a full dataset."""
        df = self._make_monthly_df(2020, 1, 24)
        aggf = {"Revenue": "sum"}
        week_ending = datetime.datetime(2021, 9, 25)

        result = create_trailing_twelve_months(df, week_ending, aggf)
        assert len(result) == 12

    def test_is_last_day_boundary(self):
        """When week_ending is the last day of the month, that month is included."""
        df = self._make_monthly_df(2020, 1, 24)
        aggf = {"Revenue": "sum"}
        # August 31 is the last day of August
        week_ending = datetime.datetime(2021, 8, 31)

        result = create_trailing_twelve_months(df, week_ending, aggf)
        assert len(result) == 12
        # The last entry should be August 2021
        last_date = pd.Timestamp(result.iloc[-1]["Date"])
        assert last_date.month == 8

    def test_mid_month_excludes_current(self):
        """When week_ending is mid-month, the last full month is the prior month."""
        df = self._make_monthly_df(2020, 1, 24)
        aggf = {"Revenue": "sum"}
        week_ending = datetime.datetime(2021, 9, 15)

        result = create_trailing_twelve_months(df, week_ending, aggf)
        last_date = pd.Timestamp(result.iloc[-1]["Date"])
        assert last_date.month == 8  # August, not September

    def test_padding_with_insufficient_data(self):
        """If less than 12 months of data, pad to exactly 12 rows."""
        # Only 6 months of data
        df = self._make_monthly_df(2021, 4, 6)
        aggf = {"Revenue": "sum"}
        week_ending = datetime.datetime(2021, 9, 25)

        result = create_trailing_twelve_months(df, week_ending, aggf)
        assert len(result) == 12
        # First several rows should be NaN
        assert pd.isna(result.iloc[0]["Revenue"])


# ---------------------------------------------------------------------------
# create_dynamic_data_frame
# ---------------------------------------------------------------------------

class TestCreateDynamicDataFrame:
    def _base_df(self):
        """Create a minimal daily DataFrame for testing."""
        return pd.DataFrame({
            "Date": pd.date_range("2021-01-01", periods=10, freq="D"),
            "ColA": range(10),
            "ColB": range(10, 20),
            "Region": ["US"] * 5 + ["EU"] * 5,
        })

    def test_column_metric(self):
        """'column' type metric extracts and aggregates a named column."""
        df = self._base_df()
        config = {
            "MetricA": {"column": "ColA", "aggf": "sum", "__line__": 1},
        }
        result = create_dynamic_data_frame(df, config)
        assert "MetricA" in result.columns
        assert "Date" in result.columns

    def test_filter_metric_with_query(self):
        """'filter' type metric applies a .query() to subset the data."""
        df = self._base_df()
        config = {
            "USOnly": {
                "filter": {"base_column": "ColA", "query": 'Region == "US"', "__line__": 2},
                "aggf": "sum",
                "__line__": 2,
            },
        }
        result = create_dynamic_data_frame(df, config)
        assert "USOnly" in result.columns

    def test_function_metric_skipped(self):
        """'function' type metrics are skipped during dynamic df creation."""
        df = self._base_df()
        config = {
            "MetricA": {"column": "ColA", "aggf": "sum", "__line__": 1},
            "Ratio": {"function": {"divide": []}, "aggf": "sum", "__line__": 3},
        }
        result = create_dynamic_data_frame(df, config)
        assert "MetricA" in result.columns
        assert "Ratio" not in result.columns

    def test_sum_uses_min_count(self):
        """Sum aggregation uses min_count=1 so all-NaN groups yield NaN, not 0."""
        df = pd.DataFrame({
            "Date": pd.to_datetime(["2021-01-01", "2021-01-01"]),
            "ColA": [np.nan, np.nan],
        })
        config = {"MetricA": {"column": "ColA", "aggf": "sum", "__line__": 1}}
        result = create_dynamic_data_frame(df, config)
        assert pd.isna(result["MetricA"].iloc[0])


# ---------------------------------------------------------------------------
# create_axis_label
# ---------------------------------------------------------------------------

class TestCreateAxisLabel:
    def test_week_labels_wrap_modulo_52(self):
        """Week numbers wrap correctly: wk 53 -> wk 1.

        WBR week numbers use % 52 + 1 so that week 53 wraps to week 1.
        This ensures axis labels are always in the 1-52 range.
        """
        week_ending = datetime.datetime(2022, 1, 1)
        week_number = 1  # First week of the year

        labels = create_axis_label(week_ending, week_number, 12)
        # First 6 labels are week labels
        week_labels = labels[:6]
        # Week 1 minus 6 = -5, which wraps to wk 48 via (1-6) % 52 + 1 = 48
        assert week_labels[0] == "wk 48"

    def test_separator_between_weeks_and_months(self):
        """There's a space separator between weekly and monthly labels."""
        labels = create_axis_label(datetime.datetime(2021, 9, 25), 38, 12)
        assert labels[6] == " "

    def test_monthly_labels_count(self):
        """Number of monthly labels matches the requested count."""
        labels = create_axis_label(datetime.datetime(2021, 9, 25), 38, 12)
        # 6 weeks + 1 separator + 12 months = 19
        assert len(labels) == 19

    def test_variable_month_count(self):
        """Labels adapt when more than 12 months are requested (fiscal year extension)."""
        labels = create_axis_label(datetime.datetime(2021, 9, 25), 38, 16)
        assert len(labels) == 23  # 6 + 1 + 16


# ---------------------------------------------------------------------------
# handle_function_metrics_for_extra_attribute
# ---------------------------------------------------------------------------

class TestHandleFunctionMetricsForExtraAttribute:
    def test_divide_operation(self):
        """Divide operation: metric = col0 / col1."""
        current = pd.DataFrame({"A": [10.0, 20.0], "B": [2.0, 4.0]})
        previous = pd.DataFrame({"A": [8.0, 16.0], "B": [2.0, 4.0]})

        config = {"divide": [{"column": {"name": "A"}}, {"column": {"name": "B"}}]}
        handle_function_metrics_for_extra_attribute("Ratio", config, current, previous)

        assert "Ratio" in current.columns
        assert current["Ratio"].iloc[0] == 5.0   # 10/2
        assert current["Ratio"].iloc[1] == 5.0   # 20/4

    def test_sum_operation(self):
        """Sum operation: metric = col0 + col1."""
        current = pd.DataFrame({"A": [10.0, 20.0], "B": [5.0, 10.0]})
        previous = pd.DataFrame({"A": [8.0, 16.0], "B": [4.0, 8.0]})

        config = {"sum": [{"column": {"name": "A"}}, {"column": {"name": "B"}}]}
        handle_function_metrics_for_extra_attribute("Total", config, current, previous)

        assert current["Total"].iloc[0] == 15.0  # 10+5
        assert previous["Total"].iloc[0] == 12.0  # 8+4

    def test_difference_operation(self):
        """Difference operation: metric = col0 - col1."""
        current = pd.DataFrame({"A": [10.0], "B": [3.0]})
        previous = pd.DataFrame({"A": [8.0], "B": [2.0]})

        config = {"difference": [{"column": {"name": "A"}}, {"column": {"name": "B"}}]}
        handle_function_metrics_for_extra_attribute("Diff", config, current, previous)

        assert current["Diff"].iloc[0] == 7.0  # 10-3
        assert previous["Diff"].iloc[0] == 6.0  # 8-2

    def test_product_operation(self):
        """Product operation: metric = col0 * col1."""
        current = pd.DataFrame({"A": [3.0], "B": [4.0]})
        previous = pd.DataFrame({"A": [2.0], "B": [5.0]})

        config = {"product": [{"column": {"name": "A"}}, {"column": {"name": "B"}}]}
        handle_function_metrics_for_extra_attribute("Prod", config, current, previous)

        assert current["Prod"].iloc[0] == 12.0  # 3*4
        assert previous["Prod"].iloc[0] == 10.0  # 2*5

    def test_unsupported_operation_raises(self):
        current = pd.DataFrame({"A": [1.0], "B": [2.0]})
        previous = pd.DataFrame({"A": [1.0], "B": [2.0]})
        config = {"modulo": [{"column": {"name": "A"}}, {"column": {"name": "B"}}]}
        with pytest.raises(ValueError, match="Unsupported"):
            handle_function_metrics_for_extra_attribute("Bad", config, current, previous)
