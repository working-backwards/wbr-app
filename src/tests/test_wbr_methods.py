# SPDX-License-Identifier: Apache-2.0
"""
Tests for WBR methods with explicit parameters.

These tests exercise the parameterized pipeline methods (calculate_box_totals,
calculate_yoy_box_total, compute_extra_months, _apply_function_to_all_series,
_compute_box_total_yoy) using hand-crafted DataFrames, without constructing
a full WBR pipeline. This verifies the explicit-params pattern works and
documents the expected behavior of each method in isolation.
"""
from datetime import datetime, timedelta
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from src.constants import (
    BOX_IDX_LAST_WK,
    BOX_IDX_MTD,
    BOX_IDX_QTD,
    BOX_IDX_WOW,
    BOX_IDX_YOY_MTD,
    BOX_IDX_YOY_QTD,
    BOX_IDX_YOY_WK,
    BOX_IDX_YOY_YTD,
    BOX_IDX_YTD,
    BPS_MULTIPLIER,
    NUM_BOX_TOTAL_ROWS,
    NUM_TRAILING_WEEKS,
    PCT_MULTIPLIER,
    PY_WEEKLY_OFFSET_DAYS,
)
from src.wbr import WBR


# ---------------------------------------------------------------------------
# Helpers to build minimal DataFrames
# ---------------------------------------------------------------------------

def _make_trailing_six(metric_names, values_per_metric, prefix=""):
    """Build a trailing-six-weeks DataFrame with a Date column and N metric columns."""
    dates = pd.date_range("2022-03-19", periods=NUM_TRAILING_WEEKS, freq="7D")
    data = {"Date": dates}
    for name, values in zip(metric_names, values_per_metric):
        data[prefix + name] = values
    return pd.DataFrame(data)


def _make_monthly(metric_names, values_per_metric, n_months=12, prefix=""):
    """Build a trailing-monthly DataFrame with a Date column and N metric columns."""
    dates = pd.date_range("2021-05-31", periods=n_months, freq="ME")
    data = {"Date": dates}
    for name, values in zip(metric_names, values_per_metric):
        data[prefix + name] = values
    return pd.DataFrame(data)


def _make_daily(metric_names, values_per_metric, start="2020-01-01", periods=730):
    """Build a daily DataFrame spanning ~2 years for box-total period aggregation."""
    dates = pd.date_range(start, periods=periods, freq="D")
    data = {"Date": dates}
    for name, values in zip(metric_names, values_per_metric):
        data[name] = values
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# calculate_yoy_box_total (static method)
# ---------------------------------------------------------------------------

class TestCalculateYoyBoxTotal:
    """Test the static YOY comparison method with explicit fn_bps_metrics."""

    def test_bps_metric_uses_subtraction(self):
        """bps metrics compare by subtraction: (CY - PY) * 10,000."""
        result = WBR.calculate_yoy_box_total(0.05, 0.03, "ConversionRate", ["ConversionRate"])
        expected = (0.05 - 0.03) * BPS_MULTIPLIER
        assert result == pytest.approx(expected)

    def test_pct_change_metric_uses_division(self):
        """pct_change metrics compare by division: ((CY / PY) - 1) * 100."""
        result = WBR.calculate_yoy_box_total(200, 100, "PageViews", [])
        expected = ((200 / 100) - 1) * PCT_MULTIPLIER
        assert result == pytest.approx(expected)

    def test_pct_change_with_zero_denominator(self):
        """Division by zero raises ZeroDivisionError (callers handle this upstream)."""
        with pytest.raises(ZeroDivisionError):
            WBR.calculate_yoy_box_total(100, 0, "PageViews", [])

    def test_bps_with_negative_change(self):
        """A decrease in a bps metric produces a negative value."""
        result = WBR.calculate_yoy_box_total(0.02, 0.05, "ConversionRate", ["ConversionRate"])
        expected = (0.02 - 0.05) * BPS_MULTIPLIER
        assert result == pytest.approx(expected)
        assert result < 0


# ---------------------------------------------------------------------------
# _apply_function_to_all_series (with explicit DataFrames)
# ---------------------------------------------------------------------------

class TestApplyFunctionToAllSeries:
    """Test function-metric computation with hand-crafted DataFrames."""

    def _make_wbr_stub(self):
        """Create a minimal WBR-like object with the attributes needed for method calls."""
        stub = MagicMock(spec=WBR)
        # calculate_yoy_box_total is a static method, bind the real one
        stub.calculate_yoy_box_total = WBR.calculate_yoy_box_total
        # _compute_box_total_yoy needs to be the real method too
        stub._compute_box_total_yoy = WBR._compute_box_total_yoy.__get__(stub)
        stub.function_bps_metrics = []
        return stub

    def test_sum_operation(self):
        """Sum operation adds two metric columns across all DataFrames."""
        metrics = ["MetricA", "MetricB"]
        cy_weekly = _make_trailing_six(metrics, [[10, 20, 30, 40, 50, 60], [1, 2, 3, 4, 5, 6]])
        py_weekly = _make_trailing_six(metrics, [[5, 10, 15, 20, 25, 30], [1, 1, 1, 1, 1, 1]], prefix="PY__")
        cy_month = _make_monthly(metrics, [[100] * 12, [10] * 12])
        py_month = _make_monthly(metrics, [[50] * 12, [5] * 12], prefix="PY__")
        box = pd.DataFrame({"MetricA": [60.0] * NUM_BOX_TOTAL_ROWS, "MetricB": [6.0] * NUM_BOX_TOTAL_ROWS})
        py_box = pd.DataFrame({"MetricA": [30.0] * NUM_BOX_TOTAL_ROWS, "MetricB": [1.0] * NUM_BOX_TOTAL_ROWS})
        period_summary = pd.DataFrame({"MetricA": [60.0] * 10, "MetricB": [6.0] * 10})

        stub = self._make_wbr_stub()
        stub.period_summary = period_summary

        WBR._apply_function_to_all_series(
            stub,
            column_list=["MetricA", "MetricB"],
            py_column_list=["PY__MetricA", "PY__MetricB"],
            metric_name="SumMetric",
            operation="sum",
            cy_trailing_six_weeks=cy_weekly,
            py_trailing_six_weeks=py_weekly,
            cy_monthly=cy_month,
            py_monthly=py_month,
            box_totals_df=box,
            py_box_total=py_box,
        )

        # CY weekly: 10+1=11, 20+2=22, ...
        assert cy_weekly["SumMetric"].tolist() == [11, 22, 33, 44, 55, 66]
        # PY weekly: 5+1=6, 10+1=11, ...
        assert py_weekly["PY__SumMetric"].tolist() == [6, 11, 16, 21, 26, 31]
        # Monthly: 100+10=110
        assert all(cy_month["SumMetric"] == 110)
        assert all(py_month["PY__SumMetric"] == 55)
        # Box totals: 60+6=66
        assert box["SumMetric"].iloc[0] == 66
        assert py_box["SumMetric"].iloc[0] == 31

    def test_difference_operation(self):
        """Difference operation subtracts second column from first."""
        metrics = ["Revenue", "Cost"]
        cy_weekly = _make_trailing_six(metrics, [[100, 200, 300, 400, 500, 600], [10, 20, 30, 40, 50, 60]])
        py_weekly = _make_trailing_six(metrics, [[50, 100, 150, 200, 250, 300], [5, 10, 15, 20, 25, 30]], prefix="PY__")
        cy_month = _make_monthly(metrics, [[1000] * 12, [100] * 12])
        py_month = _make_monthly(metrics, [[500] * 12, [50] * 12], prefix="PY__")
        box = pd.DataFrame({"Revenue": [600.0] * NUM_BOX_TOTAL_ROWS, "Cost": [60.0] * NUM_BOX_TOTAL_ROWS})
        py_box = pd.DataFrame({"Revenue": [300.0] * NUM_BOX_TOTAL_ROWS, "Cost": [30.0] * NUM_BOX_TOTAL_ROWS})
        period_summary = pd.DataFrame({"Revenue": [600.0] * 10, "Cost": [60.0] * 10})

        stub = self._make_wbr_stub()
        stub.period_summary = period_summary

        WBR._apply_function_to_all_series(
            stub,
            column_list=["Revenue", "Cost"],
            py_column_list=["PY__Revenue", "PY__Cost"],
            metric_name="Profit",
            operation="difference",
            cy_trailing_six_weeks=cy_weekly,
            py_trailing_six_weeks=py_weekly,
            cy_monthly=cy_month,
            py_monthly=py_month,
            box_totals_df=box,
            py_box_total=py_box,
        )

        # CY weekly: 100-10=90, 200-20=180, ...
        assert cy_weekly["Profit"].tolist() == [90, 180, 270, 360, 450, 540]
        assert cy_month["Profit"].iloc[0] == 900

    def test_divide_operation(self):
        """Divide operation divides first column by second."""
        metrics = ["Revenue", "Units"]
        cy_weekly = _make_trailing_six(metrics, [[100, 200, 300, 400, 500, 600], [10, 20, 30, 40, 50, 60]])
        py_weekly = _make_trailing_six(metrics, [[50, 100, 150, 200, 250, 300], [5, 10, 15, 20, 25, 30]], prefix="PY__")
        cy_month = _make_monthly(metrics, [[1000] * 12, [100] * 12])
        py_month = _make_monthly(metrics, [[500] * 12, [50] * 12], prefix="PY__")
        box = pd.DataFrame({"Revenue": [600.0] * NUM_BOX_TOTAL_ROWS, "Units": [60.0] * NUM_BOX_TOTAL_ROWS})
        py_box = pd.DataFrame({"Revenue": [300.0] * NUM_BOX_TOTAL_ROWS, "Units": [30.0] * NUM_BOX_TOTAL_ROWS})
        period_summary = pd.DataFrame({"Revenue": [600.0] * 10, "Units": [60.0] * 10})

        stub = self._make_wbr_stub()
        stub.period_summary = period_summary

        WBR._apply_function_to_all_series(
            stub,
            column_list=["Revenue", "Units"],
            py_column_list=["PY__Revenue", "PY__Units"],
            metric_name="Price",
            operation="divide",
            cy_trailing_six_weeks=cy_weekly,
            py_trailing_six_weeks=py_weekly,
            cy_monthly=cy_month,
            py_monthly=py_month,
            box_totals_df=box,
            py_box_total=py_box,
        )

        # CY weekly: 100/10=10, 200/20=10, ... all = 10
        assert all(cy_weekly["Price"] == 10.0)
        assert all(py_weekly["PY__Price"] == 10.0)
        assert all(cy_month["Price"] == 10.0)

    def test_product_operation(self):
        """Product operation multiplies first column by second."""
        metrics = ["Rate", "Volume"]
        cy_weekly = _make_trailing_six(metrics, [[0.1, 0.2, 0.3, 0.4, 0.5, 0.6], [100, 200, 300, 400, 500, 600]])
        py_weekly = _make_trailing_six(metrics, [[0.1] * 6, [50] * 6], prefix="PY__")
        cy_month = _make_monthly(metrics, [[0.5] * 12, [1000] * 12])
        py_month = _make_monthly(metrics, [[0.3] * 12, [500] * 12], prefix="PY__")
        box = pd.DataFrame({"Rate": [0.6] * NUM_BOX_TOTAL_ROWS, "Volume": [600.0] * NUM_BOX_TOTAL_ROWS})
        py_box = pd.DataFrame({"Rate": [0.1] * NUM_BOX_TOTAL_ROWS, "Volume": [50.0] * NUM_BOX_TOTAL_ROWS})
        period_summary = pd.DataFrame({"Rate": [0.6] * 10, "Volume": [600.0] * 10})

        stub = self._make_wbr_stub()
        stub.period_summary = period_summary

        WBR._apply_function_to_all_series(
            stub,
            column_list=["Rate", "Volume"],
            py_column_list=["PY__Rate", "PY__Volume"],
            metric_name="Result",
            operation="product",
            cy_trailing_six_weeks=cy_weekly,
            py_trailing_six_weeks=py_weekly,
            cy_monthly=cy_month,
            py_monthly=py_month,
            box_totals_df=box,
            py_box_total=py_box,
        )

        # CY weekly: 0.1*100=10, 0.2*200=40, ...
        expected = [0.1 * 100, 0.2 * 200, 0.3 * 300, 0.4 * 400, 0.5 * 500, 0.6 * 600]
        for actual, exp in zip(cy_weekly["Result"], expected):
            assert actual == pytest.approx(exp)


# ---------------------------------------------------------------------------
# calculate_box_totals (with explicit parameters)
# ---------------------------------------------------------------------------

class TestCalculateBoxTotals:
    """Test box-total construction with hand-crafted inputs."""

    def _make_wbr_for_box_totals(self, week_ending="30-APR-2022", fiscal_month="DEC"):
        """Build a WBR-like stub with just enough state for calculate_box_totals."""
        cy_week_ending = datetime.strptime(week_ending, "%d-%b-%Y")

        metrics = ["PageViews"]
        cy_values = [100, 200, 300, 400, 500, 600]
        py_values = [90, 180, 270, 360, 450, 540]

        cy_trailing = _make_trailing_six(metrics, [cy_values])
        py_trailing = _make_trailing_six(metrics, [py_values], prefix="PY__")

        # Build daily data covering ~2 years for MTD/QTD/YTD aggregation
        n_days = 730
        daily_values = np.random.default_rng(42).integers(50, 150, size=n_days).astype(float)
        daily = _make_daily(metrics, [daily_values], start="2020-07-01", periods=n_days)

        metric_aggregation = {"PageViews": lambda x: x.sum(skipna=False)}

        stub = MagicMock(spec=WBR)
        stub.cfg = {"setup": {"__line__": 1}}
        stub.calculate_box_totals = WBR.calculate_box_totals.__get__(stub)

        return stub, cy_trailing, py_trailing, cy_week_ending, fiscal_month, daily, metric_aggregation

    def test_returns_three_dataframes(self):
        """calculate_box_totals returns (box_totals, py_box_total, period_summary)."""
        stub, cy_t, py_t, cwe, fm, daily, agg = self._make_wbr_for_box_totals()

        box_totals, py_box_total, period_summary = stub.calculate_box_totals(
            cy_trailing_six_weeks=cy_t,
            py_trailing_six_weeks=py_t,
            cy_week_ending=cwe,
            fiscal_month=fm,
            daily_metrics=daily,
            metric_aggregation=agg,
            bps_metrics=[],
            pct_change_metrics=["PageViews"],
        )

        assert isinstance(box_totals, pd.DataFrame)
        assert isinstance(py_box_total, pd.DataFrame)
        assert isinstance(period_summary, pd.DataFrame)

    def test_box_totals_has_nine_rows(self):
        """Box totals always has exactly 9 rows (LastWk through YTD YOY)."""
        stub, cy_t, py_t, cwe, fm, daily, agg = self._make_wbr_for_box_totals()

        box_totals, _, _ = stub.calculate_box_totals(
            cy_trailing_six_weeks=cy_t,
            py_trailing_six_weeks=py_t,
            cy_week_ending=cwe,
            fiscal_month=fm,
            daily_metrics=daily,
            metric_aggregation=agg,
            bps_metrics=[],
            pct_change_metrics=["PageViews"],
        )

        assert len(box_totals) == NUM_BOX_TOTAL_ROWS

    def test_box_totals_has_axis_labels(self):
        """Box totals includes Date and Axis columns with the expected labels."""
        stub, cy_t, py_t, cwe, fm, daily, agg = self._make_wbr_for_box_totals()

        box_totals, _, _ = stub.calculate_box_totals(
            cy_trailing_six_weeks=cy_t,
            py_trailing_six_weeks=py_t,
            cy_week_ending=cwe,
            fiscal_month=fm,
            daily_metrics=daily,
            metric_aggregation=agg,
            bps_metrics=[],
            pct_change_metrics=["PageViews"],
        )

        assert "Date" in box_totals.columns
        assert "Axis" in box_totals.columns
        expected_labels = ["LastWk", "WOW", "YOY", "MTD", "YOY", "QTD", "YOY", "YTD", "YOY"]
        assert box_totals["Axis"].tolist() == expected_labels

    def test_last_week_row_matches_cy_week6(self):
        """Row 0 (LastWk) should contain CY week 6 values."""
        stub, cy_t, py_t, cwe, fm, daily, agg = self._make_wbr_for_box_totals()

        box_totals, _, _ = stub.calculate_box_totals(
            cy_trailing_six_weeks=cy_t,
            py_trailing_six_weeks=py_t,
            cy_week_ending=cwe,
            fiscal_month=fm,
            daily_metrics=daily,
            metric_aggregation=agg,
            bps_metrics=[],
            pct_change_metrics=["PageViews"],
        )

        # LastWk row should have CY week 6 value (600)
        assert box_totals["PageViews"].iloc[BOX_IDX_LAST_WK] == pytest.approx(600.0)

    def test_period_summary_has_ten_rows(self):
        """period_summary has 10 rows: 4 weekly + 6 period (MTD/QTD/YTD x CY/PY)."""
        stub, cy_t, py_t, cwe, fm, daily, agg = self._make_wbr_for_box_totals()

        _, _, period_summary = stub.calculate_box_totals(
            cy_trailing_six_weeks=cy_t,
            py_trailing_six_weeks=py_t,
            cy_week_ending=cwe,
            fiscal_month=fm,
            daily_metrics=daily,
            metric_aggregation=agg,
            bps_metrics=[],
            pct_change_metrics=["PageViews"],
        )

        assert len(period_summary) == 10
