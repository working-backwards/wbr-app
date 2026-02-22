# SPDX-License-Identifier: Apache-2.0
"""
Behavioral tests for standalone functions in wbr.py.

These tests exercise the module-level functions (build_agg,
get_bps_and_pct_change_metrics, etc.) that don't require a full WBR
instance. Each test uses minimal inputs to isolate one behavior.
"""
import numpy as np
import pandas as pd

from src.wbr import (
    build_agg,
    get_bps_and_pct_change_metrics,
    get_function_metrics_configs,
)
from src.wbr_utility import (
    apply_operation_and_return_denominator_values,
    apply_sum_operations,
)


# ---------------------------------------------------------------------------
# build_agg
# ---------------------------------------------------------------------------

class TestBuildAgg:
    def test_function_metric_returns_none(self):
        """Function metrics are computed later, so build_agg returns None."""
        item = ("Revenue", {"function": {"divide": []}})
        assert build_agg(item) is None

    def test_sum_returns_skipna_false_lambda(self):
        """Sum aggregation uses skipna=False so partial weeks produce NaN.

        In a WBR, showing a partial weekly total (e.g., only 5 of 7 days)
        would be misleading. It's better to show nothing than an incomplete
        number, so we intentionally propagate NaN through sums.
        """
        item = ("Revenue", {"aggf": "sum"})
        name, agg_fn = build_agg(item)
        assert name == "Revenue"
        # The lambda should propagate NaN
        series = pd.Series([1.0, 2.0, np.nan])
        result = agg_fn(series)
        assert np.isnan(result), "Sum with NaN should return NaN (skipna=False)"

    def test_sum_without_nan(self):
        """Sum without NaN returns the correct total."""
        item = ("Revenue", {"aggf": "sum"})
        _, agg_fn = build_agg(item)
        series = pd.Series([1.0, 2.0, 3.0])
        assert agg_fn(series) == 6.0

    def test_non_sum_returns_string(self):
        """Non-sum aggregation functions (mean, max, etc.) return as-is."""
        item = ("Satisfaction", {"aggf": "mean"})
        name, agg_fn = build_agg(item)
        assert name == "Satisfaction"
        assert agg_fn == "mean"

    def test_last_agg(self):
        item = ("Stock", {"aggf": "last"})
        name, agg_fn = build_agg(item)
        assert agg_fn == "last"

    def test_first_agg(self):
        item = ("Price", {"aggf": "first"})
        name, agg_fn = build_agg(item)
        assert agg_fn == "first"


# ---------------------------------------------------------------------------
# get_bps_and_pct_change_metrics
# ---------------------------------------------------------------------------

class TestGetBpsAndPercentileMetrics:
    def test_four_way_classification(self):
        """Metrics are classified into 4 lists: fn_bps, bps, fn_pct, pct.

        WBR metrics compare current vs prior year in one of two ways:
        - BPS (basis points): rate metrics compared by subtraction (e.g., conversion rate)
        - Percent change: amount metrics compared by division (e.g., revenue)

        Each category is further split by whether the metric is a 'function'
        (computed from other metrics) or a direct column metric.
        """
        configs = {
            "ConvRate": {"metric_comparison_method": "bps", "column": "conv_rate", "aggf": "mean"},
            "Revenue": {"column": "revenue", "aggf": "sum"},
            "FnRate": {"metric_comparison_method": "bps", "function": {"divide": []}},
            "FnRevenue": {"function": {"sum": []}},
        }
        fn_bps, bps, fn_pct, pct = get_bps_and_pct_change_metrics(configs)

        assert fn_bps == ["FnRate"]
        assert bps == ["ConvRate"]
        assert fn_pct == ["FnRevenue"]
        assert pct == ["Revenue"]

    def test_all_bps(self):
        """All metrics marked as bps."""
        configs = {
            "Rate1": {"metric_comparison_method": "bps", "column": "r1", "aggf": "mean"},
            "Rate2": {"metric_comparison_method": "bps", "column": "r2", "aggf": "mean"},
        }
        fn_bps, bps, fn_pct, pct = get_bps_and_pct_change_metrics(configs)
        assert bps == ["Rate1", "Rate2"]
        assert fn_bps == []
        assert pct == []
        assert fn_pct == []

    def test_all_percentile(self):
        """All metrics without bps flag default to percent change."""
        configs = {
            "Rev": {"column": "rev", "aggf": "sum"},
            "Units": {"column": "units", "aggf": "sum"},
        }
        fn_bps, bps, fn_pct, pct = get_bps_and_pct_change_metrics(configs)
        assert pct == ["Rev", "Units"]
        assert bps == []

    def test_empty_config(self):
        """Empty config returns all empty lists."""
        fn_bps, bps, fn_pct, pct = get_bps_and_pct_change_metrics({})
        assert fn_bps == []
        assert bps == []
        assert fn_pct == []
        assert pct == []

    def test_function_bps_only(self):
        configs = {
            "FnRate": {"metric_comparison_method": "bps", "function": {"divide": []}},
        }
        fn_bps, bps, fn_pct, pct = get_bps_and_pct_change_metrics(configs)
        assert fn_bps == ["FnRate"]
        assert bps == []


# ---------------------------------------------------------------------------
# get_function_metrics_configs
# ---------------------------------------------------------------------------

class TestGetFunctionMetricsConfigs:
    def test_filters_function_metrics(self):
        configs = {
            "Revenue": {"column": "rev", "aggf": "sum"},
            "Ratio": {"function": {"divide": []}},
            "Total": {"function": {"sum": []}},
        }
        result = get_function_metrics_configs(configs)
        assert "Ratio" in result
        assert "Total" in result
        assert "Revenue" not in result

    def test_no_function_metrics(self):
        configs = {
            "Revenue": {"column": "rev", "aggf": "sum"},
        }
        result = get_function_metrics_configs(configs)
        assert result == {}


# ---------------------------------------------------------------------------
# apply_sum_operations / apply_operation_and_return_denominator_values
# ---------------------------------------------------------------------------

class TestApplyOperations:
    def test_apply_sum_operations(self):
        """Sums specific columns at a given row index."""
        df = pd.DataFrame({"A": [10, 20, 30], "B": [1, 2, 3]})
        result = apply_sum_operations(df, ["A", "B"], 0)
        assert result == 11  # 10 + 1

    def test_apply_sum_operations_different_index(self):
        df = pd.DataFrame({"A": [10, 20, 30], "B": [1, 2, 3]})
        result = apply_sum_operations(df, ["A", "B"], 2)
        assert result == 33  # 30 + 3

    def test_denominator_values_sum(self):
        """Sum operation returns denominator values at PY row indices."""
        df = pd.DataFrame({
            "A": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            "B": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        })
        result = apply_operation_and_return_denominator_values("sum", ["A", "B"], df)
        # Indices: 1, 2, 5, 7, 9
        assert len(result) == 5
        assert result[0] == 220  # A[1]+B[1] = 200+20
        assert result[1] == 330  # A[2]+B[2] = 300+30

    def test_denominator_values_difference(self):
        """Difference operation returns A[i] - B[i] at PY row indices."""
        df = pd.DataFrame({
            "A": [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000],
            "B": [10, 20, 30, 40, 50, 60, 70, 80, 90, 100],
        })
        result = apply_operation_and_return_denominator_values("difference", ["A", "B"], df)
        assert result[0] == 180  # 200-20
        assert result[1] == 270  # 300-30

    def test_zero_replaced_with_nan(self):
        """Zero denominators are replaced with NaN to avoid division by zero."""
        df = pd.DataFrame({
            "A": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            "B": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        })
        result = apply_operation_and_return_denominator_values("sum", ["A", "B"], df)
        for val in result:
            assert np.isnan(val), f"Expected NaN but got {val}"
