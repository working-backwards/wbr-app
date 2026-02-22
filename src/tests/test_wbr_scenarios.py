# SPDX-License-Identifier: Apache-2.0
"""
Golden-output tests for WBR scenarios.

These tests validate that the WBR computation engine and deck builder produce
the exact same outputs as the expected values in each scenario's testconfig.yml.
The 42 test cases cover all 9 scenarios and exercise every metric type (column,
filter, function) and every deck block type (6_12 chart, trailing table).

Ported from the HTTP endpoint at /wbr-unit-test (src/test.py) so they run in CI
via pytest.  The original endpoint is preserved for manual browser-based testing.
"""
import math

import numpy as np
import pytest

from src.controller_utility import SixTwelveChart, TrailingTable
from src.tests.conftest import collect_all_test_cases, load_scenario

# ---------------------------------------------------------------------------
# Comparison helpers (ported from src/test.py)
# ---------------------------------------------------------------------------


def round_off(n, ndigits):
    """Round-half-away-from-zero, matching the original WBR test logic."""
    if type(n) is float and np.isnan(n) or type(n) is str:
        return np.nan
    part = n * 10**ndigits
    delta = part - int(part)
    if delta >= 0.5 or -0.5 < delta <= 0:
        part = math.ceil(part)
    else:
        part = math.floor(part)
    return part / (10**ndigits) if ndigits >= 0 else part * 10 ** abs(ndigits)


def nearly_equal(a, b, sig_fig):
    """Check near-equality accounting for NaN and floating point."""
    if np.isnan(a) and not np.isnan(b):
        return False
    if np.isnan(b) and not np.isnan(a):
        return False
    return a is b or int(a * 10**sig_fig) == int(b * 10**sig_fig) or round_off(a, 1) == round_off(b, 1)


def assert_values_equal(actual, expected, label=""):
    """Assert two lists of numeric values are nearly equal."""
    assert len(actual) == len(expected), f"{label}: length mismatch {len(actual)} != {len(expected)}"
    for i, (a, e) in enumerate(zip(actual, expected)):
        a_r, e_r = round_off(a, 2), round_off(e, 2)
        assert nearly_equal(a_r, e_r, 1), f"{label}[{i}]: {a_r} != {e_r}"


def replace_string_with_nan(lst):
    """Replace string values with NaN for numeric comparison."""
    return [x if not isinstance(x, str) else np.nan for x in lst]


# ---------------------------------------------------------------------------
# Collect parametrize cases
# ---------------------------------------------------------------------------

ALL_CASES = collect_all_test_cases()


@pytest.mark.parametrize(
    "scenario_name, test_dict",
    [(name, td) for name, td, _ in ALL_CASES],
    ids=[tid for _, _, tid in ALL_CASES],
)
class TestGoldenOutput:
    """Validates golden outputs for every test case across all scenarios."""

    def _get_block(self, scenario_name, test_dict):
        """Find the deck block matching the test's metric_name."""
        _, deck = load_scenario(scenario_name)
        metric_name = test_dict["metric_name"]
        blocks = [b for b in deck.blocks if b.title == metric_name]
        assert blocks, f"No block found for metric '{metric_name}'"
        return blocks[0]

    def _get_wbr(self, scenario_name):
        wbr1, _ = load_scenario(scenario_name)
        return wbr1

    # -- DataFrame length tests --

    def test_cy_dataframe_length(self, scenario_name, test_dict):
        """CY monthly DataFrame has the expected number of rows."""
        wbr1 = self._get_wbr(scenario_name)
        metric_name = test_dict["metric_name"]
        expected = test_dict["cy_monthly_data_frame_length"]
        block = self._get_block(scenario_name, test_dict)

        if isinstance(block, TrailingTable):
            # TrailingTable checks total DataFrame shape, not a specific metric column
            actual = wbr1.cy_monthly.shape[0]
        elif "WOW" in metric_name or "MOM" in metric_name or "YOY" in metric_name:
            actual = len(wbr1.metrics[metric_name][7:])
        else:
            actual = len(list(wbr1.cy_monthly[metric_name]))
        assert actual == expected, f"CY length {actual} != {expected}"

    def test_py_dataframe_length(self, scenario_name, test_dict):
        """PY monthly DataFrame has the expected number of rows."""
        wbr1 = self._get_wbr(scenario_name)
        metric_name = test_dict["metric_name"]
        expected = test_dict["py_monthly_data_frame_length"]
        block = self._get_block(scenario_name, test_dict)

        if isinstance(block, TrailingTable):
            actual = wbr1.py_trailing_twelve_months.shape[0]
        elif "WOW" in metric_name or "MOM" in metric_name or "YOY" in metric_name:
            return  # Not applicable for comparison metrics
        else:
            actual = len(wbr1.metrics["PY__" + metric_name][7:])
        assert actual == expected, f"PY length {actual} != {expected}"

    # -- SixTwelveChart-specific tests --

    def test_cy_six_weeks(self, scenario_name, test_dict):
        """CY 6-week values match expected golden output."""
        if "cy_6_weeks" not in test_dict:
            pytest.skip("No cy_6_weeks in test config")
        block = self._get_block(scenario_name, test_dict)
        if not isinstance(block, SixTwelveChart):
            pytest.skip("Not a SixTwelveChart block")

        y_axis = block.yAxis
        for y_axis_obj in y_axis:
            metric_obj = y_axis_obj.get("metric") or y_axis_obj.get("Target")
            primary = metric_obj.current[0]
            actual = replace_string_with_nan(primary["primaryAxis"][0:6])
            assert_values_equal(actual, test_dict["cy_6_weeks"], "cy_6_weeks")

    def test_py_six_weeks(self, scenario_name, test_dict):
        """PY 6-week values match expected golden output."""
        if "py_6_weeks" not in test_dict:
            pytest.skip("No py_6_weeks in test config")
        block = self._get_block(scenario_name, test_dict)
        if not isinstance(block, SixTwelveChart):
            pytest.skip("Not a SixTwelveChart block")
        if "graph_prior_year_flag" in test_dict and not test_dict["graph_prior_year_flag"]:
            pytest.skip("graph_prior_year_flag is False")

        y_axis = block.yAxis
        for y_axis_obj in y_axis:
            metric_obj = y_axis_obj.get("metric") or y_axis_obj.get("Target")
            if metric_obj.previous is None:
                pytest.skip("No previous year data")
            primary = metric_obj.previous[0]
            actual = replace_string_with_nan(primary["primaryAxis"][0:6])
            assert_values_equal(actual, test_dict["py_6_weeks"], "py_6_weeks")

    def test_cy_twelve_months(self, scenario_name, test_dict):
        """CY 12-month values match expected golden output."""
        if "cy_monthly" not in test_dict:
            pytest.skip("No cy_monthly in test config")
        block = self._get_block(scenario_name, test_dict)
        if not isinstance(block, SixTwelveChart):
            pytest.skip("Not a SixTwelveChart block")

        y_axis = block.yAxis
        for y_axis_obj in y_axis:
            metric_obj = y_axis_obj.get("metric") or y_axis_obj.get("Target")
            secondary = metric_obj.current[1]
            actual = replace_string_with_nan(secondary["secondaryAxis"][7:])
            assert_values_equal(actual, test_dict["cy_monthly"], "cy_monthly")

    def test_py_twelve_months(self, scenario_name, test_dict):
        """PY 12-month values match expected golden output."""
        if "py_monthly" not in test_dict:
            pytest.skip("No py_monthly in test config")
        block = self._get_block(scenario_name, test_dict)
        if not isinstance(block, SixTwelveChart):
            pytest.skip("Not a SixTwelveChart block")
        if "graph_prior_year_flag" in test_dict and not test_dict["graph_prior_year_flag"]:
            pytest.skip("graph_prior_year_flag is False")

        y_axis = block.yAxis
        for y_axis_obj in y_axis:
            metric_obj = y_axis_obj.get("metric") or y_axis_obj.get("Target")
            if metric_obj.previous is None:
                pytest.skip("No previous year data")
            secondary = metric_obj.previous[1]
            actual = replace_string_with_nan(secondary["secondaryAxis"][7:])
            assert_values_equal(actual, test_dict["py_monthly"], "py_monthly")

    def test_x_axis(self, scenario_name, test_dict):
        """X-axis labels match expected golden output."""
        if "x_axis" not in test_dict:
            pytest.skip("No x_axis in test config")
        block = self._get_block(scenario_name, test_dict)
        if not isinstance(block, SixTwelveChart):
            pytest.skip("Not a SixTwelveChart block")

        x_axis = list(block.xAxis)
        # Remove separator space between weeks and months, matching src/test.py
        if " " in x_axis:
            x_axis.remove(" ")
        assert x_axis == test_dict["x_axis"], f"x_axis mismatch: {x_axis} != {test_dict['x_axis']}"

    def test_box_totals(self, scenario_name, test_dict):
        """Box total (summary table) values match expected golden output."""
        if "box_totals" not in test_dict:
            pytest.skip("No box_totals in test config")
        block = self._get_block(scenario_name, test_dict)
        if isinstance(block, SixTwelveChart):
            actual = replace_string_with_nan(block.table["tableBody"][0])
            assert_values_equal(actual, test_dict["box_totals"], "box_totals")
        elif isinstance(block, TrailingTable):
            pytest.skip("Box totals not applicable to TrailingTable")

    # -- TrailingTable-specific tests --

    def test_trailing_table_headers(self, scenario_name, test_dict):
        """Trailing table headers match expected golden output."""
        if "headers" not in test_dict:
            pytest.skip("No headers in test config")
        block = self._get_block(scenario_name, test_dict)
        if not isinstance(block, TrailingTable):
            pytest.skip("Not a TrailingTable block")

        assert block.headers == test_dict["headers"], (
            f"Headers mismatch: {block.headers} != {test_dict['headers']}"
        )

    def test_trailing_table_rows(self, scenario_name, test_dict):
        """Trailing table row data matches expected golden output."""
        block = self._get_block(scenario_name, test_dict)
        if not isinstance(block, TrailingTable):
            pytest.skip("Not a TrailingTable block")

        for row in block.rows:
            if row.rowHeader not in test_dict:
                continue  # Only validate rows that have expected values
            expected = list(test_dict[row.rowHeader])
            assert_values_equal(row.rowData, expected, row.rowHeader)
