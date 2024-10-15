import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

import math
import numpy as np
import pandas
import yaml

import src.wbr as wbr
from src.controller_utility import SixTwelveChart, TrailingTable, get_wbr_deck, SafeLineLoader

test_suite_folder = Path(os.path.dirname(__file__)) / 'unit_test_case'


@dataclass
class Result:
    def __init__(self):
        self.scenarios = []
        self.resultStatement: str


class ScenarioResult:
    def __init__(self):
        self.scenario: str
        self.weekEnding: str
        self.fiscalMonth: str
        self.testCases: List[Test]


class Test:
    def __init__(self, block_type):
        self.blockType = block_type
        self.cyDataframeLength: TestResult
        self.pyDataframeLength: TestResult
        self.testNumber: str


class SixTwelveChartTest(Test):
    def __init__(self):
        super().__init__("SixTwelveChart")
        self.cySixWeekTestResult: TestResult
        self.cyTwelveMonthTestResult: TestResult
        self.pySixWeekTestResult: TestResult
        self.pyTwelveMonthTestResult: TestResult
        self.summaryResult: TestResult
        self.xAxis: TestResult


class TrailingTableTest(Test):
    def __init__(self):
        super().__init__("TrailingTable")
        self.rowResult: TestResult
        self.headerResult: List[TestResult]


class TestResult:
    def __init__(self, result: str, failure_message=None, expected=None, calculated=None):
        self.result = result
        self.failureMessage = failure_message
        self.expected = expected
        self.calculated = calculated


def test_wbr():
    """
    Executes a series of tests on WBR scenarios defined in a test suite.

    This function traverses the specified directory for test scenarios, loading the corresponding CSV
    and YAML configuration files for each scenario. It then creates a WBR object for each scenario,
    collects results for each test case defined in the scenario's configuration, and aggregates the
    results into a final result object.

    Returns:
        Result: An object containing the results of all executed scenarios and their respective test cases.

    Process:
        - For each scenario directory found, the function checks if it contains a 'scenario' in its name.
        - It loads the configuration and test files and attempts to create a WBR object.
        - If successful, it initializes a `ScenarioResult` object for the scenario, captures the week ending
          and fiscal month, and runs each defined test case against the WBR object.
        - The results of each scenario are stored in a `Result` object, which is returned after all scenarios
          have been processed.

    Raises:
        Exception: Propagates any errors encountered during the creation of the WBR object or while executing tests.
    """
    result = Result()
    for i, scr in enumerate(sorted(os.walk(test_suite_folder))):
        scenario = scr[0]
        if 'scenario' not in scenario:
            continue

        scenario_name = scenario.split("/")[-1]
        csv_file = scenario + '/original.csv'
        config_file_path = scenario + '/config.yaml'
        test_config_file = scenario + '/testconfig.yml'

        config = yaml.load(open(config_file_path), SafeLineLoader)
        test_config = yaml.safe_load(open(test_config_file))
        try:
            # Create a WBR object using the CSV data and configuration
            wbr1 = wbr.WBR(config, csv=csv_file)
        except Exception as error:
            logging.error(error, exc_info=True)
            raise error

        scenario_result = ScenarioResult()
        scenario_result.scenario = scenario_name
        scenario_result.weekEnding = str(wbr1.cy_week_ending)
        scenario_result.fiscalMonth = wbr1.fiscal_month

        scenario_result.testCases = [build_and_test_wbr(wbr1, test["test"]) for test in test_config["tests"]]

        result.scenarios.append(scenario_result)
    return result


def build_and_test_wbr(wbr1, test):
    """
    Generates a WBR deck from a WBR object and tests specific metrics against predefined test cases.

    This function retrieves the WBR deck for the given WBR object, searches for the specified metric
    within the deck blocks, and executes the corresponding extraction process based on the type of
    the metric (either a chart or a table). If the metric is not found, it logs an appropriate message.

    Parameters:
        wbr1 (WBR): The WBR object containing the configuration and data for generating the deck.
        test (dict): A dictionary containing the test case details, including the metric name to be tested.

    Returns:
        Test: An object representing the outcome of the test for the specified metric. Returns None if the metric is not found.

    Raises:
        Exception: Propagates any errors encountered during the deck generation or extraction processes.
    """
    try:
        # Generate the WBR deck using the WBR object
        deck = get_wbr_deck(wbr1)
    except Exception as error:
        logging.error(error, exc_info=True)
        raise error

    blocks = list(filter(lambda x: x.title == test["metric_name"], deck.blocks))

    if len(blocks) == 0:
        logging.warning(f"no metric found for {test['metric_name']}")
        return Test(None)

    block = blocks[0]
    if isinstance(block, SixTwelveChart):
        return extract_six_twelve_chart(block, test, wbr1)
    if isinstance(block, TrailingTable):
        return extract_trailing_table(block, test, wbr1)


def extract_six_twelve_chart(block: SixTwelveChart, test, wbr1):
    """
    Extracts and validates test results for a SixTwelveChart block against predefined test cases.

    This function takes a SixTwelveChart block and performs a series of validation checks on the
    data within the block, comparing it against the specified test cases. It collects results
    for various time frames (six weeks, twelve months) and stores them in a test case object.

    Parameters:
        block (SixTwelveChart): The SixTwelveChart block containing the data to be validated.
        test (dict): A dictionary containing the details of the test case, including the test case number and metrics to validate.
        wbr1 (WBR): The WBR object that provides the necessary context and data for the validation.

    Returns:
        SixTwelveChartTest: An object containing the results of the validation for the test case, including
                            lengths of dataframes and validation results for each metric.

    Raises:
        Exception: Propagates any errors encountered during the validation processes.
    """

    # Initialize a test case object to store results
    test_case = SixTwelveChartTest()

    # Set the test case number from the input test dictionary
    test_case.testNumber = test["test_case_no"]

    # Validate and store the current year (cy) dataframe length
    test_case.cyDataframeLength = cy_validate_dataframe_length(wbr1, test)

    # Validate and store the previous year (py) dataframe length
    test_case.pyDataframeLength = py_validate_dataframe_length(wbr1, test)

    # Get the x-axis data from the block and validate it
    x_axis = block.xAxis
    test_case.xAxis = validate_axis(x_axis, test)

    # Get the y-axis data and perform validations for each metric
    y_axis = block.yAxis
    for y_axis_object in y_axis:
        # Determine which metric to validate, falling back to "Target" if necessary
        metric_object = y_axis_object["metric"] if "metric" in y_axis_object else y_axis_object["Target"]

        # Validate current year six weeks data and store the result
        test_case.cySixWeekTestResult = validate_cy_six_weeks(metric_object, test)

        # Validate current year twelve months data and store the result
        test_case.cyTwelveMonthTestResult = validate_cy_twelve_months(metric_object, test)

        # Validate previous year six weeks data and store the result
        test_case.pySixWeekTestResult = validate_py_six_weeks(metric_object, test)

        # Validate previous year twelve months data and store the result
        test_case.pyTwelveMonthTestResult = validate_py_twelve_months(metric_object, test)

    # Validate summary table values and store the result
    test_case.summaryResult = validate_summary_table_values(block.table["tableBody"][0], test)

    # Return the populated test case object
    return test_case


def extract_trailing_table(block: TrailingTable, test, wbr1):
    """
    Extracts and validates test results for a TrailingTable block against predefined test cases.

    This function takes a TrailingTable block and performs validation checks on the data within
    the block, comparing it against the specified test cases. It collects results for dataframe
    lengths, headers, and rows and stores them in a test case object.

    Parameters:
        block (TrailingTable): The TrailingTable block containing the data to be validated.
        test (dict): A dictionary containing the details of the test case, including the test case number,
                     expected headers, and row data to validate.
        wbr1 (WBR): The WBR object that provides the necessary context and data for the validation.

    Returns:
        TrailingTableTest: An object containing the results of the validation for the test case, including
                           lengths of dataframes, header validation results, and row validation results.

    Raises:
        Exception: Propagates any errors encountered during the validation processes.
    """

    # Initialize a test case object to store results
    test_case = TrailingTableTest()

    # Set the test case number from the input test dictionary
    test_case.testNumber = test["test_case_no"]

    # Validate and store the current year (cy) dataframe length
    test_case.cyDataframeLength = check_cy_df_shape(test, wbr1)

    # Validate and store the previous year (py) dataframe length
    test_case.pyDataframeLength = check_py_df_shape(test, wbr1)

    # Validate the table headers against expected values
    try:
        string_assertion(block.headers, test["headers"])
        test_case.headerResult = TestResult("SUCCESS")  # Set success if headers match
    except AssertionError:
        # Log failure if headers do not match and provide details
        test_case.headerResult = TestResult("FAILED", "Trailing table header test failed",
                                            test["headers"], block.headers)

    # Check the rows in the block against the expected values
    test_case.rowResult = check_row(block.rows, test)

    # Return the populated test case object
    return test_case


def check_row(rows, test_config):
    """
    Validates the data in each row of a table against expected values from the test configuration.

    This function iterates through the provided rows of a table and checks whether the actual
    row data matches the expected values defined in the test configuration. It returns a result
    indicating success or failure for each row validation.

    Parameters:
        rows (list): A list of row objects from the table, where each object contains actual data.
        test_config (dict): A dictionary containing the expected values for each row, keyed by
                            the row header.

    Returns:
        TestResult: An object indicating the overall result of the row validation. It contains
                    a status of "SUCCESS" or "FAILED", along with details of any failures.

    Raises:
        AssertionError: If the actual row data does not match the expected values, an assertion
                        error is raised, which is caught and used to create a failed TestResult.
    """

    # Iterate over each row in the provided list of rows
    for row in rows:
        try:
            # Validate that the row data matches the expected values from test_config
            assertion(row.rowData, list(test_config[row.rowHeader]))
        except AssertionError:
            # If an assertion fails, return a failed TestResult with details
            return TestResult("FAILED", f"{row.rowHeader} test failed for table",
                              list(test_config[row.rowHeader]), row.rowData)

    # If all rows pass validation, return a successful TestResult
    return TestResult("SUCCESS")


def check_cy_df_shape(test, wbr1):
    """
    Checks the shape of the current year (CY) monthly DataFrame against the expected length
    specified in the test configuration.

    This function retrieves the current year trailing twelve months DataFrame from the WBR object
    and compares the number of rows against the expected value provided in the test configuration.

    Parameters:
        test (dict): A dictionary containing test configuration details, including the expected
                     length of the CY monthly DataFrame.
        wbr1 (WBR): An instance of the WBR class containing the current year DataFrame to be checked.

    Returns:
        TestResult: An object indicating the result of the test. It contains a status of "SUCCESS"
                    or "FAILED", along with details of any failures.

    Raises:
        AssertionError: If the shape of the current year DataFrame does not match the expected
                        length, an assertion error is raised, which is caught and used to
                        create a failed TestResult.
    """

    # Retrieve the current year trailing twelve months DataFrame from the WBR object
    cy_monthly_df = wbr1.cy_trailing_twelve_months

    # Get the number of rows and columns in the DataFrame
    row_length, col_length = cy_monthly_df.shape

    try:
        # Assert that the number of rows matches the expected length from the test configuration
        assert row_length == test["cy_monthly_data_frame_length"]
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, return a failed TestResult with details
        return TestResult("FAILED", "cy data frame length test failed",
                          test["cy_monthly_data_frame_length"], row_length)


def check_py_df_shape(test, wbr1):
    """
    Checks the shape of the previous year (PY) monthly DataFrame against the expected length
    specified in the test configuration.

    This function retrieves the previous year trailing twelve months DataFrame from the WBR object
    and compares the number of rows against the expected value provided in the test configuration.

    Parameters:
        test (dict): A dictionary containing test configuration details, including the expected
                     length of the PY monthly DataFrame.
        wbr1 (WBR): An instance of the WBR class containing the previous year DataFrame to be checked.

    Returns:
        TestResult: An object indicating the result of the test. It contains a status of "SUCCESS"
                    or "FAILED", along with details of any failures.

    Raises:
        AssertionError: If the shape of the previous year DataFrame does not match the expected
                        length, an assertion error is raised, which is caught and used to
                        create a failed TestResult.
    """

    # Retrieve the previous year trailing twelve months DataFrame from the WBR object
    py_monthly_df = wbr1.py_trailing_twelve_months

    # Get the number of rows and columns in the DataFrame
    row_length, col_length = py_monthly_df.shape

    try:
        # Assert that the number of rows matches the expected length from the test configuration
        assert row_length == test["py_monthly_data_frame_length"]
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, return a failed TestResult with details
        return TestResult("FAILED", "py data frame length test failed",
                          test["py_monthly_data_frame_length"], row_length)


def py_validate_dataframe_length(wbr1: pandas.DataFrame, test):
    """
    Validates the length of the previous year (PY) DataFrame for a specific metric against the expected length
    specified in the test configuration.

    This function checks if the metric name contains certain keywords that indicate a special case (WOW, MOM, YOY).
    If not, it compares the actual length of the relevant PY DataFrame to the expected length.

    Parameters:
        wbr1 (pandas.DataFrame): The DataFrame containing the metrics for the previous year.
        test (dict): A dictionary containing test configuration details, including the expected length of the
                     PY DataFrame for the specified metric.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of "SUCCESS"
                    or "FAILED", along with details of any failures.

    Raises:
        AssertionError: If the length of the PY DataFrame does not match the expected value, an assertion error
                        is raised, which is caught to create a failed TestResult.
    """

    # Extract the metric name from the test configuration
    metric_name = test['metric_name']

    # If the metric name contains "WOW", "MOM", or "YOY", return a success result immediately
    if "WOW" in metric_name or "MOM" in metric_name or "YOY" in metric_name:
        return TestResult("SUCCESS")

    # Calculate the length of the relevant PY DataFrame, excluding the first 7 elements
    py_monthly_length = len(wbr1.metrics["PY__" + metric_name][7:])

    try:
        # Assert that the length of the PY DataFrame matches the expected length from the test configuration
        assert py_monthly_length == test["py_monthly_data_frame_length"]
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, return a failed TestResult with details
        return TestResult("FAILED", "py data frame length test failed",
                          test["py_monthly_data_frame_length"], py_monthly_length)


def cy_validate_dataframe_length(wbr1, test):
    """
    Validates the length of the current year (CY) DataFrame for a specific metric against the expected length
    specified in the test configuration.

    This function checks if the metric name contains certain keywords (WOW, MOM, YOY) that indicate special
    cases for length calculation. If not, it calculates the length based on the current year DataFrame.

    Parameters:
        wbr1: The WBR object containing the metrics for the current year.
        test (dict): A dictionary containing test configuration details, including the expected length of the
                     CY DataFrame for the specified metric.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of "SUCCESS"
                    or "FAILED", along with details of any failures.

    Raises:
        AssertionError: If the length of the CY DataFrame does not match the expected value, an assertion error
                        is raised, which is caught to create a failed TestResult.
    """

    # Extract the metric name from the test configuration
    metric_name = test['metric_name']

    # Determine the length of the CY DataFrame based on whether the metric name includes special keywords
    if "WOW" in metric_name or "MOM" in metric_name or "YOY" in metric_name:
        # For special cases, length is calculated excluding the first 7 elements
        cy_monthly_length = len(wbr1.metrics[metric_name][7:])
    else:
        # For regular metrics, calculate the length of the metric in the current year DataFrame
        cy_monthly_length = len(list(wbr1.cy_trailing_twelve_months[metric_name]))

    try:
        # Assert that the length of the CY DataFrame matches the expected length from the test configuration
        assert cy_monthly_length == test["cy_monthly_data_frame_length"]
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, return a failed TestResult with details
        return TestResult("FAILED", "cy data frame length test failed",
                          test["cy_monthly_data_frame_length"], cy_monthly_length)


def validate_axis(x_axis, test):
    """
    Validates the x-axis labels against expected values from the test configuration.

    This function removes any spaces from the x-axis labels and checks if they match the expected
    x-axis labels specified in the test configuration.

    Parameters:
        x_axis (list): A list of x-axis labels to be validated.
        test (dict): A dictionary containing test configuration details, including the expected x-axis labels.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of "SUCCESS"
                    or "FAILED", along with details of any failures.

    Raises:
        AssertionError: If the x-axis labels do not match the expected values, an assertion error
                        is raised, which is caught to create a failed TestResult.
    """

    # Remove any spaces from the x-axis labels
    x_axis.remove(' ')

    try:
        # Assert that the cleaned x-axis labels match the expected values in the test configuration
        string_assertion(x_axis, test["x_axis"])
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, return a failed TestResult with details
        return TestResult("FAILED", "axis label test failed", test["x_axis"], x_axis)


def validate_cy_six_weeks(metric_object, test):
    """
    Validates the current six weeks of data against expected values from the test configuration.

    This function extracts the six-week values from the provided metric object and compares them
    to the expected values specified in the test configuration. It returns the result of this
    validation.

    Parameters:
        metric_object: An object representing the metric, containing current data for validation.
        test (dict): A dictionary containing the expected six-week values for comparison.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of
                    "SUCCESS" or "FAILED", along with details of any discrepancies.

    Raises:
        Exception: If the 'current' attribute is missing from the metric object.
    """

    # Get the primary data for the current metric
    primary = metric_object.current[0] if metric_object.current is not None else Exception(
        "current missing from metric"
    )

    # Extract the first six weeks of primary axis data and replace strings with NaN where applicable
    six_week_values = replace_string_with_nan(primary["primaryAxis"][0:6])

    try:
        # Assert that the extracted six-week values match the expected values from the test configuration
        assertion(six_week_values, test["cy_6_weeks"])
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, replace NaN values with string representation and return a failed TestResult
        strings = replace_nan_with_string_nan(six_week_values)
        expected_strings = replace_nan_with_string_nan(test["cy_6_weeks"])
        return TestResult("FAILED", "cy six weeks test failed", expected_strings, strings)


def validate_py_six_weeks(metric_object, test):
    """
    Validates the previous year's six weeks of data against expected values from the test configuration.

    This function checks if the 'graph_prior_year_flag' is set to True or is missing, then extracts
    the six-week values from the previous year's metric object and compares them to the expected values
    specified in the test configuration. It returns the result of this validation.

    Parameters:
        metric_object: An object representing the metric, containing previous year data for validation.
        test (dict): A dictionary containing the expected six-week values for comparison.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of
                    "SUCCESS" or "FAILED", along with details of any discrepancies.

    Raises:
        Exception: If the 'previous' attribute is missing from the metric object or the
                    'graph_prior_year_flag' condition is not met.
    """

    # Check if the 'graph_prior_year_flag' is either not present or is set to True
    if 'graph_prior_year_flag' not in test or test['graph_prior_year_flag']:
        # Ensure that the previous data exists in the metric object
        if metric_object.previous is None:
            raise Exception("previous missing from metric")

        # Get the primary data for the previous year's metric
        primary = metric_object.previous[0]

        # Extract the first six weeks of primary axis data and replace strings with NaN where applicable
        six_week_values = replace_string_with_nan(primary["primaryAxis"][0:6])

        try:
            # Assert that the extracted six-week values match the expected values from the test configuration
            assertion(six_week_values, test["py_6_weeks"])
            return TestResult("SUCCESS")
        except AssertionError:
            # If the assertion fails, replace NaN values with string representation and return a failed TestResult
            strings = replace_nan_with_string_nan(six_week_values)
            expected_strings = replace_nan_with_string_nan(test["py_6_weeks"])
            return TestResult("FAILED", "py six weeks test failed", expected_strings, strings)


def validate_cy_twelve_months(metric_object, test):
    """
    Validates the current year's twelve months of data against expected values from the test configuration.

    This function retrieves the secondary axis values for the current year's metric object and
    compares them to the expected twelve-month values specified in the test configuration.
    It returns the result of this validation.

    Parameters:
        metric_object: An object representing the metric, containing current year data for validation.
        test (dict): A dictionary containing the expected twelve-month values for comparison.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of
                    "SUCCESS" or "FAILED", along with details of any discrepancies.

    Raises:
        Exception: If the 'current' attribute is missing from the metric object.
    """

    # Ensure that the current data exists in the metric object
    secondary = metric_object.current[1] if metric_object.current is not None else Exception(
        "current missing from metric")

    # Extract the twelve months of data from the secondary axis, starting from the eighth element
    twelve_month_values = replace_string_with_nan(secondary["secondaryAxis"][7:])

    try:
        # Assert that the extracted twelve-month values match the expected values from the test configuration
        assertion(twelve_month_values, test["cy_monthly"])
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, replace NaN values with string representation and return a failed TestResult
        strings = replace_nan_with_string_nan(twelve_month_values)
        expected_strings = replace_nan_with_string_nan(test["cy_monthly"])
        return TestResult("FAILED", "cy monthly test failed", expected_strings, strings)


def validate_py_twelve_months(metric_object, test):
    """
    Validates the prior year's twelve months of data against expected values from the test configuration.

    This function retrieves the secondary axis values for the previous year's metric object and
    compares them to the expected twelve-month values specified in the test configuration.
    It returns the result of this validation.

    Parameters:
        metric_object: An object representing the metric, containing prior year data for validation.
        test (dict): A dictionary containing the expected twelve-month values for comparison.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of
                    "SUCCESS" or "FAILED", along with details of any discrepancies.

    Raises:
        Exception: If the 'previous' attribute is missing from the metric object.
    """

    # Check if the test configuration allows for prior year validation
    if 'graph_prior_year_flag' not in test or test['graph_prior_year_flag']:
        # Ensure that the previous data exists in the metric object
        if metric_object.previous is None:
            raise Exception("previous missing from metric")

        # Retrieve the secondary axis values for the previous year's data
        secondary = metric_object.previous[1]

        # Extract the twelve months of data from the secondary axis, starting from the eighth element
        twelve_month_values = replace_string_with_nan(secondary["secondaryAxis"][7:])

        try:
            # Assert that the extracted twelve-month values match the expected values from the test configuration
            assertion(twelve_month_values, test["py_monthly"])
            return TestResult("SUCCESS")
        except AssertionError:
            # If the assertion fails, replace NaN values with string representation and return a failed TestResult
            strings = replace_nan_with_string_nan(twelve_month_values)
            expected_strings = replace_nan_with_string_nan(test["py_monthly"])
            return TestResult("FAILED", "py monthly test failed", expected_strings, strings)


def validate_summary_table_values(summary_data, test):
    """
    Validates the summary table values against expected box totals from the test configuration.

    This function processes the summary data by replacing specific values with NaN and compares the
    resulting data to the expected values specified in the test configuration. It returns the result
    of this validation.

    Parameters:
        summary_data: The summary data from the WBR, which needs to be validated against expected values.
        test (dict): A dictionary containing the expected box totals for comparison.

    Returns:
        TestResult: An object indicating the result of the validation. It contains a status of
                    "SUCCESS" or "FAILED", along with details of any discrepancies.
    """

    # Replace specific strings in summary_data with NaN for comparison
    summary_table_body = replace_string_with_nan(summary_data)

    try:
        # Assert that the processed summary table body matches the expected box totals from the test configuration
        assertion(summary_table_body, test["box_totals"])
        return TestResult("SUCCESS")
    except AssertionError:
        # If the assertion fails, replace NaN values with string representation and return a failed TestResult
        strings = replace_nan_with_string_nan(summary_table_body)
        expected_strings = replace_nan_with_string_nan(test["box_totals"])
        return TestResult("FAILED", "box total test failed", expected_strings, strings)


def string_assertion(real, expected):
    assert all([a == b for a, b in zip(real, expected)])


def assertion(real, expected):
    assert all([nearly_equal(round_off(a, 2), round_off(b, 2), 1)
                for a, b in zip(real, expected)])


def replace_nan_with_string_nan(actual_list):
    return [str(x) for x in actual_list]


def replace_string_with_nan(actual_list):
    return [x if not isinstance(x, str) else np.nan for x in actual_list]


def nearly_equal(a, b, sig_fig):
    if np.isnan(a) and not np.isnan(b):
        return False
    elif np.isnan(b) and not np.isnan(a):
        return False
    return a is b or int(a * 10 ** sig_fig) == int(b * 10 ** sig_fig) or round_off(a, 1) == round_off(b, 1)


def round_off(n, ndigits):
    if type(n) is float and np.isnan(n) or type(n) is str:
        return np.nan
    part = n * 10 ** ndigits
    delta = part - int(part)
    # always round 'away from 0'
    if delta >= 0.5 or -0.5 < delta <= 0:
        part = math.ceil(part)
    else:
        part = math.floor(part)
    return part / (10 ** ndigits) if ndigits >= 0 else part * 10 ** abs(ndigits)
