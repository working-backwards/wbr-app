var totalTests;
var totalPassed;
var totalFailed;
var unit_test_result;

const testInput = document.getElementById('execute-tests');

testInput.addEventListener('click', () => {
    page_loader_div = document.createElement( 'div' );
    page_loader_div.id = "page_loader_div";
    page_loader_div.className = "loader";
    document.getElementsByTagName('body')[0].appendChild(page_loader_div);

    fetchText();
});

async function fetchText() {
    var requestOptions = {
        method: 'GET',
        redirect: 'follow'
    };
    var response = await fetch("/wbr-unit-test", requestOptions);
    if (response.status === 200) {
        document.getElementById("page_loader_div").remove();
        var textArea = document.getElementById("test_result");
        var data = await response.json();
        totalTests = 0;
        totalPassed = 0;
        totalFailed = 0;
        unit_test_result = "Result of the Test\n\n";
        unit_test_result += "---------------------------------------\n";
        data.scenarios.forEach(function(scenario) {
            build_scenario(scenario);
        });
        unit_test_result += "\nTotal Test cases: " + totalTests;
        unit_test_result += "\nTotal Failed: " + totalFailed;
        unit_test_result += "\nTotal Passed: " + totalPassed;
        document.getElementById('test_result').value = unit_test_result;
    }
    else {
        document.getElementById("page_loader_div").remove();
        textArea.innerHTML = "Tests failed! below are the errors \n" + data.message +
        "\n Please report this bug with files uploaded and result at developer@workingbackwards.com";
    }
}

function build_scenario(scenario) {
    unit_test_result += "SCENARIO: " + scenario.scenario + "\n";
    unit_test_result += "Fiscal Month: " + scenario.fiscalMonth + "\n";
    unit_test_result += "Week Ending: " + scenario.weekEnding + "\n";
    unit_test_result += scenario.scenario + " test result -->\n\n"
    scenario.testCases.forEach(function(test) {
        each_scenario(test);
    });
    unit_test_result += "---------------------------------------\n";
}

function each_scenario(test) {
    unit_test_result += "Test: " + test.testNumber + "\n";
    let testFailCount = 0;
    unit_test_result += "CY Data Frame length test result " + test.cyDataframeLength.result + "\n";
    if (test.cyDataframeLength.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.cyDataframeLength.expected + "\n";
        unit_test_result += "Calculated: " + test.cyDataframeLength.calculated + "\n\n";
        testFailCount += 1;
    }
    unit_test_result += "PY Data Frame length test result " + test.pyDataframeLength.result + "\n";
    if (test.pyDataframeLength.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.pyDataframeLength.expected + "\n";
        unit_test_result += "Calculated: " + test.pyDataframeLength.calculated + "\n\n";
        testFailCount += 1;
    }

    if (test.blockType == "SixTwelveChart") {
        sixTwelve(test) ? testFailCount += 1 : testFailCount;
    }
    
    if (test.blockType == "TrailingTable") {
        trailingTable(test);
    }

    if (testFailCount > 0) {
        totalFailed += 1;
    } else {
        totalPassed += 1;
    }

    totalTests += 1;
    unit_test_result += "\n";
}

function sixTwelve(test) {
    let isFailed = false;
    unit_test_result += "CY Six week test result: " + test.cySixWeekTestResult.result + "\n";
    if (test.cySixWeekTestResult.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.cySixWeekTestResult.expected + "\n";
        unit_test_result += "Calculated: " + test.cySixWeekTestResult.calculated + "\n\n";
        isFailed = true;
    }
    unit_test_result += "CY Twelve months test result: " + test.cyTwelveMonthTestResult.result + "\n";
    if (test.cyTwelveMonthTestResult.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.cyTwelveMonthTestResult.expected + "\n";
        unit_test_result += "Calculated: " + test.cyTwelveMonthTestResult.calculated + "\n\n";
        isFailed = true;
    }
    if (test.pySixWeekTestResult !== undefined && test.pySixWeekTestResult !== null) {
        unit_test_result += "PY Six week test result: " + test.pySixWeekTestResult.result + "\n";
        if (test.pySixWeekTestResult.result == "FAILED") {
            unit_test_result += "\nExpected: " + test.pySixWeekTestResult.expected + "\n";
            unit_test_result += "Calculated: " + test.pySixWeekTestResult.calculated + "\n\n";
            isFailed = true;
        }
    }
    if (test.pyTwelveMonthTestResult !== undefined && test.pyTwelveMonthTestResult !== null) {
        unit_test_result += "PY Twelve months test result: " + test.pyTwelveMonthTestResult.result + "\n";
        if (test.pyTwelveMonthTestResult.result == "FAILED") {
            unit_test_result += "\nExpected: " + test.pyTwelveMonthTestResult.expected + "\n";
            unit_test_result += "Calculated: " + test.pyTwelveMonthTestResult.calculated + "\n\n";
            isFailed = true;
        }
    }
    unit_test_result += "Summary table test result: " + test.summaryResult.result + "\n";
    if (test.summaryResult.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.summaryResult.expected + "\n";
        unit_test_result += "Calculated: " + test.summaryResult.calculated + "\n\n";
        isFailed = true;
    }
    unit_test_result += "X-axis labels test result: " + test.xAxis.result + "\n";
    if (test.xAxis.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.xAxis.expected + "\n";
        unit_test_result += "Calculated: " + test.xAxis.calculated + "\n\n";
        isFailed = true;
    }
    return isFailed;
}

function trailingTable(test) {
    let isFailed = false;
    unit_test_result += "Header label test result: " + test.headerResult.result + "\n";
    if (test.headerResult.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.headerResult.expected + "\n";
        unit_test_result += "Calculated: " + test.headerResult.calculated + "\n\n";
        isFailed = true;
    }
    unit_test_result += "Row data test result: " + test.rowResult.result + "\n";
    if (test.rowResult.result == "FAILED") {
        unit_test_result += "\nExpected: " + test.rowResult.expected + "\n";
        unit_test_result += "Calculated: " + test.rowResult.calculated + "\n\n";
        isFailed = true;
    }
}

