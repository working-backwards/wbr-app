const input = document.getElementById('submitter');
const dataInput = document.getElementById('data');
const dataInputLabel = document.getElementById('dataFileLabel');
const configInput = document.getElementById('config');
const configInputLabel = document.getElementById('configFileLabel');
const testTransformedInput = document.getElementById('test_transformed');
const download = document.getElementById('submitter-download');
const downloadData = document.getElementById('data1');
const jsonInput = document.getElementById('jsonFile');
const jsonInputButton = document.getElementById('json_submitter');
const initialWBRPage = document.getElementById('initial-wbr-page');
const leftNavBtn = document.getElementById("close-left-nav-btn");
const yaml_loader = document.getElementById("yaml_loader");
const docs_div = document.getElementById("docs");
var allCharts = [];
var passwordGlobal = '';
const chartsList = [];
const boxTotals = [];
const tablesList = [];

const markerMap = new Map([["primary", "circle"], ["secondary", "rect"], ["tertiary", "diamond"], ["quaternary", "triangle"]]);
const cyColorMap = new Map([["primary", "#3A2FDE"], ["secondary", "#5B75F6"], ["tertiary", "#799FF3"], ["quaternary", "#9BBDE3"], ["quinary", "#9BBDE3"]]);
const pyColorMap = new Map([["primary", "#DA5069"], ["secondary", "#ffd6dd"], ["tertiary", "#fad9df"], ["quaternary", "#fae1e5"], ["quinary", "#fff0f2"]]);
const labelMap = new Map([["MM", "M"], ["BB", "B"], ["KK", "K"], ["bps", "bps"], ["%", "%"]])

const env = window.env;
const domain = window.location.hostname;
var docsAnchorTag = document.createElement('a');
docsAnchorTag.href = "https://app.workingbackwards.com/docs/index.html";

var docsSpan = document.createElement('span');
docsSpan.style.cssText = "color:#B59154; font-size: larger;";
docsSpan.textContent = "Learn how to get the most of the WBR App >";
docsAnchorTag.appendChild(docsSpan);
docs_div.appendChild(docsAnchorTag);

var strongElementCSV = document.createElement("strong");
strongElementCSV.textContent = "Weekly Data ";

var smallElementCSV = document.createElement("small");
smallElementCSV.textContent = " CSV";

var strongElementYAML = document.createElement("strong");
strongElementYAML.textContent = "Configuration ";

var smallElementYAML = document.createElement("small");
smallElementYAML.textContent = " YAML";

download.addEventListener('click', () => {
    yaml_loader.style.display = "inline-block";
    downloadFile(downloadData.files[0]);
    clearInputFile(downloadData);
});

jsonInputButton.addEventListener('click', () => {
    let file = new Blob([jsonInput.files[0]], { type: 'application/json' });
    file.text()
        .then(value => {
            var data = JSON.parse(value);
            initialWBRPage.style.display = "none";
            allCharts = data;
            createDynamicElements();
            data.forEach(chart => drawCharts(chart));
        })
        .catch(error => {
            console.log("Something went wrong" + error);
        });
    clearInputFile(jsonInput);
});

document.getElementById('showPassword').onclick = function () {
    this.checked ? document.getElementById('password').type = "text" : document.getElementById('password').type = "password";
};

// add event listener
input.addEventListener('click', () => {
    uploadFile(dataInput.files[0], configInput.files[0]);
    clearInputFile(dataInput);
    clearInputFile(configInput);
    leftNavBtn.click();
    dataInputLabel.innerText = "";
    dataInputLabel.appendChild(strongElementCSV);
    dataInputLabel.appendChild(smallElementCSV);
    configInputLabel.innerText = "";
    configInputLabel.appendChild(strongElementYAML);
    configInputLabel.appendChild(smallElementYAML);
});

const uploadFile = async (data, config) => {
    const page_loader_div = document.createElement('div');
    page_loader_div.id = "page_loader_div";
    page_loader_div.className = "loader";
    document.getElementsByTagName('body')[0].appendChild(page_loader_div);

    const formdata = new FormData();
    formdata.append("configfile", config, "WBR-Sample-Dataset.yaml");
    formdata.append("csvfile", data, "WBR Sample Dataset.csv");

    const requestOptions = {
        method: 'POST',
        body: formdata,
        redirect: 'follow'
    };

    const fetchText = async () => {
        try {
            const response = await fetch("/get-wbr-metrics", requestOptions);
            const loaderDiv = document.getElementById("page_loader_div");
            if (response.status === 200) {
                if (loaderDiv) loaderDiv.remove();
                if (initialWBRPage) initialWBRPage.style.display = "none";
                const data = await response.json();
                allCharts.push(data);
                createDynamicElements();
                drawCharts(data);
            } else {
                if (initialWBRPage) initialWBRPage.style.display = "none";
                if (loaderDiv) loaderDiv.remove();
                const message = await response.json();
                const container_block = document.getElementById('charts');
                const deckDiv = document.createElement('div');
                const titleH2 = document.createElement('h2');
                titleH2.innerHTML = message && message.description;
                titleH2.className = "error-message";
                deckDiv.className = "deckset";
                deckDiv.appendChild(titleH2);
                container_block.appendChild(deckDiv);
            }
        } catch (error) {
            console.error('Fetch error:', error);
            const loaderDiv = document.getElementById("page_loader_div");
            if (loaderDiv) loaderDiv.remove();
            if (initialWBRPage) initialWBRPage.style.display = "none";
            // Optionally, show an error message on the page
            var container_block = document.getElementById('charts');
            var deckDiv = document.createElement('div');
            var titleH2 = document.createElement('h2');
            titleH2.innerHTML = "Failed to build the WBR deck!";
            titleH2.className = "error-message";
            deckDiv.className = "deckset";
            deckDiv.appendChild(titleH2);
            container_block.appendChild(deckDiv);
        }
    };

    fetchText();

}

function createDynamicElements() {
    const dynamicButtonDiv = document.createElement('div');
    dynamicButtonDiv.className = 'no-print dynamic-buttons';
    dynamicButtonDiv.id = "dynamic_buttons";

    createPublishButtonElement(dynamicButtonDiv);
    createJsonButtonElement(dynamicButtonDiv);

    document.getElementsByTagName('body')[0].appendChild(dynamicButtonDiv);
}

async function createJsonButtonElement(dynamicButtonDiv) {
    const jsonButton = document.createElement('button');
    jsonButton.className = 'no-print row btn json-download-button btn-info';
    jsonButton.textContent = "Download JSON";
    jsonButton.title = "Download a JSON version of the WBR deck based on the latest csv data and yaml config";
    jsonButton.id = 'json_download_button';

    try {
        const svgIcon = await fetchSvgIcon('/icons/json.svg');
        jsonButton.appendChild(document.createTextNode(' '));
        jsonButton.appendChild(svgIcon);
    } catch (error) {
        console.error('Error fetching SVG icon:', error);
    }

    dynamicButtonDiv.appendChild(jsonButton);

    jsonButton.addEventListener('click', () => {
        const blob = new Blob([JSON.stringify(allCharts, null, 2)], { type: 'application/json' });
        const a = document.createElement("a");
        a.href = window.URL.createObjectURL(blob);
        a.download = "wbr.json";
        a.click();
    });
}

async function createPublishButtonElement(dynamicButtonDiv) {
    const publishButton = document.createElement('button');
    publishButton.className = 'no-print row btn publish-button btn-info';
    publishButton.textContent = "Publish";
    publishButton.title = "Publish the report to a web, a link will be provided which can be then used to generate the same report without any config or data files";
    publishButton.id = 'publishToWeb';
    publishButton.dataset.target = "#publishModal";

    try {
        const svgIcon = await fetchSvgIcon('/icons/publish.svg');
        publishButton.appendChild(document.createTextNode(' '));
        publishButton.appendChild(svgIcon);
    } catch (error) {
        console.error('Error fetching SVG icon:', error);
    }

    dynamicButtonDiv.appendChild(publishButton);

    // Remaining event listener code unchanged
    const submitButton = document.getElementById('submit');
    const checkBox = document.getElementById("data0");
    const passText = document.getElementById("pass_text");
    const showPass = document.getElementById('showPassword');
    const showPassLabel = document.getElementById('showPassword_Text');
    const passwordField = document.getElementById("password");

    publishButton.addEventListener('click', () => {
        $("#publishModal").modal('toggle');
        passText.style.display = "none";
        showPass.style.display = "none";
        showPassLabel.style.display = "none";
        passwordField.style.display = "none";
        submitButton.disabled = false;
        passwordGlobal = "";
    });

    checkBox.addEventListener('change', () => {
        if (checkBox.checked) {
            passwordField.type = "password";
            submitButton.disabled = true;
            showPasswordFields(true);
            passwordField.addEventListener('input', () => {
                submitButton.disabled = passwordField.value === "";
                if (!submitButton.disabled) {
                    passwordGlobal = passwordField.value;
                }
            });
        } else {
            showPasswordFields(false);
            passwordField.value = "";
            submitButton.disabled = false;
            passwordGlobal = "";
        }
    });

    submitButton.addEventListener('click', async () => {
        const requestOptions = {
            method: 'POST',
            body: JSON.stringify(allCharts),
            redirect: 'follow',
            headers: {
                'Content-Type': 'application/json'
            }
        };

        const url = passwordGlobal === "" ? "/publish-wbr-report" : `/publish-protected-report?password=${passwordGlobal}`;

        try {
            const response = await fetch(url, requestOptions);
            if (response.ok) {
                const jsonResponse = await response.json();
                Swal.fire(swalConfigForCopyPopup(jsonResponse.path));
            } else {
                Swal.fire(swalConfigForFailurePopup("Failed to publish the report!"))
            }
        } catch (error) {
            console.error('Error publishing report:', error);
        }
    });
}

async function fetchSvgIcon(url) {
    const response = await fetch(url);
    const svgText = await response.text();
    const svgElement = document.createElement('div');
    svgElement.innerHTML = svgText;
    return svgElement.firstElementChild;
}

function showPasswordFields(show) {
    const elementsToToggle = [
        document.getElementById("pass_text"),
        document.getElementById("showPassword"),
        document.getElementById("showPassword_Text"),
        document.getElementById("password")
    ];

    elementsToToggle.forEach(element => {
        element.style.display = show ? "block" : "none";
    });
}

function swalConfigForCopyPopup(message) {
    return {
        title: "Copy this URL and share it with with those who need to view the WBR report.",
        confirmButtonText: "COPY",
        html: message,
        showCancelButton: true,
        cancelButtonColor: "#6c757d",
        didOpen: () => {
            Swal.getPopup().addEventListener('click', () => {
                navigator.clipboard.writeText(Swal.getHtmlContainer().innerText)
            })
        },
        allowOutsideClick: false,
        allowEscapeKey: false,
        preConfirm: () => {
            return false; // Prevent confirmed
        }
    }
}

function swalConfigForFailurePopup(message) {
    return {
        title: message,
        showConfirmButton: false,
        showCancelButton: true,
        cancelButtonColor: "#6c757d",
        cancelButtonText: "Close",
        allowOutsideClick: false,
        allowEscapeKey: false,
        preConfirm: () => {
            return false; // Prevent confirmed
        }
    }
}

function clearInputFile(f) {
    if (f !== undefined && f.value) {
        try {
            f.value = ''; //for IE11, latest Chrome/Firefox/Opera...
        } catch (err) { }

        if (f.value) { //for IE5 ~ IE10
            var form = document.createElement('form'),
                parentNode = f.parentNode, ref = f.nextSibling;
            form.appendChild(f);
            form.reset();
            parentNode.insertBefore(f, ref);
        }
    }
}

async function downloadFile(csvDataFile) {
    var formdata = new FormData();
    formdata.append("csvfile", csvDataFile, "WBR Sample Dataset.csv");

    var requestOptions = {
        method: 'POST',
        body: formdata,
    };
    try {
        fetch("/download_yaml", requestOptions)
            .then((res) => {
                yaml_loader.style.display = "none";
                return res.blob();
            })
            .then((data) => {
                var a = document.createElement("a");
                a.href = window.URL.createObjectURL(data);
                a.download = "wbr_config.yaml";
                a.click();
            });
    } catch (error) {
        console.error('There was a problem with the fetch operation:', error);
    }
};

function createTitle(data) {
    const titleDiv = document.createElement('div');
    titleDiv.className = 'titleDiv'; // Use a class instead of inline style
    const titleH2 = document.createElement('h3');
    titleH2.innerHTML = `${data.title} (Week Ending ${data.weekEnding})`;
    titleH2.className = 'title';
    titleDiv.appendChild(titleH2);
    return titleDiv;
}

function createMainDiv() {
    const maindiv = document.createElement('div');
    const counterDiv = document.createElement('div');
    counterDiv.className = 'counterdiv';
    maindiv.appendChild(counterDiv);
    return maindiv;
}

function createSection(subData, deckDiv) {
    const sectionDivId = `charDiv_${Math.random()}`;
    const sectionDiv = document.createElement('div');
    sectionDiv.id = sectionDivId;
    sectionDiv.className = 'sectionDiv';
    deckDiv.appendChild(sectionDiv);

    if (subData.plotStyle === 'section') {
        plotSection(sectionDivId, subData);
    } else {
        displayEmbeddedContent(sectionDivId, subData);
    }
}

function createChartBlock(subData, deckDiv, counter) {
    const chartId = `charDiv_${Math.random()}`;
    const block_to_insert = document.createElement('div');
    block_to_insert.id = chartId;
    block_to_insert.className = 'chartdiv';

    const table_block = document.createElement('div');
    table_block.id = `${chartId}_table`;
    table_block.className = 'tablediv';

    const maindiv = createMainDiv();
    maindiv.className = 'maindiv';
    maindiv.appendChild(block_to_insert);
    maindiv.appendChild(table_block);

    deckDiv.appendChild(maindiv);
    plotChart(chartId, subData, counter);
    createTable(`${chartId}_table`, subData);
}

function createSixWeeksTable(subData, deckDiv, tableId, counter) {
    const blockTableDivId = `charDiv_${Math.random()}`;
    const table_block_to_insert = document.createElement('div');
    table_block_to_insert.id = blockTableDivId;
    table_block_to_insert.className = 'blockTableDiv';

    const maindiv = createMainDiv();
    maindiv.className = 'maindivTable';
    maindiv.appendChild(table_block_to_insert);

    deckDiv.appendChild(maindiv);
    plotSixWeeksTable(blockTableDivId, subData, tableId, counter);
}

function createTwelveMonthsTable(subData, deckDiv, tableId, counter) {
    const blockTableDivId = `charDiv_${Math.random()}`;
    const table_block_to_insert = document.createElement('div');
    table_block_to_insert.id = blockTableDivId;
    table_block_to_insert.className = 'blockTableDiv';

    const maindiv = createMainDiv();
    maindiv.className = 'maindivTable';
    maindiv.appendChild(table_block_to_insert);

    deckDiv.appendChild(maindiv);
    plotTwelveMonthsTable(blockTableDivId, subData, tableId, counter);
}

function drawCharts(data) {
    const container_block = document.getElementById('charts');
    const deckDiv = document.createElement('div');
    deckDiv.className = 'deckset';

    const titleDiv = createTitle(data);
    deckDiv.appendChild(titleDiv);
    container_block.appendChild(deckDiv);

    let counter = data.blockStartingNumber;
    let tableId = 0;

    data.blocks.forEach((subData) => {
        switch (subData.plotStyle) {
            case 'section':
            case 'embedded_content':
                createSection(subData, deckDiv);
                break;
            case '6_12_chart':
                createChartBlock(subData, deckDiv, counter);
                counter++;
                break;
            case '6_week_table':
                tableId++;
                createSixWeeksTable(subData, deckDiv, tableId, counter);
                counter++;
                break;
            case '12_MonthsTable':
                tableId++;
                createTwelveMonthsTable(subData, deckDiv, tableId, counter);
                counter++;
                break;
        }
    });
}

function getLabel(mask, addLabel) {
    return addLabel ? labelMap.get(mask) : "";
}

function dataLabelFormatter(formatMask, value, addLabel) {
    let precision = 0
    let formatted = value;
    if (formatMask.includes(".")) {
        precision = formatMask.split(".")[1][0]
    }
    if (formatMask.includes("MM")) {
        formatted = ((value / 1000000).toFixed(precision)) + getLabel("MM", addLabel);
    } else if (formatMask.includes("BB")) {
        formatted = ((value / 1000000000).toFixed(precision)) + getLabel("BB", addLabel);
    } else if (formatMask.includes("KK")) {
        formatted = ((value / 1000).toFixed(precision)) + getLabel("KK", addLabel);
    } else if (formatMask.includes("bps")) {
        formatted = ((value * 10000).toFixed(precision)) + getLabel("bps", addLabel);
    } else if (formatMask.includes("%")) {
        formatted = ((value * 100).toFixed(precision)) + getLabel("%", addLabel);
    } else {
        formatted = value.toFixed(precision);
    }
    return formatted;
}

function createSeries(subData, seriesData, lineName, cyColor, pyColor, yAxisIndex = 0) {
    return [
        {
            name: `${lineName} - CY`,
            type: 'line',
            smooth: true,
            label: { show: true, formatter: params => dataLabelFormatter(subData.yScale, params.value) },
            color: cyColor,
            symbol: markerMap.get(seriesData.lineStyle),
            symbolSize: 8,
            data: seriesData && seriesData.metric && seriesData.metric.current[0] && seriesData.metric.current[0].primaryAxis,
            coordinateSystem: 'cartesian2d'
        },
        {
            name: `${lineName} - CY`,
            type: 'line',
            smooth: true,
            label: { show: true, formatter: params => dataLabelFormatter(subData.yScale, params.value) },
            data: seriesData && seriesData.metric && seriesData.metric.current[1] && seriesData.metric.current[1].secondaryAxis,
            color: cyColor,
            coordinateSystem: 'cartesian2d',
            yAxisIndex: yAxisIndex,
            symbol: markerMap.get(seriesData.lineStyle),
            symbolSize: 8
        },
        {
            name: `${lineName} - PY`,
            type: 'line',
            data: seriesData && seriesData.metric && seriesData.metric.previous[0] && seriesData.metric.previous[0].primaryAxis,
            color: pyColor,
            symbolSize: 8,
            itemStyle: { opacity: 0 },
            coordinateSystem: 'cartesian2d'
        },
        {
            name: `${lineName} - PY`,
            type: 'line',
            data: seriesData && seriesData.metric && seriesData.metric.previous[1] && seriesData.metric.previous[1].secondaryAxis,
            symbolSize: 8,
            itemStyle: { opacity: 0 },
            color: pyColor,
            coordinateSystem: 'cartesian2d',
            yAxisIndex: yAxisIndex,
        }
    ];
}

function createTargetSeries(seriesData, lineName, yAxisIndex = 0) {
    return [
        {
            name: lineName,
            type: 'scatter',
            data: seriesData && seriesData.Target && seriesData.Target.current[0] && seriesData.Target.current[0].primaryAxis,
            color: "green",
            tooltip: { formatter: params => `week: ${params.name}, value: ${params.value}` },
            symbol: 'triangle',
            symbolSize: 10,
            coordinateSystem: 'cartesian2d'
        },
        {
            name: lineName,
            type: 'scatter',
            data: seriesData && seriesData.Target && seriesData.Target.current[1] && seriesData.Target.current[1].secondaryAxis,
            symbol: 'triangle',
            symbolSize: 10,
            color: "green",
            coordinateSystem: 'cartesian2d',
            yAxisIndex: yAxisIndex,
        }
    ];
}

function extractSeriesData(seriesData, axisType, cyOrPy, index, metricType) {
    const data = seriesData && seriesData[metricType] && seriesData[metricType][axisType] && seriesData[metricType][axisType][index] &&
        seriesData[metricType][axisType][index][cyOrPy + "Axis"].filter(item => typeof item === 'number' && item !== undefined && !isNaN(item));
    return data ? data : [];
}

function setAxisOptions(axisData) {
    const minVal = Math.min(...axisData);
    const maxVal = Math.max(...axisData);
    const range = niceNum(maxVal - minVal, false);
    const tickSpacing = niceNum(range / 5, true);
    let min = Math.floor(minVal / tickSpacing) * tickSpacing;
    let max = Math.ceil(maxVal / tickSpacing) * tickSpacing;
    let interval = (max - min) / 5;

    if (minVal - min < interval * 0.10) {
        min -= interval;
        interval = (max - min) / 5;
    }
    if (max - maxVal < interval * 0.10) {
        max += interval;
        interval = (max - min) / 5;
    }

    return {
        min, max, interval
    };
}

function createYAxisOptions(subData, primaryOptions, secondaryOptions) {
    return [
        {
            splitNumber: 4,
            splitLine: { show: false },
            gridIndex: 0,
            axisLabel: {
                formatter: value => dataLabelFormatter(subData.yScale, value, true),
                color: '#058DC7'
            },
            position: 'left',
            type: 'value',
            min: primaryOptions.min,
            max: primaryOptions.max,
            interval: primaryOptions.interval
        },
        {
            splitNumber: 4,
            axisLabel: {
                formatter: value => dataLabelFormatter(subData.yScale, value, true),
                color: '#ED561B'
            },
            position: 'right',
            type: 'value',
            min: secondaryOptions.min,
            max: secondaryOptions.max,
            interval: secondaryOptions.interval
        }
    ];
}


function plotChart(divId, subData, counter) {
    var subseries = [];
    var primaryAxis = [];
    var secondayAxis = [];
    subData.yAxis.forEach(seriesData => {

        var lineName = seriesData.legendName;

        if (seriesData.lineStyle === undefined) {
            return;
        }

        if (seriesData.lineStyle !== "target") {

            if (subData.axes == 2) {
                primaryAxis.push(...extractSeriesData(seriesData, "current", "primary", 0, "metric"));
                primaryAxis.push(...extractSeriesData(seriesData, "previous", "primary", 0, "metric"));
                secondayAxis.push(...extractSeriesData(seriesData, "current", "secondary", 1, "metric"));
                secondayAxis.push(...extractSeriesData(seriesData, "previous", "secondary", 1, "metric"));
                subseries.push(createSeries(subData, seriesData, lineName, cyColorMap.get(seriesData.lineStyle), pyColorMap.get(seriesData.lineStyle), 1));
            }
            else if (subData.axes == 1) {
                primaryAxis.push(...extractSeriesData(seriesData, "current", "primary", 0, "metric"));
                primaryAxis.push(...extractSeriesData(seriesData, "previous", "primary", 0, "metric"));
                primaryAxis.push(...extractSeriesData(seriesData, "current", "secondary", 1, "metric"));
                primaryAxis.push(...extractSeriesData(seriesData, "previous", "secondary", 1, "metric"));
                subseries.push(createSeries(subData, seriesData, lineName, cyColorMap.get(seriesData.lineStyle), pyColorMap.get(seriesData.lineStyle), 0));
            }

        } else {

            if (subData.axes == 2) {
                primaryAxis.push(...extractSeriesData(seriesData, "current", "primary", 0, "Target"));
                secondayAxis.push(...extractSeriesData(seriesData, "current", "secondary", 1, "Target"));

                subseries.push(createTargetSeries(seriesData, lineName, 1));
            }
            else if (subData.axes == 1) {
                primaryAxis.push(...extractSeriesData(seriesData, "current", "primary", 0, "Target"));
                primaryAxis.push(...extractSeriesData(seriesData, "current", "secondary", 1, "Target"));

                subseries.push(createTargetSeries(seriesData, lineName, 0));
            }

        }
    });

    const primaryAllData = [].concat(...primaryAxis).filter(item => item !== undefined && !isNaN(item));
    const secondaryAllData = [].concat(...secondayAxis).filter(item => item !== undefined && !isNaN(item));

    const primaryOptions = setAxisOptions(primaryAllData);
    const secondaryOptions = setAxisOptions(secondaryAllData);

    const chartDom = document.getElementById(divId);
    const myChart = echarts.init(chartDom, null, { renderer: 'svg' });

    const resizeObserver = new ResizeObserver(() => {
        myChart.resize();
    });

    resizeObserver.observe(chartDom);

    const option = {
        title: {
            text: counter + ". " + subData.title,
            left: 'center'
        },
        tooltip: {
            show: (subData.tooltip == 'true'),
            formatter: function (params) {
                var tooltipValue = dataLabelFormatter(subData.yScale, params.value, true);
                return '<b>' + params.name + '<b>: ' + '<b>' + tooltipValue + '</b>';
            }
        },
        xAxis: {
            type: 'category',
            data: subData && subData.xAxis,
            axisLabel: {
                interval: 0,
                rotate: 30 //If the label names are too long you can manage this by rotating the label.
            },
            position: 'bottom',
            axisLine: {
                onZero: false
            }
        },
        yAxis: createYAxisOptions(subData, primaryOptions, secondaryOptions),
        legend: {
            left: 'center',
            top: 'bottom',
            itemWidth: 16,
            itemHeight: 10,
            itemGap: 4
        },
        series: subseries.flat(),
    }
    myChart.setOption(option);
}

function niceNum(range, round) {
    var exponent; /** exponent of range */
    var fraction; /** fractional part of range */
    var niceFraction; /** nice, rounded fraction */

    exponent = Math.floor(Math.log10(range));
    fraction = range / Math.pow(10, exponent);

    if (round) {
        if (fraction < 1.5)
            niceFraction = 1;
        else if (fraction < 3)
            niceFraction = 2;
        else if (fraction < 7)
            niceFraction = 5;
        else
            niceFraction = 10;
    } else {
        if (fraction <= 1)
            niceFraction = 1;
        else if (fraction <= 2)
            niceFraction = 2;
        else if (fraction <= 5)
            niceFraction = 5;
        else
            niceFraction = 10;
    }

    return niceFraction * Math.pow(10, exponent);
}

function createTable(dynamicTable, subData) {
    boxTotals.push(subData);

    const { tableHeader: header, tableBody: body } = subData.table;

    // Create a table element.
    const table = document.createElement("table");

    // Create table header row.
    const headerRow = table.insertRow(-1);
    header.forEach(headerText => {
        const th = document.createElement("th");
        th.innerHTML = headerText;
        th.style.width = '98px';
        headerRow.appendChild(th);
    });

    // Helper function to format cell data.
    const formatCellData = (data, index) => {
        if (data === "N/A") return data;

        const { yScale: formatMask, boxTotalScale: boxTotalMask } = subData;
        let precision = 0;
        if (formatMask.includes(".")) {
            precision = formatMask.split(".")[1][0];
        }

        if ((index === 1 || index % 2 === 0) && index !== 0) {
            if (boxTotalMask.includes("bps")) {
                return Math.round(data) + "bps";
            } else {
                return parseFloat(data).toFixed(precision) + "%";
            }
        } else {
            return dataLabelFormatter(formatMask, data, true);
        }
    };

    // Create table body rows.
    body.forEach(rowData => {
        const row = table.insertRow(-1);
        rowData.forEach((cellData, index) => {
            const cell = row.insertCell(-1);
            cell.innerHTML = formatCellData(cellData, index);
        });
    });

    // Add the newly created table to the container.
    const divShowData = document.getElementById(dynamicTable);
    divShowData.innerHTML = "";
    divShowData.appendChild(table);
}

// Function to format cell data
const formatCellData = (value, formatMask, precision) => {
    if (value === " ") return value;

    if (formatMask.includes("MM")) {
        return (value / 1000000).toFixed(precision) + "M"
    }
    else if (formatMask.includes("BB") || value >= 1E9 && value <= 1E12) {
        return (value / 1000000000).toFixed(precision) + "B"
    }
    else if (formatMask.includes("KK")) {
        return (value / 1000).toFixed(precision) + "K"
    }
    else if (formatMask.includes("bps")) {
        return (value * 10000).toFixed(precision) + "bps"
    }
    else if (formatMask.includes("%")) {
        return (value * 100).toFixed(precision) + "%"
    }
    else {
        return parseFloat(value).toFixed(precision)
    }
};

function plotSixWeeksTable(divId, subData, tableId, counter) {
    // Create table element
    const table = document.createElement('table');
    table.id = tableId;
    table.classList.add('table', 'table-sm');
    table.border = '1';

    // Create title row
    const titleRow = table.insertRow(-1);
    const titleTh = document.createElement('th');
    titleTh.colSpan = 10;
    titleTh.className = "tableTitle";
    titleTh.textContent = `${counter}. ${subData.title}`;
    titleRow.appendChild(titleTh);

    // Create header row
    const headerRow = table.insertRow(-1);
    headerRow.appendChild(document.createElement('th')); // Empty top-left corner cell
    subData.headers.forEach(headerText => {
        const th = document.createElement('th');
        th.textContent = headerText;
        headerRow.appendChild(th);
    });

    // Add data to tables list for later use
    tablesList.push(subData);

    // Create data rows
    subData.rows.forEach(row => {
        const dataRow = table.insertRow(-1);
        dataRow.classList.add('blockTable');

        // Row header
        const headerCell = document.createElement('td');
        headerCell.textContent = row.rowHeader;
        headerCell.style.cssText = row.rowStyle;
        dataRow.appendChild(headerCell);

        // Row data cells
        const formatMask = row.yScale;
        const precision = formatMask.includes(".") ? formatMask.split(".")[1][0] : 0;

        if (row.rowData.length === 0) {
            subData.headers.forEach(() => {
                const emptyCell = document.createElement('td');
                emptyCell.style.cssText = row.rowStyle;
                emptyCell.classList.add('rowData');
                emptyCell.textContent = "";
                dataRow.appendChild(emptyCell);
            });
        } else {
            row.rowData.forEach(cellData => {
                const dataCell = document.createElement('td');
                dataCell.classList.add('rowData');
                dataCell.textContent = formatCellData(cellData, formatMask, precision);
                dataRow.appendChild(dataCell);
            });
        }
    });

    // Append table to div
    const divShow = document.getElementById(divId);
    divShow.innerHTML = "";
    divShow.appendChild(table);

    // Adjust height if necessary
    if (divShow.offsetHeight < divShow.scrollHeight) {
        divShow.style.height = "100%";
    }
}

function displayEmbeddedContent(divId, subData) {
    const divShow = document.getElementById(divId);

    // Create iframe element
    const iframe = document.createElement("iframe");
    iframe.id = subData.id;
    iframe.src = subData.source;
    iframe.title = subData.title;
    iframe.setAttribute("aria-label", subData.title);
    iframe.setAttribute("scrolling", "yes");
    iframe.setAttribute("frameborder", "1");

    // Set width and height based on conditions
    const bodyWidth = document.body.clientWidth;
    const bodyHeight = document.body.clientHeight;

    iframe.setAttribute("width", subData.width > bodyWidth ? "100%" : subData.width);
    iframe.setAttribute("height", subData.height > bodyHeight ? bodyHeight : subData.height);

    // Append iframe to the specified div
    divShow.innerHTML = "";
    divShow.appendChild(iframe);
}

function plotTwelveMonthsTable(divId, subData, tableId, counter) {
    // Create table element
    var table = document.createElement('table');
    table.setAttribute("id", tableId);
    table.classList.add('table', 'table-sm');
    table.border = '1';

    // Create title row
    var titleTh = document.createElement('th');
    titleTh.colSpan = 13; // Adjusted colspan based on the number of columns
    titleTh.textContent = counter + ". " + subData.title;
    titleTh.className = "tableTitle";
    var titleTr = document.createElement('tr');
    titleTr.appendChild(titleTh);
    table.appendChild(titleTr);

    // Create header row
    var headerTr = document.createElement('tr');
    var emptyHeaderCell = document.createElement('th');
    headerTr.appendChild(emptyHeaderCell);

    subData.headers.forEach(function (headerText) {
        var th = document.createElement('th');
        th.textContent = headerText;
        headerTr.appendChild(th);
    });
    table.appendChild(headerTr);

    // Create data rows
    subData.rows.forEach(function (row) {
        var dataTr = document.createElement('tr');
        dataTr.className = 'blockTable';

        // Create row header cell
        var headerTd = document.createElement('td');
        headerTd.textContent = row.rowHeader;
        headerTd.style.cssText = row.rowStyle;
        dataTr.appendChild(headerTd);

        // Create data cells
        row.rowData.forEach(cellData => {
            var td = document.createElement('td');
            var formatMask = row.yScale;
            const precision = formatMask.includes(".") ? formatMask.split(".")[1][0] : 0;
            td.textContent = formatCellData(cellData, formatMask, precision);
            td.className = "rowData";
            dataTr.appendChild(td);
        });

        table.appendChild(dataTr);
    });

    // Append table to the specified div
    var divShow = document.getElementById(divId);
    divShow.innerHTML = "";
    tablesList.push(subData);
    divShow.appendChild(table);

    // Adjust div height if overflow
    if (divShow.offsetHeight < divShow.scrollHeight) {
        divShow.style.cssText = "height: 100%; !important";
    }
}


function plotSection(sectionDivId, subData) {
    var div = document.createElement('div');
    div.classList.add('section_div');

    var h3 = document.createElement('h3');
    var text = document.createTextNode(subData.title);
    h3.appendChild(text);

    div.appendChild(h3);

    var divShow = document.getElementById(sectionDivId);
    divShow.innerHTML = "";
    divShow.appendChild(div)
}
