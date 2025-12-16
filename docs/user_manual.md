# User Manual

## Introduction

Dive Deep is one of Amazon’s Leadership Principles[^1]. It states

> *Leaders operate at all levels, stay connected to the details, audit
> frequently, and are skeptical when metrics and anecdote differ. No
> task is beneath them.*

Jeff Bezos wanted every senior leader to stay connected to the details
of their business. So Amazon created a scalable and repeatable process
where teams can comprehensively answer the following questions with
actionable data:

1. What did our customers experience last week?

2. How did our business do last week?

3. Are we on track to hit our targets?

The process is called the Weekly Business Review (WBR). The WBR is a
carefully constructed metrics review process, where the right set of
input and output metrics, combined with a specialized user experience,
allows leaders to analyze hundreds of metrics within a relatively short
time. It remains one of Amazon’s secret weapons — an organizational
practice that has enabled them to get the little details right over a
period of decades.

The WBR App is a web application that takes your input and output metric
data and builds an HTML-based report called a WBR Deck, so that you may
implement an Amazon-style WBR process in your organization. The App is
designed for you to get up and running quickly; we expect that you will
move to a more automated process once you’ve gotten used to the WBR.

Like many Amazon processes, the WBR has a fractal quality to it. The WBR
App can be used by a small team, a departmental organization, or for the
entire company.

The WBR App won’t help you identify your input and output metrics. But
it should save you some time in creating an actionable WBR Deck once you
have the right set of metrics.

## Target Audience

The WBR App is targeted towards Business Intelligence team members,
Program Managers, Product Managers, and Data Analysts who want to use
the WBR App to prototype or build their WBR Decks.

In order to use the WBR App, you should be comfortable working with
metrics that come from your organization’s source data systems and
editing simple text configuration files that tell the WBR App how to
build the WBR Deck according to your needs. Once you have built your WBR
Deck, you can publish it to a URL and optionally password protect access
to the Deck.

## How It Works

You provide a configuration YAML file that describes your WBR deck. This
configuration specifies where your data comes from (databases and/or CSV
files), which metrics you want included, and how those metrics should be
aggregated and displayed. We'll describe exactly how to do this below.

If you wish to import the WBR data into your own data visualization
system such as Tableau or Looker, the WBR App also generates a JSON file
which you can use. JSON stands for JavaScript Object Notation. It’s a
standardized data format to transport data from one system to another
system.

Finally, this document assumes that the WBR App is hosted and reachable
via a web URL. This URL may be a public one or it may be restricted
within your organization. Contact us at Working Backwards if you’d like
more information on how to deploy the WBR App inside your corporate
network.

**Sounds simple. Why do we need the WBR App?**

Most data visualization applications cannot generate an Amazon-style WBR
Deck out of the box. The WBR App takes daily data as an input, and then
transforms it in three ways (weekly, monthly, and period-to-date) which
is required for Amazon-style WBR graphs and tables. The WBR App doesn’t
change any of your data. It just aggregates and stores the data in this
manner so that it is easier to construct your WBR Deck.

Let’s break down the two pieces of information you’ll need to provide to
the WBR App.

## Generate Your Daily Metrics Data

The WBR App expects daily metrics in a simple tabular shape:

| Date       | metric_one | metric_two | metric_three |
|------------|------------|------------|--------------|
| 2022-05-01 | 123        | 456,000    | 0.234        |
| 2022-05-02 | 321        | 654,100    | 0.235        |
| 2022-05-03 | 236        | 885,200    | 0.352        |

**Important:** the App is looking for data in the form  
`Date, metric_one, metric_two, metric_three, ...` — i.e., a `Date` value plus one or more metric columns per row. The
App can obtain these daily metrics **from database queries or from CSV files**. Below are the expectations, examples,
and the runtime behaviors to be aware of.

---

### Database queries (most common workflow)

- **Shape & requirement:** Each SQL query must return a column named **`Date`** (the loader requires `Date` and converts
  it to a datetime). The remaining columns in the query result become the metric columns. Typical SQL pattern:

  ```sql
  SELECT
    date AS "Date",
    SUM(sales_value) AS total_sales,
    COUNT(DISTINCT user_id) AS active_users
  FROM daily_sales_aggregates
  GROUP BY date;
  ```

* **Parsing & validation:** The loader converts `Date` to a pandas datetime and raises an error if `Date` is missing or
  cannot be parsed. Non-`Date` columns are treated as metric columns in the same way as CSV columns.

* **Multiple queries / merging:** You may define multiple named queries. The loader will load each query result, *
  *namespace non-`Date` columns by `queryName.ColumnName`**, and then **merge the results on `Date`** so all metrics
  align by date. Be mindful to alias columns in SQL to avoid name collisions.

* **Multiple rows per date:** Queries may return multiple rows for the same `Date`. The metric aggregation rules you
  define in the YAML (e.g., `sum`, `mean`, `last`) determine how those rows are combined.

---

### CSV files

* **Shape & requirement:** CSVs must follow the same tabular shape — **the first column must be named `Date`** and the
  remainder are metric columns:

  ```
  Date,metric_one,metric_two,metric_three
  2022-05-01,123,'456,000',0.234
  2022-05-02,321,'654,100',0.235
  2022-05-03,236,'885,200',0.352
  ```

* **Quoting thousands & special characters:** Numbers containing commas (e.g., `456,000`) must be quoted so they are
  parsed as a single field. If your metric names include punctuation (e.g., `Defects/Million`), quote them consistently
  in SQL and be aware they'll be preserved (and namespaced) by the loader.

* **Multiple rows per date / missing days:** CSVs may include multiple rows for a `Date` (e.g., per country);
  aggregation is controlled by your YAML. You do not need rows for every calendar day — missing dates are acceptable.
  For weekly metrics, include a single representative `Date` for the week.

---

### Two ways to use CSVs (key runtime behavior)

There are two distinct patterns for using CSV files, and they behave differently at run-time:

1. **Pass a CSV directly to the run (CSV override).**
   If you supply a CSV *directly* to a run (for example, uploading in the web UI or passing a `csv_data` stream via the
   API/CLI), the loader **uses that CSV as the sole data source for the run** and **ignores any data sources defined in
   the YAML** (both `data_sources` and top-level `csv_files`). This mode is useful for ad-hoc testing, prototyping, or
   temporary overrides.

2. **Reference CSVs in the configuration (merged sources).**
   If you want CSVs to be combined with database queries in the same run, list the CSVs in your configuration under the
   top-level `csv_files` section (a sibling to `data_sources`). The loader will then load DB query results and the
   configured CSV files, namespace non-`Date` columns by source/query, and **merge all sources on `Date`** so metrics
   from both CSVs and databases appear together in the deck.

**Note:** if you both (a) pass a CSV directly for a run and (b) also have `data_sources`/`csv_files` in the YAML, the
directly supplied CSV **wins** and the YAML data sources are ignored for that run. If you want DB + CSV merged in the
same run, do **not** pass a CSV directly — instead, put the CSV under `csv_files` in the config.

---

#### Example YAML (DB queries + CSV merged together)

```yaml
data_sources:
  MyProdPostgres:
    main_metrics:
      query: >
        SELECT
          date AS "Date",
          "Impressions", "Clicks", "PageViews"
        FROM wbr_sample_1;

    sales_metrics:
      query: >
        SELECT
          date AS "Date",
          "pct_customer_group_1", "pct_customer_group_2"
        FROM wbr_sample_1;

csv_files:
  external_metrics:
    url_or_path: ./data/external_metrics.csv
```

When the loader processes this config it will:

* run each query, expect a `Date` column, and convert `Date` to datetime;
* read each CSV listed under `csv_files`, expect a `Date` column;
* namespace non-`Date` columns (e.g., `main_metrics.PageViews`, `sales_metrics.pct_customer_group_1`,
  `external_metrics.PageViews`); and
* merge everything on `Date` to produce the daily table used by the deck.

---

### Practical tips

* **Always include a `Date` column** in every source (direct CSV, CSV in config, or DB query). The loader parses `Date`
  to pandas datetime.
* **Prefer clear aliases** in SQL (`AS "Date"`, `AS total_sales`) so the loader can validate and namespace columns
  predictably.
* **If you need temporary overrides**, pass a CSV directly. **If you need deterministic merging of multiple sources**,
  put CSVs in `csv_files` so they are merged with DB results.
* **If you combine sources**, remember the loader namespaces non-`Date` columns by `queryName.ColumnName` — reference
  those names in your `metrics` definitions or document how to map them for readability.

It is also possible to have multiple rows for each date. For instance:

| Date       | metric_one | metric_two | Country |
|------------|------------|------------|---------|
| 2022-05-01 | 123        | 456,000    | US      |
| 2022-05-01 | 498        | 231        | UK      |
| 2022-05-02 | 321        | 654,100    | US      |
| 2022-05-02 | 236        | 885,200    | US      |

Notice that for 2022-05-02, *both* rows are labeled ‘US’. This is valid
data — in this particular case, the WBR software will just aggregate the
data for that date according to an aggregation function (such as ‘sum’,
or ‘mean’) which you will define in your configuration file. We’ll cover
how to do that in the next section.

Finally you should note that:

- You do **not** need data for every day. It is perfectly ok to skip
  days.

- If you have a metric that is generated weekly, then you just need to
  put **one row in the CSV file with that metric** and associate it with
  a **single day within that week**.

Once you have your metric data ready, you may start writing
a configuration file to turn these raw columns into aggregations and
visualizations. Let’s take a look at that now.

## Define Your Configuration Using a YAML File

[YAML](https://yaml.org/) is a human-friendly data serialization
language for all programming languages.[^2] It is commonly used to
create configuration files. In order to generate your deck, you will
need to create a configuration file to define your metrics and lay out
your visualizations.

The WBR YAML configuration contains three main sections. The **setup**
section is the metadata about the deck (e.g. the title of the deck, the
time period the deck covers, etc ...). The **metrics** section is where
you define the metrics that will be used in the deck. A metric can be as
simple as mapping one-to-one to a column in the data you provided, or
you can define more complex transformations to generate new derived
metrics. Finally, the **deck** section defines how the data will be
displayed in the deck.

Putting it all together, the YAML format looks like this (do not worry
if you do not understand it right now — we’ll break it down in a bit):

```yaml
setup:
 week_ending: 25-SEP-2021
 week_number: 38
 title: WBR Daily
 fiscal_year_end_month: DEC 
 block_starting_number: 1  
 tooltip: true 
 
metrics:
 Impressions:
   column: Impressions
   aggf: sum
 
 Clicks:
   column: Clicks
   aggf: sum
 
 ClickThruRate:
   metric_comparison_method: bps
   function:
     divide:
       - metric:
           name: Clicks
       - metric:
           name: Impressions
 
 PageViews:
   column: "PageViews"
   aggf: sum
 
 PageViews Target:
   column: "PageViews__Target"
   aggf: sum
 
 MobilePage_Views:
   column: "MobilePageViews"
   aggf: sum
 
 VarianceToPlanPageViews:
   function:
     difference:
       - metric:
           name: PageViews
       - metric:
           name: PageViews Target
 
 PercentageVarianceToPlanPageViews:
   function:
     divide:
       - metric:
           name: VarianceToPlanPageViews
       - metric:
           name: PageViews Target


deck:
- block:
   ui_type: 6_12Graph
   title: Ad Impressions (Millions)
   y_scaling: "##.2MM"
   metrics:
     Impressions:
       line_style: primary
       graph_prior_year_flag: true

- block:
   ui_type: 6_12Graph
   title: Total Page Views (Millions)
   y_scaling: "##MM"
   metrics:
     PageViews:
       graph_prior_year_flag: true
       legend_name: Page Views
     PageViews Target:
       line_style: target
       graph_prior_year_flag: false
       legend_name: Page Views - Target
     MobilePage_Views:
       line_style: secondary
       graph_prior_year_flag: true
       legend_name: Mobile Page Views

- block:
   ui_type: section
   title: ""

- block:
   ui_type: 6_WeeksTable
   title: "Page Views Actual vs Plan Summary"
   rows:
     - row:
         header: "Page Views"
         style: "font-weight: bold; background-color: LightGrey; text-align:left;"
     - row:
         header: "Actual"
         metric: PageViews
         style: "text-align:right;"
         y_scaling: "##MM"
     - row:
         header: "Plan"
         metric: PageViews Target
         style: "text-align:right;"
         y_scaling: "##MM"
     - row:
         header: "Variance to Plan"
         metric: VarianceToPlanPageViews
         style: "text-align:right;"
         y_scaling: "##MM"
     - row:
         header: "Variance to Plan (%)"
         metric: PercentageVarianceToPlanPageViews
         style: "font-style: italic; text-align:right;"
         y_scaling: "##.1%"
     - row:
         header: "YOY"
         metric: PageViewsYOY
         style: "font-style: italic; text-align:right;"
         y_scaling: "##.1%"
     - row:
         header: "WOW"
         metric: PageViewsWOW
         style: "font-style: italic; text-align:right;"
         y_scaling: "##.1%"

- block:
   ui_type: 6_12Graph
   title: Page View YOY Growth Rate
   y_scaling: "##.1%"
   metrics:
     PageViewsYOY:
       line_style: primary
       graph_prior_year_flag: false
       legend_name: PV YOY
```

Let’s go through what these are.

As mentioned above, the YAML file is split into three parts:

1. The deck setup

2. The metrics definitions

3. The blocks

## The Deck Setup

The deck setup section is the first set of properties you will see in a
WBR App YAML file, denoted with the name **setup**.

```yaml
setup:
  week_ending: 25-SEP-2021
  week_number: 44
  title: WBR Daily
  fiscal_year_end_month: MAY
  block_starting_number: 2
  tooltip: true
```

- **week_ending** — (String, format: 1-JAN-2022) This date should mark
  the end of the latest week that is represented in the csv data. The
  WBR App interprets the 7 days prior to this date as a ‘week’.

- **week_number** — (Numeric) This will show up as the number of the
  latest week in the generated 6_12Graphs. Conceptually, this is the
  week number in the year (with a maximum week number of 52, since there
  are 52 weeks in a year).

![image](media/image10.png)

- **title** — (String) The title of your deck. Will be displayed at the
  top of the WBR deck.

- **fiscal_year_end_month** — (Three Letter Month Abbreviation) Set to
  the last month of fiscal year. If no value is provided, DEC (December)
  is used. You only need this entry if your organization’s fiscal and
  calendar year are different.

- **block_starting_number** — (Numeric) Each generated block will have a
  number on the top left corner of the block. The default value is 1, if
  no value is provided. Use this to change the starting number for your
  block labels. This is intended to be used if you need to upload
  multiple csv + yaml files for a single WBR Deck. See the **Uploading
  Multiple Files section** below.

- **tooltip** — (Boolean) This parameter will tell the WBR App whether
  to display numerical values of data points when the mouse hovers over
  a data point. The default value is false, if no value is provided.

## The Metrics Definitions

The metrics definitions section are where you’ll define metrics for use
in the rendered blocks in your WBR Deck.

Note that you **must** create metrics — no column in the CSV file will
be used in the WBR Deck if you do not first define it as a metric.

There are three different ways to create a metric: basic, function and
filter.

- The basic method is just a one-to-one mapping with a column in the CSV
  file.

- A filter allows you to aggregate certain rows in your CSV file, while
  ignoring others.

- A function is a metric that is based on one or more other metrics.

Let’s walk through what these actually look like.

### Basic

The simplest way to define a metric is with a one-to-one mapping to a
csv column.

```
MetricName:
  column: “CSVColumnName”
  metric_comparison_method: % / bps
  aggf: mean/sum/min/max/last
```

- **MetricName** — (String) This is the metric name that you will use
  when setting up the blocks. It is recommended that you use CamelCase.

- **column** — (String) The name must match the CSVColumn this metric
  will draw from.

- **metric_comparison_method** — (String, one of %/bps) Specifies
  whether the WoW and YoY variance metrics should be shown in percentage
  or bps terms. This calculation will be performed for the WOW and YOY
  calculations in the table at the bottom of the 6_12Graph as well as
  the WOW and YOY comparisons that are automatically generated for each
  metric that is automatically generated with the appended values of
  WOW, MOM, and YOY. If no parameter is specified, then variance metrics
  will be shown in percentage terms.

- **aggf** — (String; one of the values of mean/sum/min/max/last) This
  defines the aggregation function that will be used.

**What is an aggregation function?** The aggregation function tells the
WBR App which function to use when transforming the daily data values to
the weekly, monthly, month-to-date, quarter-to-date and year-to-date
transformations needed in a WBR Deck. The most common aggf functions
are:

- sum

- mean

- min

- max

- last

Let’s walk through an example to see this in action**.**

**Example**

```yaml
DefectsPerMillion:
  column: "Defects/Million"
  aggf: mean

Impressions:
  column: Impressions
  aggf: sum

```

The above metric definition defines two new metrics called
‘DefectsPerMillion’ and ‘Impressions’ that are drawn from the CSVColumn
titled ‘Defects/Million’ and ‘Impressions’ respectively. The WBR App
will aggregate the DefectsPerMillion metric by calculating the **mean**
for a few time periods: each week in the prior six weeks, each month in
the prior 12 months, and the month-to-date, quarter-to-date and
year-to-date time ranges. It will aggregate the Impressions metric by
calculating the **sum** for those same time ranges.

Why would you pick mean for one metric but sum for the other?

- The DefectsPerMillion metric lists the number of defective parts per
  million pieces manufactured for each day. We don’t want to sum this
  number up — it wouldn’t make sense! Instead, we report the mean value
  across the time period in question.

- By contrast, the Impressions metric tells us the number of ad
  impressions we get each day. In this case we want to know the *total*
  number of ad impressions for the time periods we’re looking at. In
  this case, it makes more sense to run a sum.

### Filters

Filters allow you to selectively aggregate a column, aggregating some
rows but ignoring others.

```
MetricName:
  filter:
    base_column: “CSVColumnName”
    query: “CSVColumn == ‘value’”
  aggf: mean/sum/min/max
```

- **MetricName** — (String) This is the metric name that you will use
  when setting up the blocks. It is recommended that you use CamelCase.

- **base_column** — (String) The name of the CSVColumn this metric will
  draw from.

- **query** — (String, format: “CSVColumn \<relation\> ‘value’”) This
  determines the query that will be used. Valid relations include: ==
  (equal), != (not equal), \> (greater than), \< (less than), \>=
  (greater than or equal), \<= (less than or equal).

- **aggf** — (String; one of the values of mean/sum/min/max) This
  defines the aggregation function that will be used.

**Example**

Suppose that you have the following data in your csv file:

| Date        | Country | RevenueUSD |
|-------------|---------|------------|
| 15-Jan-2022 | US      | 1500       |
| 15-Jan-2022 | JP      | 600        |
| 16-Jan-2022 | JP      | 850        |
| 16-Jan-2022 | US      | 1250       |

Three possible Metrics in this instance are

**TotalRevenue** = RevenueUSD aggregated by Date

**JPRevenue** = RevenueUSD aggregated by Date where Country = “JP”

**USRevenue** = RevenueUSD aggregated by Date where Country = “US”

Using a Filter allows you to express this, in the following manner:

```yaml
JPRevenue:
  filter:
    baseColumn: “RevenueUSD”
    query: “Country == ‘JP’”
  aggf: sum

USRevenue:
  filter:
    baseColumn: “RevenueUSD”
    query: “Country == ‘US’”
  aggf: sum
```

Of course, if you’d just like to generate TotalRevenue, which sums up
everything, you can express this using a One-to-One mapping:

```yaml
TotalRevenue:
  column: "RevenueUSD"
  aggf: sum
```

### Functions

Functions allow you to create new metrics from other previously defined
metrics.

```
MetricName: metric_comparison_method: % / bps
  function:
    sum/difference/divide/product:
      - metric:
          name: “Metric name”
      - metric:
          name: “Metric name”
```

- **MetricName** — (String) This is the metric name that you will use
  when setting up the blocks. It is recommended that you use CamelCase.

- **metric_comparison_method** — (String, one of % or bps) Specifies
  whether the WoW and YoY variance metrics should be shown in percentage
  or bps terms. This calculation will be performed for the WOW and YOY
  calculations in the table at the bottom of the 6_12Graph as well as
  the WOW and YOY comparisons that are automatically generated for each
  metric that is automatically generated with the appended values of
  WOW, MOM, and YOY. If no parameter is specified, then variance metrics
  will be shown in percentage terms.

- **sum/difference/divide/product** — (String) This is the function that
  would be applied to the two metrics or CSVColumns that are supplied
  below

- **name** — (String) This is the metric name that is defined previously
  in the YAML file.

**Examples**

The following Function Metric, MobileAndDesktopPageViews is the sum of
two other Metrics, MobilePage_Views and DesktopPageViews that have been
defined elsewhere in the YAML file.

```yaml
ClickThruRate:
  metric_comparison_method: bps
  function:
    divide:
      - metric:
          name: Clicks
      - metric:
          name: Impressions
```

**Important Note**

Conceptually, the right way to think about metrics created from a
function is that each metric is aggregated *first*, before the function
is applied. So, for instance, if you use division as a function on two
metrics, the WBR App will aggregate each metric *first* before dividing
one with the other.

## The Block Setup

Once you’ve set up your deck and defined your metrics, it’s time to
define how these metrics will be rendered on a webpage. A WBR Deck
consists of multiple Blocks. A block is basically a rendered square that
may contain one of three possible ui_types — a 6_12Graph, a 6WeeksTable,
and a 12MonthsTable.

### The 6_12Graph

A 6_12Graph is the most common WBR ui_type. It displays the trailing 6
weeks of data along with the trailing 12 months of data on the same
x-axis. It also has a summary table below the chart that lists the last
week, month-to-date, quarter-to-date, and year-to-date data along with
the relevant period-over-over-period comparisons expressed as a
percentage.

It looks like this:

![image](media/image12.png)

A 6WeeksTable displays the trailing 6 weeks of data along with the
quarter-to-date and year-to-date data for metrics. You will need to
define each row in the table. A row can contain metrics or text for
headers and blank lines.

It looks like this:

![image](media/image13.png)

A 12MonthsTable displays the trailing 12 months of data. The
6_WeeksTable and 12_MonthsTable work best when displayed together
side-by-side.

It looks like this:

![image](media/image15.png)

#### Rendering a 6_12Graph

The configuration for a 6_12Graph looks like this:

```
- block:
    ui_type: 6_12Graph
    title: “Text to be displayed at top of Block”
    y_scaling: “##(.0-3)\[BB\|MM\|KK\|%\]”
    x_axis_monthly_display: trailing_twelve_months / fiscal_year
    metrics:
      MetricName:
        line_style: primary/secondary/target
        graph_prior_year_flag: true/false
        legend_name: “Name displayed as legend at bottom”
      MetricName2 (optional):
        line_style: primary/secondary/target
        graph_prior_year_flag: true/false
        legend_name: “Name displayed as legend at bottom”
      MetricName3 (optional):
        line_style: primary/secondary/target
        graph_prior_year_flag: true/false
        legend_name: “Name displayed as legend at bottom”
```

A block must have at least one Metric, and may have multiple Metrics
displayed. We recommend no more than three — one with a primary
line_style, one with a secondary line_style, and one target.

- **ui_type** — (String, 6_12Graph) This is the format for the
  6_12Graph.

- **title** — (String) This is the text that will be displayed at the
  top of the Block

- **y_scaling** — (String, \##(.0-3)\[BB\|MM\|KK\|%\|bps\]) This is the
  scaling measure on the y axis of the 6_12Graph. Only billions,
  millions, thousands, and percentages are allowed. So, for instance,
  \##MM will turn numbers like ‘2,321,432’ into ‘2M’, ‘3,232,512’ into
  ‘3M’, and so on. See the section titled **y_scaling Formats** for more
  details.

- **x_axis_monthly_display** — (String of trailing_twelve_months
  /fiscal_year) Specifies whether the x_axis for the monthly data points
  will be the trailing_twelve_months or the fiscal_year. If no
  x_axis_monthly_display parameter is specified, then the
  trailing_twelve_months will be used.

- **line_style** — (String, one of primary/secondary/target) The
  line_style that the metric will be drawn in. Use primary for the main
  metric you want to showcase, secondary for a less prominent line, and
  target to display the intended targets for this metric.

- **graph_prior_year_flag** — (Boolean, true/false) Plots data from a
  year ago on both the 6 week plot and the 12 month plot.

- **legend_name** — (String) The name to be displayed in the legends
  section at the bottom of the chart.

### The 6_WeeksTable

The configuration for a 6_WeeksTable looks like this:

```
- block:
    ui_type: 6_WeeksTable
    title: "Title of Block"
    rows:
      - row:
          header: "Title of Section Header"
          style: "\<valid CSS\>"
      - row:
          header: "Title of Row Header"
          metric: MetricName
          style: "\<valid CSS\>"
          y_scaling: “##(.0-3)\[BB\|MM\|KK\|%\|bps\]”
      - row:
          header: "Title of Row Header"
          metric: MetricName2
          style: "\<valid CSS\>"
          y_scaling: “##(.0-3)\[BB\|MM\|KK\|%\|bps\]”
      - row:
          header: "Title of Row Header"
          metric: MetricName3
          style: “\<valid CSS\>”
          y_scaling: “##(.0-3)\[BB\|MM\|KK\|%\|bps\]”
```

- **ui_type** — (String, 6_WeeksTable) This is the format for the
  6WeeksTable.

- **title** — (String) This is the text that will be displayed at the
  top of the Block

- **header** — (String) Title of the row header.

- **metric** — (String, must be name of valid Metric) The Metric name
  that is to be displayed in this row

- **y_scaling** — (String,
  \##(.0-3)\[BB\|MM\|KK\|%\|bps\]) This is the scaling measure on the y
  axis of the 6_12Graph. Only billions, millions, thousands, percentages
  and bps are allowed. So, for instance, \##MM will turn numbers like
  ‘2,321,432’ into ‘2M’, ‘3,232,512’ into ‘3M’, and so on. See the
  section titled **y_scaling Formats** for more details.

- **style** — (String, valid CSS properties)
  [CSS properties](https://developer.mozilla.org/en-US/docs/Web/CSS) that
  apply to the header in this row. For instance, font-style: italic;
  text-align:right; causes the header to be italicized and aligned right
  in the cell.

Note that you can create a section header or break by writing:

```
- row:
    header: "Title of Section Header"
    style: "\<valid CSS\>"
```

Leaving the header property blank will create an empty row to serve as a
section break instead.

### The 12_MonthsTable

The configuration for a 12_MonthsTable looks like this:

```
- block:
    ui_type: 12_MonthsTable
    title: "Title of Block"
    x_axis_monthly_display: trailing_twelve_months / fiscal_year
    rows:
      - row:
          header: "Title of Section Header"
          style: "\<valid CSS\>"
      - row:
          header: "Title of Row Header"
          metric: MetricName
          style: "\<valid CSS properties\>"
          y_scaling: “##(.0-3)\[BB\|MM\|KK\|%\|bps\]”
      - row:
          header: "Title of Row Header"
          metric: MetricName2
          style: "\<valid CSS\>"
          y_scaling: “##(.0-3)\[BB\|MM\|KK\|%\|bps\]”
      - row:
          header: "Title of Row Header"
          metric: MetricName3
          style: "\<valid CSS\>"
          y_scaling: “##(.0-3)\[BB\|MM\|KK\|%\|bps\]”
```

- **ui_type** — (String, 12_MonthsTable) This is the format for the
  12MonthsTable.

- **title** — (String) This is the text that will be displayed at the
  top of the Block

- **header** — (String) Title of the row header.

- **metric** — (String, must be name of valid Metric) The Metric name
  that is to be displayed in this row

- **y_scaling** — (Optional String,
  \##(.0-3)\[BB\|MM\|KK\|%\|bps\]) This is the scaling measure on the y
  axis of the 6_12Graph. Only billions, millions, thousands, percentages
  and bps are allowed. So, for instance, \##MM will turn numbers like
  ‘2,321,432’ into ‘2M’, ‘3,232,512’ into ‘3M’, and so on. See the
  section titled **y_scaling Formats** for more details.

- **x_axis_monthly_display** — (Boolean) Specifies whether the x_axis
  for the monthly data points will be the trailing_twelve_months or the
  fiscal_year. If no x_axis_monthly_display parameter is specified, then
  the trailing_twelve_months will be used.

- **style** — (String, valid CSS properties, optional)
  [CSS properties](https://developer.mozilla.org/en-US/docs/Web/CSS) that
  apply to the header in this row. For instance, font-style: italic;
  text-align:right; causes the header to be italicized and aligned right
  in the cell.

### The Section Break

You may render a section break that splits the Deck up by writing the
following YAML:

```
- block:
    ui_type: section
    title: ""
```

You may leave the title property blank if you’d like a plain section
break. Alternatively, you may add a title to render a section header.

### Embed Content from a Third Party Source in Your WBR Deck

The embedded_content ui_type can be useful in cases where you want your
WBR Deck to display data from a different data source from your CSV file
or you want to use a different visual display (ui_type) that is not
native to the WBR App.

```
- block:
    ui_type: embedded_content
    source: "enter-your-url-here"
    height:: 700px
    width: 2000px
```

The content generated by the URL will be displayed in an iframe with the
specified pixel height and width. You can embed content such as static
images or dynamic data from a system like

Looker, Google Sheets, Excel, or Tableau.

### y_scaling Formats

The general format for y_scaling is “##(.0-3)\[BB\|MM\|KK\|%\|bps\]”

Let’s go through each option.

- \##(.n)BB — divides the number by 1 billion, and displays n decimal
  places. Examples:

    - “##.2BB” and 1,263,7800,000 becomes ‘1.26B’

    - “##.1BB” and 1,263,7800,000 becomes ‘1.3B’

    - “##BB” and 1,263,7800,000 becomes ‘1B’

- \##(.n)MM — divides the number by 1 million, and displays n decimal
  places. Examples:

    - “##.2MM” and 1,263,7800 becomes ‘1.26M’

    - “##.1MM” and 1,263,7800 becomes ‘1.3M’

    - “##MM” and 1,263,7800 becomes ‘1M’

- \##(.n)KK — divides the number by 1 thousand, and displays n decimal
  places.

    - “##.2KK” and 1,263 becomes ‘1.26K’

    - “##.1KK” and 1,263 becomes ‘1.3K’

    - “##KK” and 1,263 becomes ‘1K’

- \##(.n)% — multiples the number by 100, then displays n decimal
  places.

    - “##.2%” and 0.0264 becomes ‘2.64%’

    - “##.1%” and 0.0264 becomes ‘2.6%’

    - “##%” and 0.0264 becomes ‘3%’

- \##(.n)bps - multiplies the number by 10,000

    - “##.2bps” and 0.026378 becomes 263.78bps

    - “##.1bps” and 0.026378 becomes 263.8bps

    - “##bps” and 0.026378 becomes 264bps

## Uploading Multiple Files

The WBR App allows you to upload multiple pairs of YAML configuration
files and daily metrics CSV files. To do so:

1. Upload the first pair of YAML and CSV files.

2. The WBR App will generate a first set of blocks.

3. Upload the second set of YAML configuration file and CSV daily
   metrics data file.

4. The WBR App will *append* the second set of blocks to the bottom of
   the first set.

5. Rinse and repeat.

6. When done, print to PDF.

In this manner, you can have different departments in your company
submit different daily metrics data files at the end of each week, which
you may then combine into one WBR Deck.

If you’d like to have block numbers run continuously throughout the
entire deck, be sure to edit the block_starting_number on subsequent
YAML files. So for instance, if the first set of YAML file and CSV data
file renders 8 blocks, set block_starting_number to 9 in the second YAML
file you upload, in order to have the next set of blocks numbered 9
onwards.

## Adding YOY, MOM, WOW as Blocks

If you append ‘YOY’, ‘MOM’, or ‘WOW’ to the end of any metric name,
you’ll get the growth rate for that particular metric.

So, for instance, let’s say that you have the following metric named
PageViews:

```yaml
metrics:
  PageViews:
    column: "PageViews"
    aggf: sum
```

Behind the scenes, the WBR App generates PageViewsYOY, PageViewsMOM, and
PageViewsWOW as derivative metrics. Therefore, you may use these metrics
without defining them to render a block with PageViews’s growth rate:

```yaml
deck:
  - block:
      ui_type: 6_12Graph
      title: Page View YOY Growth Rate
      y_scaling: "##.1%"
      metrics:
        PageViewsYOY:
          line_style: primary
          graph_prior_year_flag: false
          legend_name: PV YOY
```

The above YAML renders the following block:

![image](media/image14.png)

Using this approach, you may render the growth rates of **any** of your
metrics.

##   

## The Admin User Interface

When you arrive at the WBR admin home page, click on the triple bar
“hamburger” menu button to view the actions you can take.

![image](media/image5.png)

### View Sample Reference Files

![image](media/image3.png)

To become familiar with the WBR APP, you can download two sets of CSV
and YAML files. The CSV files have some sample daily data. The YAML
files contain all of the configuration information in order to build two
sample WBR Decks. The YAML files have comments throughout each section
to demonstrate various features of the WBR App. A WBR Deck for a typical
business will be significantly larger. But these sample files should
contain enough information to help you figure out how to build your own
WBR Deck.

### Upload Your CSV and YAML Files and Generate Your WBR Deck

![image](media/image7.png)

This menu option will be the one you use most often. You select your CSV
and YAML files from your local drive and click on the GENERATE REPORT
button. The WBR App reads these files and renders a WBR Deck. If the WBR
App encounters and error, it will display a message to help you debug
the data or configuration.

### Get a Head Start - Auto-Generate Your YAML File

![image](media/image7.png)

Writing a YAML file from scratch when you have dozens of metrics can be
daunting. That’s why we created a feature that helps you get started on
building a new YAML configuration file. To access it, click the menu
button on the top left corner of the WBR App and select "Generate YAML".

You'll see the following popup:

![image](media/image9.png)

Upload your CSV file, and the WBR App will generate a sample YAML file
for you to modify.

The WBR App assumes that each metric column in your CSV file maps
directly to a single WBR metric. It also assumes that each metric will
be rendered with a 6_12Graph. You can then take this file and modify it
with any changes you need, which is much quicker than hand-crafting a
YAML file from scratch.

### Upload Your JSON File

![image](media/image11.png)

## Publishing and Exporting the Deck

Once you have successfully generated a WBR Deck (by uploading a CSV and
YAML file pair), green ‘JSON’ and ‘PUBLISH’ buttons will appear on the
top right hand portion of your screen.

![image](media/image2.png)

### Publish Your Deck

If you want to have a static URL that other people can use to view the
WBR Deck you created, then this feature is for you. Click on the PUBLISH
button.

![image](media/image6.png)

### Save Your Deck to JSON

If you want to create a WBR Deck in another visualization tool such as
Tableau or Looker, then the JSON feature is for you.

Clicking the JSON button will automatically download a JSON
representation of your WBR DEck. You can then use this JSON file as
source data for import into your data visualization tool of choice.

If you’d like to require a password before anyone can see the WBR Deck,
then check the “Click the checkbox to require a password” box. If not,
leave it unchecked and click the blue PUBLISH.

If you did check the password box, then enter whatever password you’d
like and then click the blue PUBLISH button.

![image](media/image1.png)

If you did not check the password box, skip this step.

Once you click on the PUBLISH button, a URL will be automatically
generated with a similar message like this one.

![image](media/image4.png)

Click on the blue COPY button and then send this URL, along with a
password if you require one, to customers who you wish to view the WBR
Deck. The people who click on the URL will only see the WBR Deck. They
will not see any of the admin functionality we discuss above.

## Wrapping Up

You now know how to use the WBR App to create your own WBR Deck.

For more information about the WBR, read Chapter 6 of *Working
Backwards*.

[^1]: https://www.amazon.jobs/en/principles

[^2]: https://yaml.org/
