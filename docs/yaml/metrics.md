# metrics

The `metrics` section maps data columns to named metrics used in your
deck. Every metric displayed must be defined here first.

There are three ways to define a metric:

1. **Basic** — one-to-one column mapping
2. **Filter** — selective aggregation with a query
3. **Function** — derived from other metrics

## Basic metric

```yaml
metrics:
  Impressions:
    column: main_metrics.Impressions    # or just "Impressions" for CSV
    aggf: sum
    metric_comparison_method: %         # optional
```

| Field | Required | Values | Description |
|-------|----------|--------|-------------|
| `column` | Yes | String | Data column name. Namespaced as `query_name.column` for DB sources. |
| `aggf` | Yes | `sum`, `mean`, `min`, `max`, `last` | Aggregation function for daily → weekly/monthly rollup. |
| `metric_comparison_method` | No | `%`, `bps` | Format for auto-generated WOW/MOM/YOY comparisons. Default: `%`. |

### Choosing an aggregation function

| Use this | When values... | Example metrics |
|----------|----------------|-----------------|
| `sum` | Accumulate over time | Revenue, page views, orders, clicks |
| `mean` | Are rates or averages | Defects/million, conversion rate, DAU |
| `min` | You want the lowest | Min response time |
| `max` | You want the highest | Peak concurrent users |
| `last` | You want the most recent | Inventory level, account balance |

### Auto-generated growth rates

For every metric, the app auto-generates three derivatives:

| Suffix | Meaning |
|--------|---------|
| `{MetricName}YOY` | Year-over-year growth rate |
| `{MetricName}MOM` | Month-over-month growth rate |
| `{MetricName}WOW` | Week-over-week growth rate |

You do **not** define these — just reference them directly in your
[`deck`](deck.md):

```yaml
deck:
  - block:
      ui_type: 6_12Graph
      title: Page View YOY Growth
      y_scaling: "##.1%"
      metrics:
        PageViewsYOY:
          line_style: primary
          graph_prior_year_flag: false
```

## Filter metric

Selectively aggregate a column based on a condition.

```yaml
metrics:
  USRevenue:
    filter:
      base_column: "RevenueUSD"
      query: "Country == 'US'"
    aggf: sum
```

| Field | Description |
|-------|-------------|
| `base_column` | Column to aggregate |
| `query` | Pandas-style query. Operators: `==`, `!=`, `>`, `<`, `>=`, `<=` |

### Example: filtering by department

Given this data:

| Date | Department | Applicants |
|------|------------|------------|
| 15-Jan-2022 | Engineering | 45 |
| 15-Jan-2022 | Sales | 30 |

```yaml
metrics:
  ApplicantsEngineering:
    filter:
      base_column: "Applicants"
      query: "Department == 'Engineering'"
    aggf: sum

  ApplicantsSales:
    filter:
      base_column: "Applicants"
      query: "Department == 'Sales'"
    aggf: sum
```

## Function metric

Derive a new metric from other previously defined metrics.

```yaml
metrics:
  ClickThruRate:
    metric_comparison_method: bps
    function:
      divide:
        - metric:
            name: Clicks
        - metric:
            name: Impressions
```

Available functions: `sum`, `difference`, `divide`, `product`.

!!! note "Aggregation happens first"
    Each input metric is aggregated independently, then the function
    is applied. For example, `divide` divides the aggregated values,
    not row-by-row daily values.

### Variance to plan example

```yaml
metrics:
  PageViews:
    column: main_metrics.PageViews
    aggf: sum

  PageViews Target:
    column: main_metrics.PageViews__Target
    aggf: sum

  VarianceToPlan:
    function:
      difference:
        - metric:
            name: PageViews
        - metric:
            name: PageViews Target

  VarianceToPlanPct:
    function:
      divide:
        - metric:
            name: VarianceToPlan
        - metric:
            name: PageViews Target
```
