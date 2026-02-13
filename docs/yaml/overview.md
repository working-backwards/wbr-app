# YAML Configuration

The WBR App is driven by a single YAML configuration file. It has five
top-level sections, and they appear in this order:

```yaml
setup:          # Required
data_sources:   # Optional (if uploading CSV directly)
annotations:    # Optional
metrics:        # Required
deck:           # Required
```

| Section | Purpose | Required |
|---------|---------|----------|
| [`setup`](setup.md) | Deck metadata: title, dates, fiscal year, display options | Yes |
| [`data_sources`](data-sources.md) | Database connections, SQL queries, and CSV file references | No (if uploading CSV directly) |
| [`annotations`](annotations.md) | Business events overlaid on charts | No |
| [`metrics`](metrics.md) | Metric definitions: column mappings, filters, functions | Yes |
| [`deck`](deck.md) | Visual blocks: charts, tables, sections, embeds | Yes |

## Complete example

```yaml
setup:
  week_ending: 25-SEP-2021
  week_number: 38
  title: Weekly Business Review
  fiscal_year_end_month: DEC
  tooltip: true
  db_config_url: https://example.com/connections.yaml

data_sources:
  MyProdPostgres:
    main_metrics:
      query: >
        SELECT date AS "Date", "Impressions", "Clicks"
        FROM wbr_sample_1;

annotations:
  - ./events.csv

metrics:
  Impressions:
    column: main_metrics.Impressions
    aggf: sum
  Clicks:
    column: main_metrics.Clicks
    aggf: sum
  ClickThruRate:
    function:
      divide:
        - metric:
            name: Clicks
        - metric:
            name: Impressions

deck:
  - block:
      ui_type: 6_12Graph
      title: Ad Impressions (Millions)
      y_scaling: "##.2MM"
      metrics:
        Impressions:
          line_style: primary
          graph_prior_year_flag: true
```

## Sample config files

The repo includes two complete, working config files you can use as
starting points:

- [**1-wbr-sample-config.yaml**](https://github.com/working-backwards/wbr-app/blob/master/src/web/static/demo_uploads/1-wbr-sample-config.yaml) — full example with database queries, multiple chart types, tables, and derived metrics
- [**2-wbr-sample-config-with-filter.yaml**](https://github.com/working-backwards/wbr-app/blob/master/src/web/static/demo_uploads/2-wbr-sample-config-with-filter.yaml) — demonstrates filter metrics for slicing data by category

These are the same files available via the admin UI's
[Get Reference Samples](../guides/admin-ui.md#get-reference-samples)
menu.

## Getting started with YAML

If you have a CSV file and want to get started quickly, use the
[YAML auto-generator](../guides/admin-ui.md#generate-yaml) — upload
your CSV and the app creates a starter YAML file with one 6_12Graph
per column.
