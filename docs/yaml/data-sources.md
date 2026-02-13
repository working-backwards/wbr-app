# data_sources

The `data_sources` section tells the WBR App where to fetch your daily
metric data. You can query databases, reference CSV files, or both.

!!! info "data_sources is optional"
    If you upload a CSV directly via the UI or API, that CSV becomes the
    sole data source and `data_sources` is ignored. Use `data_sources`
    when you want the app to fetch data automatically or merge multiple
    sources.

## Database queries

Each entry under `data_sources` references a named connection from your
[connections file](../reference/connections.md), and contains one or
more named queries.

```yaml
setup:
  db_config_url: https://example.com/connections.yaml

data_sources:
  MyProdPostgres:              # Must match a connection name
    main_metrics:              # Descriptive query name
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
```

!!! important "Every query must return a `Date` column"
    Alias the date column as `"Date"` (quoted, capitalized) in your SQL.

### Column namespacing

Non-`Date` columns are automatically prefixed with the query name:

- `main_metrics.Impressions`
- `main_metrics.Clicks`
- `sales_metrics.pct_customer_group_1`

Reference these namespaced names in your [`metrics`](metrics.md)
section.

### Supported databases

| Type | `connections.yaml` type value | Notes |
|------|------|-------|
| PostgreSQL | `postgres` | Default port 5432 |
| Amazon Redshift | `redshift` | Default port 5439, auto-lowercases columns |
| Snowflake | `snowflake` | Auto-handles uppercase DATE → Date |
| Amazon Athena | `athena` | Async polling; requires `s3_staging_dir` |

See [Connections File](../reference/connections.md) for full
connection config details and credential management.

## CSV files (in config)

To merge CSV data alongside database queries, list CSVs under
`csv_files`:

```yaml
data_sources:
  MyProdPostgres:
    main_metrics:
      query: >
        SELECT date AS "Date", "Impressions" FROM wbr_sample_1;

  csv_files:
    external_metrics:
      url_or_path: ./data/external_metrics.csv
    remote_data:
      url_or_path: https://example.com/data.csv
```

CSV columns are namespaced the same way: `external_metrics.{column}`.

## Merging behavior

All sources (queries + CSVs) are outer-joined on `Date`:

- A date present in one source but not another produces NaN for the
  missing columns
- Multiple rows per date are allowed — your `aggf` in
  [`metrics`](metrics.md) handles aggregation

## CSV upload vs. config: precedence rules

| Scenario | Behavior |
|----------|----------|
| CSV uploaded directly (UI or API) | **Overrides** all YAML `data_sources` |
| `data_sources` defined in YAML, no CSV uploaded | DB queries + config CSVs merged |
| Both CSV uploaded AND `data_sources` in YAML | Uploaded CSV wins, `data_sources` ignored |

!!! tip
    Use direct CSV upload for prototyping. Use `data_sources` for
    production workflows where data is fetched automatically.
