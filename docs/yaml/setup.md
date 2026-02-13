# setup

The `setup` section defines deck-level metadata and options.

```yaml
setup:
  week_ending: 25-SEP-2021
  week_number: 38
  title: Weekly Business Review
  fiscal_year_end_month: DEC
  block_starting_number: 1
  tooltip: true
  db_config_url: https://example.com/connections.yaml
```

## Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `week_ending` | String (`DD-MMM-YYYY`) | **Yes** | — | End date of the latest week in your data. The app treats the 7 days prior as a "week". |
| `week_number` | Number | No | — | Week number displayed on charts (1–52). |
| `title` | String | No | — | Deck title shown at the top of the report. |
| `fiscal_year_end_month` | String (3-letter abbrev) | No | `DEC` | Last month of your fiscal year. Only needed if fiscal ≠ calendar year. |
| `block_starting_number` | Number | No | `1` | Starting block label number. Use when [uploading multiple files](../guides/multiple-files.md). |
| `tooltip` | Boolean | No | `false` | Show data values when hovering over chart data points. |
| `db_config_url` | String (URL or path) | No | — | Location of your [connections file](../reference/connections.md). Required if using `data_sources`. |

![Week number on chart](../images/week-number-example.png)
