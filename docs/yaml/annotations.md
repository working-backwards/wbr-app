# annotations

Annotations overlay business events on your WBR charts, displayed as
"Noteworthy Events" beneath relevant blocks.

```yaml
annotations:
  - ./events.csv
  - https://example.com/team-events.csv
```

`annotations` is a top-level key (sibling to `setup`, `data_sources`,
`metrics`, `deck`). It takes a list of CSV file paths or URLs. Multiple
files are concatenated.

## Annotation CSV format

Each CSV must have exactly these three columns:

| Column | Description |
|--------|-------------|
| `Date` | Event date (must be parseable, same format as your data) |
| `MetricName` | Must exactly match a metric name from your `metrics` section |
| `EventDescription` | Free-text description shown in the deck |

### Example CSV

```csv
Date,MetricName,EventDescription
2021-09-04,Impressions,Major marketing campaign launch
2021-09-08,Clicks,Website redesign deployed
2021-09-15,PageViews,Social media viral post
```

## Filtering rules

- Only events within the **trailing 6 weeks** (current year and prior
  year) are displayed — older events are filtered out
- Events referencing a `MetricName` that doesn't match a defined metric
  are logged as errors (visible in the deck's `eventErrors` field)
- Only **one event per MetricName** is kept — if multiple events share
  the same `MetricName`, the last one in the CSV wins

## Display

Events appear as a "Noteworthy Events" section beneath the relevant
chart or table block:

```
Noteworthy Events:
September 04 2021    Impressions    Major marketing campaign launch
September 08 2021    Clicks         Website redesign deployed
```

Events are displayed on both `6_12Graph` and `6_WeeksTable` blocks
when a matching metric is present in the block.

## Complete example

```yaml
setup:
  week_ending: 25-SEP-2021
  week_number: 38
  title: WBR with Annotations

annotations:
  - ./events.csv

metrics:
  Impressions:
    column: Impressions
    aggf: sum

deck:
  - block:
      ui_type: 6_12Graph
      title: Ad Impressions
      y_scaling: "##.2MM"
      metrics:
        Impressions:
          line_style: primary
          graph_prior_year_flag: true
```
