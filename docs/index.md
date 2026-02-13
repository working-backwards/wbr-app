# WBR App

The WBR App generates Amazon-style Weekly Business Review decks from your
metric data. Connect to databases or upload CSV files, define your metrics
in a YAML configuration, and publish interactive reports.

[See a sample WBR report](https://workingbackwards.com/wbr-app/sample-wbr-report/){ .md-button }
[Get started](getting-started.md){ .md-button .md-button--primary }

## What is a WBR?

The Weekly Business Review is a metrics review process where input and output
metrics are displayed in a standardized format — trailing 6 weeks and 12 months
side by side — so leaders can spot trends and anomalies quickly. For background,
see Chapter 6 of *Working Backwards*.

## Features

- **6/12 Charts** — 6 weeks + 12 months on a single axis with summary tables
- **Database connectors** — Postgres, Snowflake, Redshift, Athena
- **CSV support** — Upload directly or reference in config
- **Annotations** — Overlay business events on your charts
- **Publishing** — Generate shareable URLs with optional password protection
- **JSON export** — Feed WBR data into Tableau, Looker, or other tools

## The YAML Config

Everything is driven by a single YAML file with these sections:

```yaml
setup:          # Deck metadata (title, dates, options)
data_sources:   # Where to get your data (DB queries, CSV files)
annotations:    # Business events to overlay on charts
metrics:        # Define and transform your metrics
deck:           # Layout your charts, tables, and sections
```

Each section has its own documentation page — see
[YAML Configuration](yaml/overview.md).
