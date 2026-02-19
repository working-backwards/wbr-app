# Migration Guide: v1.0 → v2.0

Use this guide to upgrade projects and configs that were built on `v1.0` to the new `v2.0` behaviors.

## 1) Update dependencies

- Recreate your virtualenv and `pip install -r requirements.txt`; new runtime deps include `boto3`, `psycopg2-binary`,
  `snowflake-connector-python`, `google-cloud-*`, and `azure-*`. `cryptography` is pinned to 41.0.7 for snowflake
  compatibility.

## 2) Adopt the new data source model

- Add `setup.db_config_url` in your WBR YAML to point to a connections YAML file (URL or local path). CSV upload remains
  supported and will override DB data when provided.
- Define `data_sources` as a dictionary keyed by the connection name from the connections YAML. Each entry contains one
  or more query blocks. Example:
  ```yaml
  setup:
    db_config_url: https://your-host/connections.yaml

  data_sources:
    MyProdPostgres:            # must match the `name` in connections.yaml
      main_metrics:
        query: >
          SELECT date AS "Date", "Impressions", "Clicks" FROM wbr_sample_1;
      sales_metrics:
        query: >
          SELECT date AS "Date", "pct_customer_group_1" FROM wbr_sample_1;
  ```
- The first column in every query must be aliased as `"Date"`. All other columns are automatically prefixed with the
  query key (`main_metrics.Impressions`, `sales_metrics.pct_customer_group_1`, etc.). Update metric definitions to
  reference these aliases.
- You can mix DB queries and CSV sources via `data_sources.csv_files.<name>.url_or_path`. Multiple sources are merged by
  `Date`; duplicate column names get source-specific suffixes.
- If you upload/pass a CSV directly to a run, it overrides all YAML `data_sources`/`csv_files`. To merge CSV with DB
  queries in the same run, list CSVs under `data_sources.csv_files` and do not pass a CSV override.

## 3) Connections and secrets

- Connections YAML entries require `name`, `type` (`postgres`, `snowflake`, `athena`, `redshift`), and `config`. If a
  `service` field is present (currently `aws`), credentials are loaded from the specified secret (e.g., AWS Secrets
  Manager) using ambient cloud credentials or environment variables (`AWS_STORAGE_KEY`, `AWS_STORAGE_SECRET`,
  `AWS_REGION_NAME`). Please refer `Configuring Database Connectivity` section from README.md for more information on
  the connections yaml file

## 4) Validation expectations
- `setup.week_ending` is mandatory and must use `DD-MMM-YYYY` (e.g., `25-SEP-2012`).

## 5) UI/runtime changes to note

- `/get-wbr-metrics` now accepts only the YAML config; CSV is optional. The frontend auto-adds JSON download and publish
  buttons after a report is built.
- YAML generation endpoint (`/download_yaml`) uses the new loading path and will fall back to the rules-based generator
  if the plugin is unavailable.

## 6) Suggested verification

- Run a small end-to-end build with a config that references `data_sources` only (no CSV) to confirm DB connectivity and
  aliasing.
- Re-run any automation/tests; new connector and validator tests live under `src/tests/`.

