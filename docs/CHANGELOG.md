# Changelog

## v2.0 (compared to v1.0)

- Added features
    - Database ingestion via new connectors for Postgres, Snowflake, Redshift, and Athena with optional AWS Secrets
      Manager hydration for credentials. Data sources can also be external CSVs and annotations merged into the same
      daily frame.
    - Data loading now auto-merges multiple queries and CSV sources keyed on `Date`, supporting aliasing per query name
      for collision-free metrics.

- Changed behavior
    - CSV upload is now optional; if omitted the backend loads data from `setup.db_config_url` using the `data_sources`
      map. Queries must alias the first column as `"Date"` and metrics reference `query_name.column`.
    - A CSV provided at request time overrides all YAML `data_sources`/`csv_files`; to merge CSV with DB queries, list
      CSVs under `data_sources.csv_files` in the YAML instead of uploading ad hoc.
    - Validation enforces `setup.week_ending` presence/format and tighter metric config checks for aggregation/function
      definitions.
    - YAML generation endpoint now reuses the new data loading path and falls back to the rules-based generator if the
      plugin fails.

- Removed or deprecated functions
    - No removals; existing endpoints remain.

- Test updates
    - New unit tests cover connector factory and individual connectors plus validator/controller utility behaviors.

- Documentation updates
    - README expanded with database configuration guidance; new `docs/user_manual.md` added covering CSV override
      precedence, DB/CSV merging rules, and updated sample YAML showing `data_sources` aliasing conventions.

- Dependency updates
    - Bumped core libs (Flask/Werkzeug, pandas, numpy, matplotlib), cloud SDKs (boto3, google-cloud-*, azure-*), DB
      drivers (psycopg2-binary, snowflake-connector-python), and pytest to current minor versions. Kept `cryptography`
      pinned for Snowflake compatibility.

