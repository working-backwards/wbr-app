# Connections File

The connections file defines database credentials. Referenced from
`setup.db_config_url` in your WBR YAML config.

```yaml
version: 1.0
connections:
  - name: MyProdPostgres
    type: postgres
    config:
      host: db.example.com
      port: 5432
      username: analyst
      password: secret
      database: metrics_db
```

## Structure

| Field | Required | Description |
|-------|----------|-------------|
| `version` | Yes | Must be `1.0` |
| `connections` | Yes | List of connection entries |
| `connections[].name` | Yes | Unique name (referenced in `data_sources`) |
| `connections[].type` | Yes | `postgres`, `snowflake`, `redshift`, or `athena` |
| `connections[].config` | Yes | Connection parameters (see below) |

## Connector configs

### PostgreSQL

```yaml
config:
  host: db.example.com
  port: 5432
  username: user
  password: pass
  database: mydb
```

### Redshift

```yaml
config:
  host: cluster.region.redshift.amazonaws.com
  port: 5439
  username: user
  password: pass
  database: mydb
```

### Snowflake

```yaml
config:
  account: xy12345.us-east-1
  user: analyst
  password: pass
  warehouse: COMPUTE_WH
  database: ANALYTICS_DB
  schema: PUBLIC         # optional
  role: ANALYST_ROLE     # optional
```

### Athena

```yaml
config:
  region_name: us-east-1
  s3_staging_dir: s3://my-bucket/athena-results/
  database: data_lake
  workgroup: primary     # optional
```

## AWS Secrets Manager

Instead of hardcoding passwords, reference a secret:

```yaml
connections:
  - name: MyPostgres
    type: postgres
    config:
      service: aws
      secret_name: prod/postgres/main
```

The secret must be a JSON object with the connection fields (`host`,
`port`, `username`, `password`, `database`).

The app uses ambient AWS credentials (IAM role recommended). Or set
environment variables:

```
AWS_STORAGE_KEY=AKIA...
AWS_STORAGE_SECRET=...
AWS_REGION_NAME=us-east-1
```
