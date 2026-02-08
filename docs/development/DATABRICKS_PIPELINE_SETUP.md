# Azure Databricks Pipeline Setup

This runbook configures the Maryland Atlas pipeline so each ingest run writes directly to Databricks Delta tables.

## 1. Create Databricks SQL target

1. In Azure Databricks, create or select:
- A Unity Catalog catalog (default: `maryland_atlas`)
- A schema (default: `default`)
- A SQL warehouse
2. Generate a Databricks personal access token (PAT).
3. Collect:
- `DATABRICKS_SERVER_HOSTNAME` (for example `adb-xxx.azuredatabricks.net`)
- `DATABRICKS_HTTP_PATH` (warehouse HTTP path)
- `DATABRICKS_ACCESS_TOKEN`

## 2. Configure local/project environment

Set these in `.env`:

```bash
DATA_BACKEND=databricks
DATABRICKS_SERVER_HOSTNAME=...
DATABRICKS_HTTP_PATH=...
DATABRICKS_ACCESS_TOKEN=...
DATABRICKS_CATALOG=maryland_atlas
DATABRICKS_SCHEMA=default
DATABRICKS_ENABLE_TELEMETRY=false
DATABRICKS_SOCKET_TIMEOUT_SECONDS=60
DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_COUNT=6
DATABRICKS_RETRY_STOP_AFTER_ATTEMPTS_DURATION=120
MAPBOX_ACCESS_TOKEN=...
CENSUS_API_KEY=...
AZURE_STORAGE_CONNECTION_STRING=...
AZURE_STORAGE_CONTAINER=geojson
```

## 3. Validate connection + initialize Delta schema

```bash
make databricks-test
make databricks-init
```

`make databricks-init` creates/updates all Delta tables from `data/schemas/databricks_schema.sql` and can load county geometries.

## 4. Run ingest + pipeline directly to Databricks

```bash
DATA_BACKEND=databricks make ingest-all
DATA_BACKEND=databricks make pipeline
```

This writes layer tables, summary outputs, and export metadata into Databricks.

## 5. Deploy scheduled Databricks workflow

```bash
make databricks-deploy
```

Then in Databricks Jobs:
1. Open `maryland-atlas-pipeline`
2. Add missing secrets/env values on the job cluster (API keys and storage connection string)
3. Unpause the schedule when ready

## 6. Azure orchestration pattern (recommended)

If you want Azure Data Factory to trigger pulls:
1. Use an ADF pipeline with a Databricks Job activity.
2. Trigger the `maryland-atlas-pipeline` job.
3. Monitor job status in Databricks; monitor trigger history in ADF.

This is cleaner than routing data through an intermediate Azure data store first, because the pipeline already writes to Databricks tables.

## Troubleshooting

- `Databricks connection failed`: verify hostname/path/token and warehouse is running.
- SQL errors with `ON CONFLICT` or `ANY(...)`: pull latest code from this repo (Databricks SQL translation support was added).
- No GeoJSON in blob container: confirm `AZURE_STORAGE_CONNECTION_STRING` and container permissions.
