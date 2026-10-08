# Excel Upload & Ingestion — Setup Guide

A Databricks App that lets users upload Excel files through a web page into a Unity Catalog Volume. Each upload is saved under a **unique name** (original name + uploader + upload time, so uploads never overwrite each other) and **automatically triggers** a job that ingests the file into a Delta table and then runs a **data quality check** on it.

The project is a [Declarative Automation Bundle](https://docs.databricks.com/dev-tools/bundles/) (DAB): one `databricks bundle deploy` creates the app, the jobs and their permissions.

> **Highlights**
> 1. **Unique file names**: uploads are stored as `<original-name>__<uploader>__<timestamp>.<ext>`, so uploading the same file twice never overwrites anything.
> 2. **Automatic ingest**: after a successful upload the app calls `run_now` on the ingest job and passes the unique file name as the job's `file_name` parameter.
> 3. **Dynamic header parsing + normalized long table**: the notebook parses the Excel headers with a regex (e.g. `Q1 2026`, `Q2 2026 預估`, `FY 2027 預估`), extracts `period_type` (Q/FY), `year`, `quarter` and `is_estimate`, and unpivots into a fixed-schema long table. New quarters in the Excel need no code change.
> 4. **Batch de-duplication + version columns**: `batch_key` (= sheet name) decides whether a batch is new. An existing batch is deleted and re-written; a new batch is appended. Every row carries `data_version`, `source_file`, `upload_user` and `upload_datetime`.
> 5. **Data quality check**: a second job task verifies that every ingested row's `batch_key` follows the `BLB_YYMMDD` date format. If it doesn't, the job fails and the app shows which values are wrong.

---

## Project layout

```
excel-upload-app/
├── databricks.yml                  # Bundle config: variables + dev/prod targets
├── resources/
│   ├── excel_upload.app.yml        # The Databricks App (env vars, warehouse/job access)
│   ├── ingest.job.yml              # Ingest job: ingest → quality_check
│   └── setup.job.yml               # One-off setup job: schema, volume, tables, UC grants
├── src/
│   ├── app/
│   │   ├── app.py                  # FastAPI backend (upload + history + job trigger + job status)
│   │   ├── requirements.txt
│   │   └── static/index.html       # Upload UI
│   └── notebooks/
│       ├── read_bloomberg.py       # Job task "ingest": read Excel, unpivot, write Delta table
│       ├── quality_check.py        # Job task "quality_check": validate the ingested batch
│       └── setup.py                # Setup job: create UC objects, grant the app access
└── README.md
```

---

## Prerequisites

1. A Databricks workspace with Unity Catalog, serverless jobs and Databricks Apps enabled.
2. An **existing catalog** you can create a schema in (the setup job creates the schema if it doesn't exist).
3. A SQL warehouse (Serverless or Pro) and its ID. You can find the ID in the warehouse's URL or under **Connection details**.
4. The [Databricks CLI](https://docs.databricks.com/dev-tools/cli/install.html), recent version (tested with v1.18.0). Check with `databricks --version`.
5. The person deploying needs:
   - `USE CATALOG` + `CREATE SCHEMA` on the catalog (or ownership of an existing schema)
   - Permission to grant on the schema, volume and tables (owner, or `MANAGE`)
   - `CAN MANAGE` on the SQL warehouse (needed to give the app `CAN USE`)

---

## Setup

### Step 1: Authenticate the CLI

```bash
databricks auth login --host https://<your-workspace-url> --profile my-workspace
```

The commands below use `-p my-workspace`. Replace it with your profile name.

### Step 2: Get the code

```bash
git clone https://github.com/jenniferwangziyi/excel-upload-app.git
cd excel-upload-app
```

### Step 3: Set your values in `databricks.yml`

Edit the `variables` under the target you'll deploy (`dev` to try it out):

```yaml
targets:
  dev:
    variables:
      catalog: my_catalog                # <-- your existing catalog
      warehouse_id: <YOUR_WAREHOUSE_ID>  # <-- your SQL warehouse ID
```

All variables:

| Variable | Default | Description |
| --- | --- | --- |
| `catalog` | — (required) | Existing catalog to deploy into |
| `schema` | `excel_upload` | Schema for the volume and tables (created if missing) |
| `volume` | `bloomberg_files` | Volume that stores the uploaded Excel files (created if missing) |
| `warehouse_id` | — (required) | SQL warehouse the app uses to read/write `upload_history` |
| `app_name` | `excel-upload` | App name (lowercase letters, numbers and hyphens; must be unique in the workspace) |

Instead of editing the file, you can also pass any variable on the command line, e.g. `--var catalog=my_catalog --var warehouse_id=abc123`.

### Step 4: Validate and deploy

```bash
databricks bundle validate -t dev -p my-workspace
databricks bundle deploy   -t dev -p my-workspace
```

This uploads the code and creates the app and both jobs.

### Step 5: Run the setup job (first deploy only)

```bash
databricks bundle run setup -t dev -p my-workspace
```

This creates the schema, volume, `upload_history` table and `bloomberg_consensus_model` table if they don't exist, and grants the app's service principal access to them. It's safe to re-run. Run it again if you change `catalog`, `schema`, `volume` or `app_name`.

### Step 6: Start the app

```bash
databricks bundle run excel_upload -t dev -p my-workspace
```

This deploys the app code and starts it, then prints the app URL. Open it, upload a test Excel file, and watch the **Job Execution Status** panel.

> The app keeps running (and consuming compute) until you stop it, either from **Compute → Apps** or with `databricks apps stop <app_name> -p my-workspace`.

### What gets deployed

| Resource | Name (dev target) | Access granted automatically |
| --- | --- | --- |
| App | `excel-upload` | All workspace users can use the app (`CAN USE`) |
| Ingest job | `[dev <you>] Read Bloomberg Excel to Delta Table` | App service principal: `CAN MANAGE RUN` |
| Setup job | `[dev <you>] Excel Upload - setup` | — |
| SQL warehouse (existing) | — | App service principal: `CAN USE` |
| Schema, volume, tables | `<catalog>.excel_upload.*` (by the setup job) | App service principal: `USE CATALOG`, `USE SCHEMA`, `READ VOLUME` + `WRITE VOLUME`, `SELECT` + `MODIFY` on `upload_history` |

The jobs run as the person who deployed the bundle, so that identity needs read/write access to the volume and both tables.

### Updating after a code change

```bash
databricks bundle deploy -t dev -p my-workspace          # notebooks + job changes take effect immediately
databricks bundle run excel_upload -t dev -p my-workspace  # also needed after changing anything under src/app
```

### Deploying to production

The `prod` target turns off the `[dev <you>]` name prefixes and deploys to `/Workspace/Users/<deployer>/.bundle/excel-upload-app/prod`. Set its variables in `databricks.yml`, then use `-t prod` in the same commands:

```bash
databricks bundle deploy -t prod -p my-workspace
databricks bundle run setup -t prod -p my-workspace
databricks bundle run excel_upload -t prod -p my-workspace
```

For a long-lived deployment, deploy as a service principal so the jobs don't depend on a personal account (authenticate the CLI as the service principal, or set `run_as` on the target). If the `dev` and `prod` targets are in the same workspace, give each a different `app_name` and `schema`.

### Removing it

```bash
databricks bundle destroy -t dev -p my-workspace
```

This deletes the app and both jobs. The schema, volume, uploaded files and tables are **not** deleted, because the setup job created them, not the bundle. Drop them yourself if you no longer need them:

```sql
DROP SCHEMA <catalog>.excel_upload CASCADE;
```

---

## How it works

```
User uploads Excel → saved to the Volume  → app triggers the job → ingest task           → quality_check task
   (app UI)           (unique file name)     (passes file_name)    (parse + unpivot +      (batch_key must be
                                                                    write Delta table)      BLB_YYMMDD)
```

1. Open the app and drag-and-drop (or browse for) an Excel file (.xlsx / .xls).
2. The file lands in the volume under a unique name (`<original-name>__<uploader>__<timestamp>`) and never overwrites an existing file.
3. The app triggers the "Read Bloomberg Excel to Delta Table" job with that file name. The catalog, schema and volume come from the job's default parameters, which the bundle sets.
4. **`ingest` task**: parses the headers, extracts `period_type` / `year` / `quarter` / `is_estimate`, unpivots into the normalized long table, and writes `bloomberg_consensus_model` (a re-upload with the same `batch_key` replaces that batch).
5. **`quality_check` task**: checks the rows from this file (see below). The app's **Job Execution Status** panel shows each step's result.
6. Query the data from the SQL editor or a dashboard, using the version columns to tell uploads apart.

### Target table schema

| Column | Type | Description |
| --- | --- | --- |
| `metric` | STRING | Metric name (e.g. revenue) |
| `period_type` | STRING | `Q` (quarter) or `FY` (full-year total) |
| `year` | INT | Year |
| `quarter` | INT | Quarter (1-4); NULL for FY |
| `is_estimate` | BOOLEAN | Whether the value is an estimate |
| `value` | DECIMAL(38,7) | Value in units (the Excel is in millions; the notebook multiplies by 1,000,000) |
| `source_file` | STRING | Source file name |
| `upload_user` | STRING | Uploader (looked up from `upload_history`; NULL for manual runs) |
| `upload_datetime` | TIMESTAMP | Upload time (looked up from `upload_history`; NULL for manual runs) |
| `data_version` | STRING | Data version key (`V_Q{earliest estimated quarter}_{year}_ESTIMATE`) |
| `batch_key` | STRING | Batch key (= sheet name, e.g. `BLB_260723`) |

### Data quality check

The `quality_check` task reads the rows the `ingest` task just wrote (the ingest task passes the file name downstream as a task value). It checks that each row's `batch_key` matches `BATCH_KEY_REGEX` (default `^BLB_(\d{6})$`) and that the captured part is a real date in `BATCH_DATE_FORMAT` (default `yyMMdd`). Both are set in the notebook's **Configuration** cell.

| Sheet name | Result |
| --- | --- |
| `BLB_260723` | ✅ Pass |
| `Sheet1` | ❌ Fail — no `BLB_YYMMDD` pattern |
| `BLB_2607` | ❌ Fail — the date part is not 6 digits |
| `BLB_260231` | ❌ Fail — not a real date. The ingest task already rejects impossible dates like this when it parses the sheet name, so nothing is written. |

On failure the task raises an error listing the bad `batch_key` values, so the job run is marked **Failed** and the app shows the message. The rows **stay in the table** so you can inspect them. To remove a bad batch, run:

```sql
DELETE FROM <catalog>.<schema>.bloomberg_consensus_model WHERE batch_key = '<bad batch_key>';
```

Then rename the sheet (e.g. `BLB_260723`) and upload the file again.

To add more checks, add cells to `quality_check` that raise an `AssertionError` when they fail. Anything that raises fails the task. The task has serverless auto-optimization turned off (`disable_auto_optimization` in `resources/ingest.job.yml`), so a failed check isn't automatically retried.

### Excel layout the notebook expects

- Row 4 is the header (`header=3`; rows 1–3 are metadata) and columns B–L are read (`usecols="B:L"`).
- Only rows between the `文字版本` and `圖` marker rows in column B are ingested.
- Period headers look like `Q{1-4} {year}`, `Q{1-4} {year} 預估`, `FY {year}` or `FY {year} 預估`, and one column header contains `單位` (the metric/unit column).

These markers are matched literally against the workbook, so they stay in the workbook's language (Chinese). They're named constants in the `read_bloomberg` notebook's **Configuration** cell (`START_MARKER`, `END_MARKER`, `METRIC_COL_MARKER`, `ESTIMATE_SUFFIX`); change them only if your workbook uses different labels.

---

## FAQ

**Q: The job didn't run after I uploaded a file.**
A: The app's success message shows the triggered run ID. If there isn't one, check the app logs (**Compute → Apps → your app → Logs**). The bundle grants the app `CAN MANAGE RUN` on the job automatically. If the trigger fails, the file still stays in the volume and you can run the job manually.

**Q: Upload fails with a permission error, or the upload history is empty.**
A: Run the setup job (`databricks bundle run setup`). It grants the app's service principal access to the volume and `upload_history`.

**Q: The job failed at `quality_check`. Is my data lost?**
A: No. The `ingest` task already wrote the rows; the check only flags them. See [Data quality check](#data-quality-check) to clean up and re-upload.

**Q: The Excel has a new quarter column (e.g. Q1 2028). Do I need to change the code?**
A: No. As long as the header follows `Q{1-4} {year}` or `FY {year}` (optionally with the `預估` estimate suffix), the notebook picks it up automatically.

**Q: Can I run the notebooks by hand?**
A: Yes. Open them in the workspace and set the `catalog`, `schema`, `volume` and (optionally) `file_name` widgets. With `file_name` empty, the ingest notebook picks the newest file in the volume, and the quality check checks the whole table.

---

## Notes

- Each run writes the unpivoted rows with a fixed schema (no `mergeSchema`). Re-uploading a file with the same sheet name (`batch_key`) replaces that batch. Different batches are kept side by side, so filter on `batch_key` or `upload_datetime` if you want only the latest.
- The ingest job runs one upload at a time. If several files are uploaded together, the extra runs queue (the app shows *Job queued...*) and run in order.
- Because file names are unique, files pile up in the volume. Delete old files periodically if you need the space; this does not affect data already in the table.
- If your workspace installs Python packages through a private PyPI mirror, add `--index-url <your-mirror-url>` to the `%pip install openpyxl` line in `read_bloomberg`.
