# Excel Upload & Ingestion — Deployment Guide

A Databricks App that lets users upload Excel files through a web page into a Unity Catalog Volume. Each upload is saved under a **unique name** (original name + uploader + upload time, so uploads never overwrite each other) and **automatically triggers** a job that ingests the file into a Delta table and then runs a **data quality check** on it.

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
├── app.py              # FastAPI backend (unique-name upload + history + job trigger + job status)
├── app.yaml            # Databricks App config (warehouse ID + ingest job ID)
├── requirements.txt    # Python dependencies
├── setup.sql           # Environment setup SQL (catalog / schema / volume / tables)
├── static/
│   └── index.html      # Upload UI
├── read_bloomberg.py   # Notebook (job task "ingest"): read Excel, unpivot, write Delta table
├── quality_check.py    # Notebook (job task "quality_check"): validate the ingested batch
└── README.md           # This file
```

The two `.py` notebooks are in Databricks notebook source format. When you import them into a workspace they become notebooks named `read_bloomberg` and `quality_check`.

---

## Prerequisites

1. A Databricks workspace with Unity Catalog
2. A SQL warehouse (Serverless or Pro)
3. A user or service principal with:
   - `CREATE TABLE` and `CREATE VOLUME` on the target catalog/schema
   - `CAN USE` on the SQL warehouse
   - `READ VOLUME` / `WRITE VOLUME` on the target volume

---

## Deployment steps

### Step 1: Create the catalog, schema, volume and tables

Open `setup.sql` in the SQL editor, replace `<YOUR_CATALOG>`, `<YOUR_SCHEMA>` and `<YOUR_VOLUME>` with your own values, and run it. It creates:

| Object | Purpose |
| --- | --- |
| `<CATALOG>.<SCHEMA>.<VOLUME>` | Volume where uploaded Excel files land |
| `<CATALOG>.<SCHEMA>.upload_history` | One row per upload; read by the app's history panel and by the notebook to look up the uploader |
| `<CATALOG>.<SCHEMA>.bloomberg_consensus_model` | Ingest target table (optional up front — the notebook creates it on its first run if missing) |

### Step 2: Set your environment values in the code

Every place that needs your values:

#### `app.py`

| Constant | Default | Change to |
| --- | --- | --- |
| `VOLUME_PATH` | `/Volumes/my_catalog/my_schema/bloomberg_files` | `/Volumes/<CATALOG>/<SCHEMA>/<VOLUME>` |
| `HISTORY_TABLE` | `my_catalog.my_schema.upload_history` | `<CATALOG>.<SCHEMA>.upload_history` |

#### `app.yaml`

| Variable | Default | Change to |
| --- | --- | --- |
| `DATABRICKS_WAREHOUSE_ID` | `<YOUR_WAREHOUSE_ID>` | Your SQL warehouse ID (used to read/write `upload_history`) |
| `INGEST_JOB_ID` | `<YOUR_INGEST_JOB_ID>` | The ingest job ID from Step 4 |

> 💡 The warehouse ID is in the SQL warehouse's URL or under **Connection details**. The job ID is in the job's URL (`/jobs/<JOB_ID>`).

#### `read_bloomberg` notebook — **Configuration** cell

| Variable | Default | Change to |
| --- | --- | --- |
| `volume_path` | `/Volumes/my_catalog/my_schema/bloomberg_files/` | `/Volumes/<CATALOG>/<SCHEMA>/<VOLUME>/` |
| `history_table` | `my_catalog.my_schema.upload_history` | `<CATALOG>.<SCHEMA>.upload_history` |
| `table_name` | `my_catalog.my_schema.bloomberg_consensus_model` | `<CATALOG>.<SCHEMA>.<TARGET_TABLE>` |

The same cell holds the text markers the notebook looks for in the workbook: `START_MARKER`, `END_MARKER`, `METRIC_COL_MARKER` and `ESTIMATE_SUFFIX`. They are matched literally against the Excel content, so they stay in the workbook's language (Chinese). Change them only if your workbook uses different labels.

#### `quality_check` notebook — **Configuration** cell

| Variable | Default | Change to |
| --- | --- | --- |
| `table_name` | `my_catalog.my_schema.bloomberg_consensus_model` | Same table as `read_bloomberg` |
| `BATCH_KEY_REGEX` | `^BLB_(\d{6})$` | Expected sheet-name pattern; the first group is the date part |
| `BATCH_DATE_FORMAT` | `yyMMdd` | Spark datetime pattern the date part must parse with |

#### `static/index.html`

| Content | Change to |
| --- | --- |
| `my_catalog.my_schema.bloomberg_files` in the subtitle | `<CATALOG>.<SCHEMA>.<VOLUME>` (display only — no functional effect) |

### Step 3: Import the files into your workspace

Upload the whole project folder to your workspace, for example `/Workspace/Shared/excel-upload-app/`. With the Databricks CLI:

```bash
databricks workspace import-dir . /Workspace/Shared/excel-upload-app
```

### Step 4: Create the ingest job (triggered by the app)

1. Go to **Jobs & Pipelines** → **Create** → **Job**.
2. Add the first task:
   - **Task name**: `ingest` (the name must match — the quality check and the app look for it)
   - **Type**: Notebook → `read_bloomberg`
   - **Compute**: Serverless
3. Add the second task:
   - **Task name**: `quality_check` (the name must match — the app looks for it)
   - **Type**: Notebook → `quality_check`
   - **Depends on**: `ingest`
   - **Compute**: Serverless
   - **Retries**: in the task's **Retries** settings, turn off serverless auto-optimization. Serverless otherwise retries a failed task automatically, and a failed quality check gives the same result every time, so the retry only delays the error.
4. Add a job-level parameter `file_name` with an empty default. The app fills it in with the uploaded file's unique name. When it's empty (manual run), the ingest notebook picks the newest file in the volume.
5. Save the job and copy its **Job ID** into `INGEST_JOB_ID` in `app.yaml`.
6. (Optional) Add a schedule if you also want periodic runs.

<details>
<summary>Or create the job with the CLI</summary>

Save this as `job.json` (adjust the notebook paths), then run `databricks jobs create --json @job.json`:

```json
{
  "name": "Read Bloomberg Excel to Delta Table",
  "parameters": [{ "name": "file_name", "default": "" }],
  "tasks": [
    {
      "task_key": "ingest",
      "notebook_task": { "notebook_path": "/Workspace/Shared/excel-upload-app/read_bloomberg" }
    },
    {
      "task_key": "quality_check",
      "depends_on": [{ "task_key": "ingest" }],
      "notebook_task": { "notebook_path": "/Workspace/Shared/excel-upload-app/quality_check" },
      "disable_auto_optimization": true
    }
  ]
}
```

Tasks without a cluster run on serverless compute. `disable_auto_optimization` stops serverless from automatically retrying a failed quality check.
</details>

### Step 5: Deploy the Databricks App

1. Go to **Compute** → **Apps** → **Create app** → **Create a custom app**, and give it a name (e.g. `excel-upload`).
2. Deploy it with **Source code path** pointing at the imported folder, or with the CLI:
   ```bash
   databricks apps deploy excel-upload --source-code-path /Workspace/Shared/excel-upload-app
   ```
3. Grant the app's service principal (shown on the app's **Authorization** tab):
   - `CAN USE` on the SQL warehouse
   - `USE CATALOG` / `USE SCHEMA` on the catalog and schema
   - `SELECT` + `MODIFY` on `upload_history`
   - `READ VOLUME` + `WRITE VOLUME` on the volume
   - `CAN MANAGE RUN` on the ingest job (needed to trigger it and read its status)
4. Make sure the job's **Run as** identity (the job owner by default) can read the volume and write both tables.

---

## How it works

```
User uploads Excel → saved to the Volume  → app triggers the job → ingest task           → quality_check task
   (app UI)           (unique file name)     (passes file_name)    (parse + unpivot +      (batch_key must be
                                                                    write Delta table)      BLB_YYMMDD)
```

1. Open the app and drag-and-drop (or browse for) an Excel file (.xlsx / .xls).
2. The file lands in the volume under a unique name (`<original-name>__<uploader>__<timestamp>`) and never overwrites an existing file.
3. The app triggers the "Read Bloomberg Excel to Delta Table" job with that file name.
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

The `quality_check` task reads the rows the `ingest` task just wrote (the ingest task passes the file name downstream as a task value). It checks that each row's `batch_key` matches `BATCH_KEY_REGEX` (default `^BLB_(\d{6})$`) and that the captured part is a real date in `BATCH_DATE_FORMAT` (default `yyMMdd`).

| Sheet name | Result |
| --- | --- |
| `BLB_260723` | ✅ Pass |
| `Sheet1` | ❌ Fail — no `BLB_YYMMDD` pattern |
| `BLB_260231` | ❌ Fail — `260231` is 6 digits but not a real date (the ingest task already rejects impossible dates like this when it parses the sheet name, so nothing is written) |
| `BLB_2607` | ❌ Fail — the date part is not 6 digits |

On failure the task raises an error listing the bad `batch_key` values, so the job run is marked **Failed** and the app shows the message. The rows **stay in the table** so you can inspect them. To remove a bad batch, run:

```sql
DELETE FROM <CATALOG>.<SCHEMA>.bloomberg_consensus_model WHERE batch_key = '<bad batch_key>';
```

Then rename the sheet (e.g. `BLB_260723`) and upload the file again.

To add more checks, add cells to `quality_check` that raise an `AssertionError` when they fail. Anything that raises fails the task.

---

## Quick reference

| Placeholder | Meaning | Example |
| --- | --- | --- |
| `<CATALOG>` | Unity Catalog name | `my_catalog` |
| `<SCHEMA>` | Schema name | `my_schema` |
| `<VOLUME>` | Volume name (stores the Excel files) | `bloomberg_files` |
| `<WAREHOUSE_ID>` | SQL warehouse ID | `abcdef1234567890` |
| `<TARGET_TABLE>` | Delta table the data is ingested into | `bloomberg_consensus_model` |

---

## FAQ

**Q: The job didn't run after I uploaded a file.**
A: Check that (1) `INGEST_JOB_ID` in `app.yaml` is correct, and (2) the app's service principal has `CAN MANAGE RUN` on the job. The app's success message shows the triggered run ID. If the trigger fails, the file still stays in the volume and you can run the job manually.

**Q: The upload history is empty.**
A: Check that `upload_history` exists and that the app's service principal has `SELECT` and `MODIFY` on it.

**Q: The job failed at `quality_check`. Is my data lost?**
A: No. The `ingest` task already wrote the rows; the check only flags them. See [Data quality check](#data-quality-check) to clean up and re-upload.

**Q: The Excel has a new quarter column (e.g. Q1 2028). Do I need to change the code?**
A: No. As long as the header follows `Q{1-4} {year}` or `FY {year}` (optionally with the `預估` estimate suffix), the notebook picks it up automatically.

---

## Notes

- The notebook uses Excel row 4 as the header (`header=3`; rows 1–3 are metadata) and reads columns B–L (`usecols="B:L"`). Make sure your workbook follows the same layout.
- Only rows between the `文字版本` and `圖` marker rows in column B are ingested.
- Period headers must look like `Q{1-4} {year}`, `Q{1-4} {year} 預估`, `FY {year}` or `FY {year} 預估`, and one column header must contain `單位` (the metric/unit column).
- Each run writes the unpivoted rows with a fixed schema (no `mergeSchema`). Re-uploading a file with the same sheet name (`batch_key`) replaces that batch. Different batches are kept side by side, so filter on `batch_key` or `upload_datetime` if you want only the latest.
- Because file names are unique, files pile up in the volume. Delete old files periodically if you need the space; this does not affect data already in the table.
- The job runs one upload at a time by default. If several files are uploaded together, the extra runs queue (the app shows *Job queued...*) and run in order.
- After deploying, run one test file through the whole flow.

---

## Deploy with Genie Code

You can paste the prompt below into Genie Code (the Databricks AI assistant) and have it set values and deploy for you.

---

**Copy this into Genie Code:**

````
Help me deploy the "excel-upload-app" project, which is in my workspace folder <paste your folder path>.

Please do the following:

1. Create the infrastructure by running setup.sql with:
   - Catalog: <your CATALOG name>
   - Schema: <your SCHEMA name>
   - Volume: <your VOLUME name>

2. In app.py, set:
   - VOLUME_PATH to /Volumes/<CATALOG>/<SCHEMA>/<VOLUME>
   - HISTORY_TABLE to <CATALOG>.<SCHEMA>.upload_history

3. In the Configuration cell of the read_bloomberg notebook, set:
   - volume_path to /Volumes/<CATALOG>/<SCHEMA>/<VOLUME>/
   - history_table to <CATALOG>.<SCHEMA>.upload_history
   - table_name to <CATALOG>.<SCHEMA>.<your TABLE name>

4. In the Configuration cell of the quality_check notebook, set table_name to the same table as step 3.

5. In static/index.html, change the volume path shown in the subtitle to my volume.

6. Create a job on serverless compute with a job-level parameter file_name (empty default) and two notebook tasks:
   - task "ingest" running read_bloomberg
   - task "quality_check" running quality_check, depending on "ingest"
   Then put the job ID into INGEST_JOB_ID in app.yaml, and my SQL warehouse ID <your SQL warehouse ID> into DATABRICKS_WAREHOUSE_ID.

7. Deploy the Databricks App from this folder, and give the app's service principal CAN USE on the warehouse, USE CATALOG/USE SCHEMA, SELECT and MODIFY on upload_history, READ VOLUME and WRITE VOLUME on the volume, and CAN MANAGE RUN on the job.

Do this step by step and tell me the result after each step.
````

> 💡 Replace the `< >` placeholders with your values before pasting. If you don't know your SQL warehouse ID, first ask Genie Code: "List my SQL warehouses and their IDs."
