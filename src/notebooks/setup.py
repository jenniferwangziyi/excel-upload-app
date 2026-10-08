# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC ## Setup: Unity Catalog objects and app grants
# MAGIC Runs as the bundle's `setup` job (`databricks bundle run setup`). Safe to re-run.
# MAGIC
# MAGIC 1. Creates the schema, volume, `upload_history` table and `bloomberg_consensus_model` table if they don't exist (the catalog must already exist).
# MAGIC 2. Grants the app's service principal what it needs: `USE CATALOG`, `USE SCHEMA`, `READ VOLUME` / `WRITE VOLUME` on the volume, and `SELECT` / `MODIFY` on `upload_history`.
# MAGIC
# MAGIC The identity running this job needs `CREATE SCHEMA` on the catalog (or ownership of an existing schema) and the right to grant on these objects.

# COMMAND ----------

# DBTITLE 1,Parameters
dbutils.widgets.text("catalog", "my_catalog", "Catalog")
dbutils.widgets.text("schema", "excel_upload", "Schema")
dbutils.widgets.text("volume", "bloomberg_files", "Volume")
dbutils.widgets.text("app_name", "excel-upload", "App name")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
volume = dbutils.widgets.get("volume").strip()
app_name = dbutils.widgets.get("app_name").strip()

fq_schema = f"`{catalog}`.`{schema}`"
fq_volume = f"{fq_schema}.`{volume}`"
history_table = f"{fq_schema}.upload_history"
target_table = f"{fq_schema}.bloomberg_consensus_model"
print(f"Schema: {fq_schema}\nVolume: {fq_volume}\nApp:    {app_name}")

# COMMAND ----------

# DBTITLE 1,Create schema, volume and tables
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {fq_schema}")

spark.sql(f"""
CREATE VOLUME IF NOT EXISTS {fq_volume}
  COMMENT 'Excel files uploaded through the app'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {history_table} (
  file_name     STRING    COMMENT 'Uploaded file name (unique name stored in the volume)',
  uploaded_by   STRING    COMMENT 'Identity of the uploader',
  size_bytes    BIGINT    COMMENT 'File size in bytes',
  status        STRING    COMMENT 'Status (landed / ingested)',
  uploaded_at   TIMESTAMP COMMENT 'Upload time'
)
COMMENT 'One row per Excel upload'
""")

# Normalized long table written by the ingest task. Columns and types must match
# the read_bloomberg notebook's output; new quarters in the Excel need no change.
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table} (
  metric           STRING        COMMENT 'Metric name (e.g. revenue, gross margin)',
  period_type      STRING        COMMENT 'Period type: Q = quarter, FY = full-year total',
  year             INT           COMMENT 'Year',
  quarter          INT           COMMENT 'Quarter (1-4), NULL for FY',
  is_estimate      BOOLEAN       COMMENT 'Whether the value is an estimate',
  value            DECIMAL(38,7) COMMENT 'Value in units (Excel values are in millions and the notebook multiplies by 1,000,000)',
  source_file      STRING        COMMENT 'Source file name',
  upload_user      STRING        COMMENT 'Uploader',
  upload_datetime  TIMESTAMP     COMMENT 'Upload time',
  data_version     STRING        COMMENT 'Data version key (format: V_Q{{quarter}}_{{year}}_ESTIMATE)',
  batch_key        STRING        COMMENT 'Batch key (sheet name, e.g. BLB_260723)'
)
COMMENT 'Bloomberg consensus model - normalized long table (Excel headers parsed dynamically, then unpivoted)'
""")

display(spark.sql(f"SHOW TABLES IN {fq_schema}"))
display(spark.sql(f"SHOW VOLUMES IN {fq_schema}"))

# COMMAND ----------

# DBTITLE 1,Grant the app's service principal access
from databricks.sdk import WorkspaceClient

app = WorkspaceClient().apps.get(app_name)
sp = app.service_principal_client_id
print(f"App service principal: {app.service_principal_name} ({sp})")

for stmt in [
    f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp}`",
    f"GRANT USE SCHEMA ON SCHEMA {fq_schema} TO `{sp}`",
    f"GRANT READ VOLUME, WRITE VOLUME ON VOLUME {fq_volume} TO `{sp}`",
    f"GRANT SELECT, MODIFY ON TABLE {history_table} TO `{sp}`",
]:
    spark.sql(stmt)
    print(f"✅ {stmt}")
