# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC ## Data Quality Check
# MAGIC Runs as the `quality_check` task, after the `ingest` task, in the ingest job.
# MAGIC
# MAGIC **Check:** every ingested row's `batch_key` (the Excel sheet name) must follow the
# MAGIC `BLB_YYMMDD` format, and `YYMMDD` must be a real calendar date. The sheet name is
# MAGIC the batch's data-version date, so a sheet named e.g. `Sheet1` or `BLB_261340` means
# MAGIC the batch cannot be dated or de-duplicated reliably.
# MAGIC
# MAGIC If any row fails, the task — and therefore the job run — fails and lists the bad
# MAGIC values. The rows stay in the table so they can be inspected; see the README for the
# MAGIC cleanup query.

# COMMAND ----------

# DBTITLE 1,Configuration
# Catalog and schema come from job parameters (set by the bundle). The defaults
# here only apply when running the notebook interactively.
dbutils.widgets.text("catalog", "my_catalog", "Catalog")
dbutils.widgets.text("schema", "excel_upload", "Schema")
dbutils.widgets.text("file_name", "", "Excel file name")
table_name = f"`{dbutils.widgets.get('catalog').strip()}`.`{dbutils.widgets.get('schema').strip()}`.bloomberg_consensus_model"

# Expected batch_key format: a regex whose first group is the date part, and the
# datetime pattern (Spark syntax) that date part must parse with.
BATCH_KEY_REGEX = r"^BLB_(\d{6})$"
BATCH_DATE_FORMAT = "yyMMdd"

# Task key of the upstream ingest task (it publishes the ingested file name).
INGEST_TASK_KEY = "ingest"

# COMMAND ----------

# DBTITLE 1,Resolve which rows to check
from urllib.parse import unquote

# In a job run, check only the file the ingest task just wrote. When run
# interactively there is no upstream task, so fall back to the file_name widget,
# and to the whole table if that is blank too.
source_file = dbutils.jobs.taskValues.get(
    taskKey=INGEST_TASK_KEY, key="source_file", default="", debugValue=""
) or unquote(dbutils.widgets.get("file_name")).strip()

df = spark.table(table_name)
if source_file:
    df = df.where(df.source_file == source_file)
    print(f"Checking rows from source_file = {source_file}")
else:
    print(f"No source file given — checking all rows in {table_name}")

# COMMAND ----------

# DBTITLE 1,Check batch_key date format
from pyspark.sql import functions as F

# regexp_extract returns '' when the pattern doesn't match, and try_to_timestamp
# returns NULL for '' or an impossible date — so NULL batch_date means "failed".
checked = df.withColumn(
    "batch_date",
    F.try_to_timestamp(
        F.regexp_extract("batch_key", BATCH_KEY_REGEX, 1), F.lit(BATCH_DATE_FORMAT)
    ),
)

summary = (
    checked.groupBy("batch_key")
    .agg(F.count("*").alias("row_count"), F.first("batch_date").alias("batch_date"))
    .withColumn("passed", F.col("batch_date").isNotNull())
    .orderBy("batch_key")
)
display(summary)

total_rows = checked.count()
failures = summary.where(~F.col("passed")).collect()
failed_rows = sum(r["row_count"] for r in failures)

if total_rows == 0:
    raise AssertionError(f"Quality check found no rows to check in {table_name} (source_file={source_file!r}).")

if failures:
    bad = ", ".join(repr(r["batch_key"]) for r in failures)
    raise AssertionError(
        f"Quality check FAILED: {failed_rows} of {total_rows} rows have a batch_key that does not "
        f"match {BATCH_KEY_REGEX} with a valid {BATCH_DATE_FORMAT} date: {bad}. "
        f"Rename the Excel sheet (e.g. BLB_260723) and upload the file again."
    )

print(f"✅ Quality check passed: {total_rows} rows, every batch_key is a valid {BATCH_DATE_FORMAT} date.")
