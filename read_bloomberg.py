# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Introduction
# MAGIC %md
# MAGIC ## Read Bloomberg Excel File into a Delta Table
# MAGIC This notebook demonstrates how to:
# MAGIC 1. List files in a Unity Catalog Volume
# MAGIC 2. Read the first sheet of an Excel file using Spark
# MAGIC 3. Preview the data
# MAGIC 4. Write it as a managed Delta table

# COMMAND ----------

# DBTITLE 1,Step 1 Header
# MAGIC %md
# MAGIC ### Step 1: List Files in the Volume
# MAGIC We first explore the UC Volume to identify the Excel file we want to ingest.

# COMMAND ----------

# DBTITLE 1,File name parameter
# Job parameter: name of the Excel file (in the Volume) to ingest.
# Leave blank to automatically pick the most recently modified file.
dbutils.widgets.text("file_name", "", "Excel file name")

# COMMAND ----------

# DBTITLE 1,List volume files
# List files in the Bloomberg volume
volume_path = "/Volumes/my_catalog/my_schema/bloomberg_files/"
files = dbutils.fs.ls(volume_path)
display(files)

# COMMAND ----------

# DBTITLE 1,Resolve the file to ingest
from urllib.parse import unquote

# The app URL-encodes the file name; decode it back to the real name.
file_name = unquote(dbutils.widgets.get("file_name")).strip()

if file_name:
    excel_file = f"{volume_path}{file_name}"
else:
    # No file specified: pick the most recently modified Excel file in the Volume.
    excel_files = [f for f in files if f.name.lower().endswith((".xlsx", ".xls"))]
    if not excel_files:
        raise ValueError(f"No Excel files found in {volume_path}")
    latest = max(excel_files, key=lambda f: f.modificationTime)
    excel_file = latest.path.replace("dbfs:", "")
    file_name = latest.name

print(f"Ingesting file: {excel_file}")

# COMMAND ----------

# DBTITLE 1,Step 2 Header
# MAGIC %md
# MAGIC ### Step 2: Read the First Sheet of the Excel File
# MAGIC We use `pandas` to read only the first sheet (`sheet_name=0`) and convert it to a Spark DataFrame.

# COMMAND ----------

# DBTITLE 1,Install openpyxl
# If your workspace installs packages through a private PyPI mirror, add:
#   --index-url <your-pypi-mirror-url>
%pip install openpyxl

# COMMAND ----------

# DBTITLE 1,Read first sheet
import pandas as pd
import re
from datetime import datetime

# excel_file was resolved from the file_name parameter (or latest file) above.

# --- 解析 Sheet 名稱取得資料版本日期 (格式: BLB_YYMMDD) ---
xl = pd.ExcelFile(excel_file)
sheet_name = xl.sheet_names[0]
print(f"📄 Sheet name: {sheet_name}")

# 從 sheet 名稱中提取日期 (YYMMDD)
date_match = re.search(r'(\d{6})', sheet_name)
if date_match:
    date_str = date_match.group(1)
    batch_date = datetime.strptime(date_str, "%y%m%d").date()
    print(f"📅 資料批次日期: {batch_date} (parsed from '{sheet_name}')")
else:
    batch_date = None
    print(f"⚠️ 無法從 sheet 名稱 '{sheet_name}' 解析日期")

# Read the sheet; use row 4 (index 3) as the header
# Only read columns B:L (financial data)
pdf = pd.read_excel(xl, sheet_name=0, header=3, usecols="B:L")

# --- 只保留 B 欄中「文字版本」與「圖」之間的資料 ---
first_col = pdf.columns[0]
col_b = pdf[first_col].astype(str).str.strip()

# 找到「文字版本」標記列（資料從其下一列開始）
start_mask = col_b.eq("文字版本")
# 找到「圖」標記列（資料到其前一列結束）
end_mask = col_b.eq("圖")

start_idx = start_mask.idxmax() + 1 if start_mask.any() else 0
end_idx = end_mask.idxmax() if end_mask.any() else len(pdf)

pdf = pdf.iloc[start_idx:end_idx].reset_index(drop=True)
print(f"✂️ 擷取「文字版本」(row {start_idx}) 至「圖」(row {end_idx}) 之間的資料: {len(pdf)} 列")

# --- 移除 B 欄（第一欄）為空的列 ---
before = len(pdf)
pdf = pdf[pdf[first_col].notna() & (pdf[first_col].astype(str).str.strip() != "") & (pdf[first_col].astype(str).str.strip().str.lower() != "nan")].reset_index(drop=True)
print(f"🗑️ 移除 B 欄為空的列: {before - len(pdf)} 列")

# Convert mixed-type object columns to string for Arrow compatibility
for col in pdf.select_dtypes(include=['object']).columns:
    pdf[col] = pdf[col].astype(str)

# Convert to Spark DataFrame
df = spark.createDataFrame(pdf)
print(f"Rows: {df.count()}, Columns: {len(df.columns)}")
print(f"📋 batch_date = {batch_date}")

# COMMAND ----------

# DBTITLE 1,Step 3 Header
# MAGIC %md
# MAGIC ### Step 3: Preview the Data
# MAGIC Let's inspect the schema and see a sample of rows.

# COMMAND ----------

# DBTITLE 1,Preview data
df.printSchema()
display(df)

# COMMAND ----------

# DBTITLE 1,Step 4 Header
# MAGIC %md
# MAGIC ### Step 4: Dynamic Parsing + Unpivot → Normalized Delta Table
# MAGIC Dynamically parse Excel column headers with regex to extract `period_type` (Q/FY), `year`, `quarter`, and `is_estimate`, then unpivot from wide format into a fixed-schema long table. No hard-coded column mapping needed — future quarters are handled automatically.

# COMMAND ----------

# DBTITLE 1,Write Delta table
import re
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Dynamic column parsing — no hard-coded mapping needed
# Pattern: "Q{1-4} {year}" | "Q{1-4} {year} 預估" | "FY {year}" | "FY {year} 預估"
# ---------------------------------------------------------------------------
period_pattern = re.compile(r'^(Q[1-4]|FY)\s+(\d{4})(\s+預估)?$')

metric_col = None
period_cols = []

for col_name in df.columns:
    if "單位" in col_name:
        metric_col = col_name
    elif period_pattern.match(col_name.strip()):
        period_cols.append(col_name)
    else:
        print(f"⚠️ Unrecognized column (skipped): {col_name}")

if not metric_col:
    raise ValueError("Cannot find metric column (expected a column containing '單位')")

print(f"✅ Metric column: '{metric_col}'")
print(f"✅ Period columns ({len(period_cols)}): {period_cols}")

# ---------------------------------------------------------------------------
# Data Version: 取「Q{1-4} {year} 預估」欄位中最小季度作為資料版本鍵
# 格式: V_Q{min_quarter}_{year}_ESTIMATE
# ---------------------------------------------------------------------------
estimate_pattern = re.compile(r'^Q([1-4])\s+(\d{4})\s+預估$')
estimate_cols_parsed = []
for c in period_cols:
    m = estimate_pattern.match(c.strip())
    if m:
        estimate_cols_parsed.append((int(m.group(1)), int(m.group(2)), c))

if not estimate_cols_parsed:
    raise ValueError("No estimate columns (Q{1-4} {year} 預估) found — cannot derive data_version")

# 取時間最早的預估季度（先比年份再比季度，如 2026Q2 < 2027Q1）
estimate_cols_parsed.sort(key=lambda x: (x[1], x[0]))
min_quarter, min_year, _ = estimate_cols_parsed[0]
data_version = f"V_Q{min_quarter}_{min_year}_ESTIMATE"
batch_key = sheet_name
print(f"🔑 Data Version: {data_version}")
print(f"🔑 Batch Key (= sheet name): {batch_key}")

# ---------------------------------------------------------------------------
# Unpivot (wide → long) using stack()
# ---------------------------------------------------------------------------
stack_args = ", ".join([f"'{c}', `{c}`" for c in period_cols])
n = len(period_cols)

df_long = df.select(
    F.col(f"`{metric_col}`").alias("metric"),
    F.expr(f"stack({n}, {stack_args}) as (period_raw, value)")
)

# ---------------------------------------------------------------------------
# Parse period_raw → period_type, year, quarter, is_estimate
#   period_type: "Q" = 季, "FY" = 年加總
#   quarter: 1-4 for Q, null for FY
# ---------------------------------------------------------------------------
df_long = (
    df_long
    .withColumn("period_type",
        F.when(F.col("period_raw").rlike(r"^Q[1-4]"), F.lit("Q"))
         .otherwise(F.lit("FY"))
    )
    .withColumn("year",
        F.regexp_extract("period_raw", r"(\d{4})", 1).cast("int")
    )
    .withColumn("quarter",
        F.when(F.col("period_type") == "Q",
               F.regexp_extract("period_raw", r"Q(\d)", 1).cast("int"))
    )
    .withColumn("is_estimate",
        F.col("period_raw").contains("預估")
    )
    .drop("period_raw")
    .withColumn("value", F.expr("try_cast(value as decimal(38,7)) * 1000000"))  # 百萬 → 元，溢位回傳 NULL
    .filter(F.col("value").isNotNull())  # 移除非數值或溢位資料（例如重複的標題列）
)

# ---------------------------------------------------------------------------
# Metadata: source file + uploader info
# ---------------------------------------------------------------------------
history_table = "my_catalog.my_schema.upload_history"
upload_user = None
upload_datetime = None
try:
    hist = (
        spark.table(history_table)
        .where(F.col("file_name") == file_name)
        .orderBy(F.col("uploaded_at").desc())
        .limit(1)
        .collect()
    )
    if hist:
        upload_user = hist[0]["uploaded_by"]
        upload_datetime = hist[0]["uploaded_at"]
except Exception as e:
    print(f"⚠️ Could not look up uploader from {history_table}: {e}")

df_final = (
    df_long
    .withColumn("data_version", F.lit(data_version))  # 預估最小季度作為版本鍵
    .withColumn("source_file", F.lit(file_name))
    .withColumn("upload_user", F.lit(upload_user))
    .withColumn("upload_datetime", F.lit(upload_datetime).cast("timestamp"))
    .withColumn("batch_key", F.lit(batch_key))
)

# ---------------------------------------------------------------------------
# Write to Delta table — 以 batch_key (= sheet_name) 判斷是否重複匯入
# 邏輯:
#   1. 若表不存在 → 直接建表
#   2. 若同 batch_key 不存在 → 直接 append
#   3. 若同 batch_key 已存在 → 刪除舊資料再匯入
# ---------------------------------------------------------------------------
table_name = "my_catalog.my_schema.bloomberg_consensus_model"

if spark.catalog.tableExists(table_name):
    # 查詢同 batch_key 的現有資料
    existing_count = (
        spark.table(table_name)
        .where(F.col("batch_key") == batch_key)
        .count()
    )

    if existing_count > 0:
        # 同 batch_key 已存在 → 刪除後重新匯入
        spark.sql(f"DELETE FROM {table_name} WHERE batch_key = '{batch_key}'")
        print(f"🗑️ 已刪除 batch_key='{batch_key}' 舊資料: {existing_count} 筆")
        df_final.write.mode("append").saveAsTable(table_name)
        new_count = df_final.count()
        print(f"✅ 重新寫入 {new_count} 筆資料 (batch_key='{batch_key}')")
    else:
        # 同 batch_key 不存在 → 直接 append
        df_final.write.mode("append").saveAsTable(table_name)
        new_count = df_final.count()
        print(f"✅ 寫入 {new_count} 筆資料 (batch_key='{batch_key}' 首次匯入)")
else:
    # 首次建表
    df_final.write.mode("overwrite").saveAsTable(table_name)
    new_count = df_final.count()
    print(f"🆕 首次建立表: {table_name}")
    print(f"✅ 寫入 {new_count} 筆資料")

print(f"   data_version={data_version}")
print(f"   batch_key={batch_key}")
print(f"   source_file={file_name}, upload_user={upload_user}")

# COMMAND ----------

# DBTITLE 1,Step 5 Header
# MAGIC %md
# MAGIC ### Step 5: Verify the Table
# MAGIC Confirm the table was created and query it directly.

# COMMAND ----------

# DBTITLE 1,Verify table
# Show sample rows with the new normalized schema
display(spark.sql(f"SELECT * FROM {table_name} LIMIT 10"))

# Summary: row count by period_type
display(
    spark.sql(f"""
        SELECT period_type, year, is_estimate, COUNT(*) as row_count
        FROM {table_name}
        GROUP BY period_type, year, is_estimate
        ORDER BY period_type, year, is_estimate
    """)
)