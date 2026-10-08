-- ============================================================
-- Excel Upload App - environment setup script
-- Replace <YOUR_CATALOG>, <YOUR_SCHEMA> and <YOUR_VOLUME> below,
-- then run each statement in order.
-- ============================================================

-- >>> Example values <<<
--   <YOUR_CATALOG> = my_catalog
--   <YOUR_SCHEMA>  = my_schema
--   <YOUR_VOLUME>  = bloomberg_files

-- 1. Create the catalog (skip if it already exists)
CREATE CATALOG IF NOT EXISTS <YOUR_CATALOG>;

-- 2. Create the schema
CREATE SCHEMA IF NOT EXISTS <YOUR_CATALOG>.<YOUR_SCHEMA>;

-- 3. Create the volume (stores the uploaded Excel files)
CREATE VOLUME IF NOT EXISTS <YOUR_CATALOG>.<YOUR_SCHEMA>.<YOUR_VOLUME>
  COMMENT 'Excel files uploaded through the app';

-- 4. Create the upload history table
CREATE TABLE IF NOT EXISTS <YOUR_CATALOG>.<YOUR_SCHEMA>.upload_history (
  file_name     STRING    COMMENT 'Uploaded file name (unique name stored in the volume)',
  uploaded_by   STRING    COMMENT 'Identity of the uploader',
  size_bytes    BIGINT    COMMENT 'File size in bytes',
  status        STRING    COMMENT 'Status (landed / ingested)',
  uploaded_at   TIMESTAMP COMMENT 'Upload time'
)
COMMENT 'One row per Excel upload';

-- 5. Ingest target table bloomberg_consensus_model (normalized long table)
--    The read_bloomberg notebook parses the Excel headers dynamically, unpivots,
--    and writes here. It de-duplicates on batch_key (= sheet name): an existing
--    batch is deleted and re-written, a new batch is appended.
--    Columns and types must match the notebook output (including data_version
--    and batch_key; value is DECIMAL(38,7)). The schema is fixed — new quarters
--    in the Excel need no change here.
--    You can create the table up front, or let the notebook create it on its first run.
CREATE TABLE IF NOT EXISTS <YOUR_CATALOG>.<YOUR_SCHEMA>.bloomberg_consensus_model (
  metric           STRING        COMMENT 'Metric name (e.g. revenue, gross margin)',
  period_type      STRING        COMMENT 'Period type: Q = quarter, FY = full-year total',
  year             INT           COMMENT 'Year',
  quarter          INT           COMMENT 'Quarter (1-4), NULL for FY',
  is_estimate      BOOLEAN       COMMENT 'Whether the value is an estimate',
  value            DECIMAL(38,7) COMMENT 'Value in units (Excel values are in millions and the notebook multiplies by 1,000,000)',
  source_file      STRING        COMMENT 'Source file name',
  upload_user      STRING        COMMENT 'Uploader',
  upload_datetime  TIMESTAMP     COMMENT 'Upload time',
  data_version     STRING        COMMENT 'Data version key (format: V_Q{quarter}_{year}_ESTIMATE)',
  batch_key        STRING        COMMENT 'Batch key (sheet name, e.g. BLB_260723)'
)
COMMENT 'Bloomberg consensus model - normalized long table (Excel headers parsed dynamically, then unpivoted)';

--    To rebuild the table from scratch (deletes all data):
--    DROP TABLE IF EXISTS <YOUR_CATALOG>.<YOUR_SCHEMA>.bloomberg_consensus_model;

-- 6. Verify
SHOW TABLES IN <YOUR_CATALOG>.<YOUR_SCHEMA>;
SHOW VOLUMES IN <YOUR_CATALOG>.<YOUR_SCHEMA>;
