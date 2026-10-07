-- ============================================================
-- Excel Upload App - 環境初始化腳本
-- 部署前請先修改以下三個參數，再逐段執行
-- ============================================================

-- >>> 修改此處 <<<
-- SET CATALOG = my_catalog;
-- SET SCHEMA  = my_schema;
-- SET VOLUME  = bloomberg_files;

-- 1. 建立 Catalog（若已存在可跳過）
CREATE CATALOG IF NOT EXISTS <您的_CATALOG>;

-- 2. 建立 Schema
CREATE SCHEMA IF NOT EXISTS <您的_CATALOG>.<您的_SCHEMA>;

-- 3. 建立 Volume（用於存放上傳的 Excel 檔案）
CREATE VOLUME IF NOT EXISTS <您的_CATALOG>.<您的_SCHEMA>.<您的_VOLUME>
  COMMENT '存放使用者上傳的 Excel 檔案';

-- 4. 建立上傳歷史紀錄表
CREATE TABLE IF NOT EXISTS <您的_CATALOG>.<您的_SCHEMA>.upload_history (
  file_name     STRING    COMMENT '上傳的檔案名稱',
  uploaded_by   STRING    COMMENT '上傳者身份',
  size_bytes    BIGINT    COMMENT '檔案大小（bytes）',
  status        STRING    COMMENT '狀態（landed / ingested）',
  uploaded_at   TIMESTAMP COMMENT '上傳時間'
)
COMMENT '記錄每次 Excel 上傳的歷史';

-- 5. 匯入目標表 bloomberg_consensus_model（正規化長表）
--    read_bloomberg Notebook 會動態解析 Excel 欄位並 unpivot 後以 append 寫入。
--    Schema 固定，未來新增季度不需調整。
--    可預先建立，或由 Notebook 首次執行時自動建立。
CREATE TABLE IF NOT EXISTS <您的_CATALOG>.<您的_SCHEMA>.bloomberg_consensus_model (
  metric           STRING    COMMENT '指標名稱（如營收、毛利率）',
  period_type      STRING    COMMENT '期間類型：Q=季, FY=年加總',
  year             INT       COMMENT '年度',
  quarter          INT       COMMENT '季度 (1-4)，FY 時為 null',
  is_estimate      BOOLEAN   COMMENT '是否為預估值',
  value            DOUBLE    COMMENT '數值（單位：新台幣百萬）',
  source_file      STRING    COMMENT '來源檔名',
  upload_user      STRING    COMMENT '上傳者',
  upload_datetime  TIMESTAMP COMMENT '上傳時間'
)
COMMENT 'Bloomberg consensus model - 正規化長表（動態解析 Excel 欄位後 unpivot）';

--    若要重建表結構（清空所有資料），可執行：
--    DROP TABLE IF EXISTS <您的_CATALOG>.<您的_SCHEMA>.bloomberg_consensus_model;

-- 6. 驗證建立成功
SHOW TABLES IN <您的_CATALOG>.<您的_SCHEMA>;
SHOW VOLUMES IN <您的_CATALOG>.<您的_SCHEMA>;
