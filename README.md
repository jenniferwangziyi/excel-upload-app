# Excel 上傳與資料匯入系統 — 部署指南

本專案提供一個 Databricks App，讓使用者透過網頁介面上傳 Excel 檔案至 Unity Catalog Volume。每次上傳會以**唯一檔名**存檔（原始檔名 + 上傳者 + 上傳時間，因此不會互相覆蓋），並**自動觸發** Job 將該檔案匯入 Delta Table。

> **本版本主要變更**
> 1. **唯一檔名**：上傳的檔案以 `原始檔名__上傳者__時間戳記.副檔名` 存檔，多次上傳同一份檔案不會覆蓋。
> 2. **自動觸發匯入**：上傳成功後，App 會自動以 `run_now` 觸發匯入 Job，並將唯一檔名帶入 Job 的 `file_name` 參數。
> 3. **動態欄位解析 + 正規化長表**：Notebook 以正則動態解析 Excel 欄位名稱（如 `Q1 2026`、`Q2 2026 預估`、`FY 2027 預估`），自動萃取 `period_type`（Q/FY）、`year`、`quarter`、`is_estimate`，並 unpivot 為固定 schema 的長表。未來 Excel 新增季度無需改 code。
> 4. **批次去重 + 版本欄位**：以 `batch_key`（= Sheet 名稱）判斷是否重複匯入——同批次先刪除舊資料再寫入，新批次直接 `append`；並附加 `data_version`、`source_file`、`upload_user`、`upload_datetime` 以區分不同版本。

---

## 專案架構

```
excel-upload-ingestion/
├── app.py              # FastAPI 後端（唯一檔名上傳 + 歷史紀錄 + 觸發 Job）
├── app.yaml            # Databricks App 設定檔（Warehouse ID + Ingest Job ID）
├── requirements.txt    # Python 套件相依
├── setup.sql           # 環境初始化 SQL（Catalog / Schema / Volume / upload_history）
├── static/
│   └── index.html      # 前端上傳介面
├── read_bloomberg.py   # Notebook（Databricks notebook source 格式）：讀取 Excel、加上版本欄位、append 寫入 Delta Table
└── README.md           # 本文件
```

---

## 前置需求

1. Databricks Workspace（支援 Unity Catalog）
2. 一個 SQL Warehouse（Serverless 或 Pro 皆可）
3. 具備以下權限的使用者或 Service Principal：
   - 對目標 Catalog/Schema 的 `CREATE TABLE`、`CREATE VOLUME` 權限
   - 對 SQL Warehouse 的 `CAN USE` 權限
   - 對目標 Volume 的 `READ VOLUME` / `WRITE VOLUME` 權限

---

## 部署步驟

### 步驟 1：建立 Catalog、Schema 與 Volume

請在 SQL Editor 或 Notebook 中執行以下 SQL，將參數替換為您的環境值：

```sql
-- ============================
-- 請修改以下參數
-- ============================
-- CATALOG 名稱（如已有可跳過 CREATE CATALOG）
-- SCHEMA 名稱
-- VOLUME 名稱

CREATE CATALOG IF NOT EXISTS <您的_CATALOG>;
CREATE SCHEMA IF NOT EXISTS <您的_CATALOG>.<您的_SCHEMA>;
CREATE VOLUME IF NOT EXISTS <您的_CATALOG>.<您的_SCHEMA>.<您的_VOLUME>
  COMMENT 'Volume for uploaded Excel files';
```

### 步驟 2：建立上傳歷史紀錄表

App 會將每次上傳記錄寫入一張 Delta Table，請先建立此表：

```sql
CREATE TABLE IF NOT EXISTS <您的_CATALOG>.<您的_SCHEMA>.upload_history (
  file_name     STRING    COMMENT '上傳的檔案名稱',
  uploaded_by   STRING    COMMENT '上傳者身份',
  size_bytes    BIGINT    COMMENT '檔案大小（bytes）',
  status        STRING    COMMENT '狀態（landed / ingested）',
  uploaded_at   TIMESTAMP COMMENT '上傳時間'
)
COMMENT '記錄每次 Excel 上傳的歷史';
```

### 步驟 3：修改程式碼中的參數

以下列出所有需要依照您的環境修改的位置：

#### `app.py`

| 行號 | 變數 | 預設值 | 說明 |
| --- | --- | --- | --- |
| 32 | `VOLUME_PATH` | `/Volumes/my_catalog/my_schema/bloomberg_files` | 改為 `/Volumes/<CATALOG>/<SCHEMA>/<VOLUME>` |
| 36 | `HISTORY_TABLE` | `my_catalog.my_schema.upload_history` | 改為 `<CATALOG>.<SCHEMA>.upload_history` |

#### `app.yaml`

| 變數 | 預設值 | 說明 |
| --- | --- | --- |
| `DATABRICKS_WAREHOUSE_ID` | `<YOUR_WAREHOUSE_ID>` | 改為您的 SQL Warehouse ID（讀寫 `upload_history` 用） |
| `INGEST_JOB_ID` | `<YOUR_INGEST_JOB_ID>` | 改為您的匯入 Job ID（上傳後自動觸發） |

> 💡 Warehouse ID 可從 SQL Warehouses 頁面的 URL 或「Connection Details」中取得。Job ID 可從 Jobs 頁面該 Job 的 URL 取得（`/jobs/<JOB_ID>`）。

#### `read_bloomberg` Notebook

| Cell 標題 | 變數 | 預設值 | 說明 |
| --- | --- | --- | --- |
| List volume files | `volume_path` | `/Volumes/my_catalog/my_schema/bloomberg_files/` | 改為 `/Volumes/<CATALOG>/<SCHEMA>/<VOLUME>/` |
| Write Delta table | `history_table` | `my_catalog.my_schema.upload_history` | 改為 `<CATALOG>.<SCHEMA>.upload_history`（查詢上傳者/時間用） |
| Write Delta table | `table_name` | `my_catalog.my_schema.bloomberg_consensus_model` | 改為 `<CATALOG>.<SCHEMA>.<目標TABLE名稱>` |

> **欄位解析方式**：Notebook 以正則 `^(Q[1-4]|FY)\s+(\d{4})(\s+預估)?$` 動態辨識 Excel 欄位，自動萃取 `period_type`、`year`、`quarter`、`is_estimate`，再 unpivot 為正規化長表。不需手動維護欄位對應。
>
> **目標表 Schema（固定）**：
> | 欄位 | 型別 | 說明 |
> | --- | --- | --- |
> | `metric` | STRING | 指標名稱（如「營收」） |
> | `period_type` | STRING | `Q`（季）或 `FY`（年加總） |
> | `year` | INT | 年度 |
> | `quarter` | INT | 季度（1-4），FY 時為 null |
> | `is_estimate` | BOOLEAN | 是否為預估值 |
> | `value` | DECIMAL(38,7) | 數值（單位：新台幣元；Excel 原始單位為百萬，Notebook 已 ×1,000,000） |
> | `source_file` | STRING | 來源檔名 |
> | `upload_user` | STRING | 上傳者 |
> | `upload_datetime` | TIMESTAMP | 上傳時間 |
> | `data_version` | STRING | 資料版本鍵（`V_Q{最早預估季度}_{年度}_ESTIMATE`） |
> | `batch_key` | STRING | 批次鍵（= Sheet 名稱，如 `BLB_260723`） |
>
> **寫入模式**：Notebook 以 `batch_key`（= Sheet 名稱）判斷是否重複匯入：表不存在時自動建表；同 `batch_key` 不存在時直接 `append`；同 `batch_key` 已存在時先 `DELETE` 舊資料再寫入（schema 固定，不需 `mergeSchema`）。若以 `setup.sql` 預先建表，欄位與型別需與上表一致。`upload_user` / `upload_datetime` 由 Notebook 依 `file_name` 從 `upload_history` 查得（手動執行且查無紀錄時為 NULL）。

#### `static/index.html`

| 行號 | 內容 | 說明 |
| --- | --- | --- |
| 139 | `my_catalog.my_schema.bloomberg_files` | 改為 `<CATALOG>.<SCHEMA>.<VOLUME>`（僅顯示用，不影響功能） |

### 步驟 4：部署 Databricks App

1. 將整個 `excel-upload-ingestion/` 資料夾上傳至您的 Workspace
2. 在 Workspace 左側導覽列中點選 **Compute** → **Apps**
3. 點選 **Create App**，填入：
   - **Name**：自訂名稱（如 `excel-upload`）
   - **Source code path**：指向上傳的資料夾路徑
4. 確認 App 使用的 Service Principal 具備以下權限：
   - `CAN USE` 您指定的 SQL Warehouse
   - `SELECT` + `MODIFY` 在 `upload_history` 表
   - `READ VOLUME` / `WRITE VOLUME` 在目標 Volume
   - `CAN MANAGE RUN` 在匯入 Job（上傳後自動觸發所需）
5. 點選 **Deploy** 啟動 App

### 步驟 5：建立匯入 Job（由 App 自動觸發）

1. 進入 **Jobs** 頁面，點選 **Create Job**
2. 新增一個 Task：
   - **Type**：Notebook
   - **Source**：選擇 `read_bloomberg` notebook 路徑
3. 在 Job 層級新增參數 `file_name`（**必要**）：App 觸發時會帶入剛上傳檔案的唯一檔名；留空手動執行時，Notebook 會自動挑選 Volume 中最新的檔案
4. 設定 Cluster：建議使用 Serverless 或 Single Node
5. 儲存 Job，並將其 **Job ID** 填入 `app.yaml` 的 `INGEST_JOB_ID`
6. 將 App 的 Service Principal 加入此 Job 的權限，設為 `CAN MANAGE RUN`
7. （選用）若仍需定時排程，可額外設定 Schedule

> 本版本上傳後會**自動觸發**此 Job，一般不需再手動執行。

---

## 使用流程

```
使用者上傳 Excel  →  以唯一檔名存入 Volume  →  App 自動觸發 Job  →  動態解析 + Unpivot  →  append 寫入 Delta Table
   (App UI)         (原始名__上傳者__時間)      (帶入 file_name)     (正則萃取維度欄位)     (正規化長表，固定 schema)
```

1. 開啟 App 網址，透過拖放或選取上傳 Excel 檔案（.xlsx / .xls）
2. 上傳成功後，檔案會以唯一檔名（`原始檔名__上傳者__時間戳記`）落地在 Unity Catalog Volume 中，不會覆蓋既有檔案
3. App 自動以該唯一檔名觸發「Read Bloomberg Excel to Delta Table」Job
4. Job 動態解析 Excel 欄位名稱，萃取 `period_type` / `year` / `quarter` / `is_estimate`，unpivot 為正規化長表後寫入 `bloomberg_consensus_model`（同 `batch_key` 會取代舊資料）
5. 匯入完成後即可在 SQL Editor 或 Dashboard 中查詢資料，並依版本欄位區分不同上傳

---

## 參數對照表（快速參考）

| 參數 | 說明 | 範例 |
| --- | --- | --- |
| `<CATALOG>` | Unity Catalog 名稱 | `my_catalog` |
| `<SCHEMA>` | Schema 名稱 | `my_schema` |
| `<VOLUME>` | Volume 名稱（存放 Excel） | `bloomberg_files` |
| `<WAREHOUSE_ID>` | SQL Warehouse ID | `abcdef1234567890` |
| `<目標TABLE名稱>` | 匯入後的 Delta Table 名稱 | `bloomberg_consensus_model` |

---

## 常見問題

**Q：上傳後 Job 沒有自動執行？**
A：本版本上傳成功後會自動觸發匯入 Job。若沒有觸發，請確認：(1) `app.yaml` 的 `INGEST_JOB_ID` 是否正確；(2) App 的 Service Principal 是否具備該 Job 的 `CAN MANAGE RUN` 權限。App 的成功訊息會顯示觸發的 run id；觸發失敗時檔案仍會保留在 Volume，可手動執行 Job。

**Q：上傳歷史沒有顯示？**
A：請確認 `upload_history` 表已建立，且 App 的 Service Principal 對該表有 `SELECT` 與 `MODIFY` 權限。

**Q：Excel 新增了新的季度欄位（如 Q1 2028），需要改 code 嗎？**
A：不需要。Notebook 以正則動態解析欄位名稱，只要格式符合 `Q{1-4} {年份}` 或 `FY {年份}`（可選 `預估` 後綴），即可自動處理。

---

## 注意事項

- Notebook 預設以 Excel 第 4 列作為表頭（`header=3`），前 3 列視為 metadata，讀取 B～L 欄（`usecols="B:L"`），請確認您的 Excel 格式一致
- Excel 欄位名稱必須符合格式：`Q{1-4} {年份}`、`Q{1-4} {年份} 預估`、`FY {年份}`、`FY {年份} 預估`，以及一欄包含「單位」的指標名稱欄
- 每次執行 Job 會將 unpivot 後的正規化資料寫入 Delta Table（schema 固定，不需 `mergeSchema`）。同一 Sheet 名稱（`batch_key`）重複上傳會取代該批次舊資料；不同批次會並存，如需只保留最新版本，可在查詢時依 `batch_key` 或 `upload_datetime` 取最新
- 因為採唯一檔名，Volume 中的檔案會隨上傳次數累積；如需節省儲存空間，可定期清理舊檔（不影響已匯入的表資料）
- 建議部署完成後先用測試檔案驗證整個流程

---

## 使用 Genie Code 協助部署

您可以將以下提示文字複製貼上至 Databricks 的 Genie Code（AI 助手），讓它協助您完成參數修改與部署：

---

**複製以下內容，貼至 Genie Code 對話框：**

````
我需要你幫我部署「excel-upload-ingestion」專案。這個專案位於我的 Workspace 中的 <貼上你的資料夾路徑> 資料夾。

請幫我完成以下工作：

1. 執行以下 SQL 來建立所需的基礎架構：
   - Catalog: <填入你的 CATALOG 名稱>
   - Schema: <填入你的 SCHEMA 名稱>
   - Volume: <填入你的 VOLUME 名稱>
   - 建立 upload_history 表（欄位：file_name STRING, uploaded_by STRING, size_bytes BIGINT, status STRING, uploaded_at TIMESTAMP）

2. 修改 app.py 中的參數：
   - VOLUME_PATH 改為: /Volumes/<CATALOG>/<SCHEMA>/<VOLUME>
   - HISTORY_TABLE 改為: <CATALOG>.<SCHEMA>.upload_history

3. 修改 app.yaml 中的環境變數：
   - DATABRICKS_WAREHOUSE_ID 改為: <填入你的 SQL Warehouse ID>
   - INGEST_JOB_ID 改為: <填入你的匯入 Job ID>

4. 修改 read_bloomberg notebook 中的：
   - volume_path 改為: /Volumes/<CATALOG>/<SCHEMA>/<VOLUME>/
   - history_table 改為: <CATALOG>.<SCHEMA>.upload_history
   - table_name 改為: <CATALOG>.<SCHEMA>.<你想要的 TABLE 名稱>
   - 確認寫入邏輯維持以 batch_key 去重（同批次先刪除再寫入，新批次 append；schema 固定為正規化長表，不需 mergeSchema）

5. 修改 static/index.html 顯示文字中的 Volume 路徑，改為我的 Volume 路徑

6. 幫我建立一個 Job 來執行 read_bloomberg notebook（Serverless compute），並在 Job 層級新增 file_name 參數；建立後把 Job ID 填回 app.yaml 的 INGEST_JOB_ID

7. 幫我部署 Databricks App，source code 指向這個資料夾，並確認 App 的 Service Principal 具備 Warehouse 的 CAN USE、upload_history 表的 SELECT/MODIFY、Volume 的 WRITE、以及匯入 Job 的 CAN MANAGE RUN 權限

請逐步執行，每一步完成後告訴我結果。
````

> 💡 **使用提示**：將 `< >` 中的佔位符替換為您的實際值後再貼上。如果您不確定 SQL Warehouse ID，可以先問 Genie Code：「請幫我列出可用的 SQL Warehouses 和它們的 ID」。
