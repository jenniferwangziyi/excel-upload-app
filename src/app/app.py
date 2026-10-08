"""Excel Upload App.

FastAPI backend that accepts Excel file uploads and saves them to a Unity
Catalog Volume. Each upload is stored under a unique name (original name +
uploader + timestamp) so uploads never overwrite one another, and it then
automatically triggers the "Read Bloomberg Excel to Delta Table" job to ingest
that specific file into the Delta table and run a data quality check on it.
"""

import io
import logging
import os
import re
import traceback
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, HTTPException, Request, UploadFile
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Excel Upload")

# Configuration — CATALOG / SCHEMA / VOLUME are set by the bundle
# (resources/excel_upload.app.yml) from the bundle variables.
CATALOG = os.environ.get("CATALOG", "")
SCHEMA = os.environ.get("SCHEMA", "")
VOLUME = os.environ.get("VOLUME", "")

# Land files in the same Volume the "Read Bloomberg Excel to Delta Table"
# notebook reads from, so the triggered job picks up uploaded files.
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# Upload history lives in a Delta table in the same schema as the data. The app
# writes one row per upload and reads it back ordered by most-recent-first.
HISTORY_TABLE = f"`{CATALOG}`.`{SCHEMA}`.upload_history"

# SQL warehouse used to read/write the history table. Set by the bundle; the app's
# service principal has CAN_USE on it and SELECT/MODIFY on the table.
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

# Ingest job triggered after each upload. Set by the bundle; the app's service
# principal has CAN_MANAGE_RUN on it. The job takes a `file_name` parameter.
INGEST_JOB_ID = os.environ.get("INGEST_JOB_ID", "")

# Cap how many history rows we surface to the UI (most recent first).
HISTORY_LIMIT = 10


def build_unique_filename(original_name: str, user: str) -> str:
    """Build a collision-free file name: <base>__<user>__<UTC-timestamp><ext>.

    The uploader and an upload timestamp are folded into the name so repeated
    uploads of the same source file never overwrite each other in the Volume,
    and each landed file is traceable to who uploaded it and when. User and base
    are sanitized to characters safe for a Volume path.
    """
    stem, ext = os.path.splitext(original_name)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def sanitize(s: str) -> str:
        # Keep letters/digits/dot/underscore/hyphen; collapse everything else to
        # '_'. Unicode letters (e.g. Chinese) are preserved via \w.
        s = re.sub(r"[^\w.\-]+", "_", s, flags=re.UNICODE).strip("_")
        return s or "file"

    return f"{sanitize(stem)}__{sanitize(user)}__{ts}{ext.lower()}"


def get_uploader(request: Request) -> str:
    """Return the signed-in user's identity from Databricks Apps headers.

    Databricks Apps inject the authenticated user via X-Forwarded-* headers.
    Falls back to 'unknown' when running locally without the proxy.
    """
    return (
        request.headers.get("X-Forwarded-Email")
        or request.headers.get("X-Forwarded-Preferred-Username")
        or request.headers.get("X-Forwarded-User")
        or "unknown"
    )


def _run_sql(w: WorkspaceClient, statement: str, parameters=None):
    """Execute a SQL statement on the configured warehouse and return the result.

    Uses parameter markers (:name) so values are never string-interpolated into
    SQL — this avoids injection from user-controlled filenames/identities.
    Raises RuntimeError if the warehouse is unset or the statement fails.
    """
    if not WAREHOUSE_ID:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set.")
    resp = w.statement_execution.execute_statement(
        statement=statement,
        warehouse_id=WAREHOUSE_ID,
        parameters=parameters or None,
        wait_timeout="30s",
    )
    state = resp.status.state if resp.status else None
    if state != StatementState.SUCCEEDED:
        msg = ""
        if resp.status and resp.status.error:
            msg = resp.status.error.message or ""
        raise RuntimeError(f"SQL statement {state}: {msg}")
    return resp


def record_upload(w: WorkspaceClient, file_name: str, user: str, size_bytes: int) -> None:
    """Insert one row into the history table (best-effort; never raises).

    A logging failure must not fail the upload itself, so all errors are caught
    and logged.
    """
    try:
        _run_sql(
            w,
            f"""
            INSERT INTO {HISTORY_TABLE}
                (file_name, uploaded_by, size_bytes, status, uploaded_at)
            VALUES (:file_name, :uploaded_by, :size_bytes, :status, current_timestamp())
            """,
            parameters=[
                StatementParameterListItem(name="file_name", value=file_name),
                StatementParameterListItem(name="uploaded_by", value=user),
                StatementParameterListItem(name="size_bytes", value=str(size_bytes), type="BIGINT"),
                StatementParameterListItem(name="status", value="landed"),
            ],
        )
    except Exception as e:
        logger.error(f"Failed to record upload history for {file_name}: {e}")


def read_history(w: WorkspaceClient) -> list[dict]:
    """Read upload history from the Delta table, most recent first."""
    resp = _run_sql(
        w,
        f"""
        SELECT file_name, uploaded_by, size_bytes, status,
               date_format(uploaded_at, "yyyy-MM-dd'T'HH:mm:ssXXX") AS uploaded_at
        FROM {HISTORY_TABLE}
        ORDER BY uploaded_at DESC
        LIMIT :lim
        """,
        parameters=[StatementParameterListItem(name="lim", value=str(HISTORY_LIMIT), type="INT")],
    )
    records = []
    data = resp.result.data_array if (resp.result and resp.result.data_array) else []
    for row in data:
        file_name, uploaded_by, size_bytes, status, uploaded_at = row
        records.append({
            "file_name": file_name,
            "user": uploaded_by,
            "size_bytes": int(size_bytes) if size_bytes is not None else None,
            "status": status,
            "uploaded_at": uploaded_at,
        })
    return records


def trigger_ingest_job(w: WorkspaceClient, file_name: str) -> int | None:
    """Trigger the ingest job for a specific file, passing file_name through.

    Returns the run_id on success, or None if the job isn't configured or the
    trigger fails. Never raises — a failed trigger must not fail the upload
    (the file is already safely landed in the Volume and can be ingested
    manually).
    """
    if not INGEST_JOB_ID:
        logger.warning("INGEST_JOB_ID not set; skipping automatic ingest trigger.")
        return None
    try:
        run = w.jobs.run_now(
            job_id=int(INGEST_JOB_ID),
            job_parameters={"file_name": file_name},
        )
        logger.info(f"Triggered ingest job {INGEST_JOB_ID} for {file_name}: run_id={run.run_id}")
        return run.run_id
    except Exception as e:
        logger.error(f"Failed to trigger ingest job for {file_name}: {e}")
        return None


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled error: {exc}\n{traceback.format_exc()}")
    return JSONResponse(
        status_code=500,
        content={"detail": f"Internal server error: {str(exc)}"},
    )


def get_workspace_client() -> WorkspaceClient:
    return WorkspaceClient()


@app.post("/api/upload")
async def upload_excel(request: Request, file: UploadFile = File(...)):
    """Upload an Excel file, land it under a unique name, and trigger ingest."""
    if not file.filename:
        raise HTTPException(status_code=400, detail="No file provided.")

    if not file.filename.lower().endswith(('.xlsx', '.xls')):
        raise HTTPException(
            status_code=400,
            detail="Only Excel files (.xlsx, .xls) are accepted."
        )

    uploader = get_uploader(request)
    original_name = file.filename
    # Unique name (original + uploader + timestamp) so uploads never overwrite.
    file_name = build_unique_filename(original_name, uploader)
    volume_file_path = f"{VOLUME_PATH}/{file_name}"
    content = await file.read()

    try:
        w = get_workspace_client()
    except Exception as e:
        logger.error(f"SDK auth failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize Databricks client: {str(e)}"
        )

    # Upload to Volume. overwrite=False guards against the (astronomically
    # unlikely) unique-name collision rather than silently clobbering.
    try:
        w.files.upload(
            file_path=volume_file_path,
            contents=io.BytesIO(content),
            overwrite=False,
        )
        logger.info(f"Uploaded {original_name} as {file_name} to {volume_file_path}")
    except Exception as e:
        logger.error(f"Volume upload failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload file to Volume: {str(e)}"
        )

    # Record the upload in the history table (best-effort; never blocks upload).
    record_upload(w, file_name, uploader, len(content))

    # Trigger the ingest job for this specific file (best-effort; never blocks).
    run_id = trigger_ingest_job(w, file_name)

    return {
        "status": "landed",
        "file_name": file_name,
        "original_name": original_name,
        "volume_path": volume_file_path,
        "ingest_run_id": run_id,
        "message": (
            f"File landed as {file_name}. "
            + (
                f"Ingest job triggered (run {run_id})."
                if run_id is not None
                else "Ingest job was not triggered automatically; run it manually."
            )
        ),
    }


@app.get("/api/job-status/{run_id}")
async def job_status(run_id: int):
    """Return the current status of an ingest job run."""
    try:
        w = get_workspace_client()
    except Exception as e:
        logger.error(f"SDK auth failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize Databricks client: {str(e)}"
        )

    try:
        run = w.jobs.get_run(run_id=run_id)
        state = run.state
        result_state = state.result_state.value if state.result_state else None
        life_cycle_state = state.life_cycle_state.value if state.life_cycle_state else None
        state_message = state.state_message or ""
        # Per-task states so the UI can tell ingest and quality_check apart.
        tasks = {}
        # A retried task appears once per attempt; keep only the latest attempt.
        latest = {}
        for t in run.tasks or []:
            prev = latest.get(t.task_key)
            if prev is None or (t.attempt_number or 0) >= (prev.attempt_number or 0):
                latest[t.task_key] = t
        for t in latest.values():
            ts = t.state
            task_result = ts.result_state.value if ts and ts.result_state else None
            error = None
            if task_result == "FAILED" and t.run_id:
                # The run-level message is generic; the task output has the real error.
                try:
                    error = w.jobs.get_run_output(run_id=t.run_id).error
                except Exception as e:
                    logger.warning(f"Could not fetch output for task {t.task_key}: {e}")
            tasks[t.task_key] = {
                "life_cycle_state": ts.life_cycle_state.value if ts and ts.life_cycle_state else None,
                "result_state": task_result,
                "error": error,
            }
        return {
            "run_id": run_id,
            "life_cycle_state": life_cycle_state,
            "result_state": result_state,
            "state_message": state_message,
            "tasks": tasks,
        }
    except Exception as e:
        logger.error(f"Failed to get job run status for run_id={run_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get job run status: {str(e)}"
        )


@app.get("/api/config")
async def config():
    """Return display configuration for the UI."""
    return {"volume": f"{CATALOG}.{SCHEMA}.{VOLUME}"}


@app.get("/api/history")
async def upload_history():
    """Return upload history from the Delta table, most recent first."""
    try:
        w = get_workspace_client()
    except Exception as e:
        logger.error(f"SDK auth failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize Databricks client: {str(e)}"
        )

    try:
        records = read_history(w)
    except Exception as e:
        logger.error(f"Failed to read upload history: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read upload history: {str(e)}"
        )
    return {"count": len(records), "history": records}


# Serve static frontend
static_dir = Path(__file__).parent / "static"
if static_dir.exists():
    app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")
else:
    @app.get("/")
    def root():
        return {"message": "Excel Upload API - static files not found"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
