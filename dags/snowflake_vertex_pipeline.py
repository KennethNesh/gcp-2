"""
Snowflake → Vertex AI Agent Pipeline
=====================================
Cloud Composer (Airflow) DAG that:
  1. Reads a high-water-mark timestamp from an Airflow Variable.
  2. Queries a Snowflake table for rows inserted since that timestamp.
  3. Sends each batch of new rows to a Vertex AI Agent that returns "Hi".
  4. Updates the high-water-mark so the next run picks up only newer rows.

Prerequisites
-------------
- A Cloud Composer 2 environment with the
  `apache-airflow-providers-snowflake` and
  `google-cloud-aiplatform` PyPI packages installed.
- An Airflow Connection named `snowflake_default` configured with
  your Snowflake account, user, password/key, warehouse, database,
  schema, and role.
- Airflow Variables:
    SNOWFLAKE_DATABASE       – Snowflake database name (e.g. MY_DB)
    SNOWFLAKE_SCHEMA         – Snowflake schema name  (e.g. MY_SCHEMA)
    SNOWFLAKE_TABLE          – table name              (e.g. test)
    SNOWFLAKE_TIMESTAMP_COL  – column used for incremental extraction
                               (default: CREATED_AT)
    GCP_PROJECT_ID           – your GCP project ID
    GCP_REGION               – region for Vertex AI (default: us-central1)
"""

from __future__ import annotations

import json
import logging
import traceback
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from google.cloud import aiplatform
from vertexai.generative_models import GenerativeModel

# ---------------------------------------------------------------------------
# Configuration (pulled from Airflow Variables at parse time)
# ---------------------------------------------------------------------------
SNOWFLAKE_CONN_ID = "snowflake_default"

log = logging.getLogger(__name__)

# Load each variable individually with detailed logging so you can tell
# at DAG-parse time exactly which values are being used and which fell
# back to defaults.


def _load_variable(key: str, default: str) -> str:
    """Load an Airflow Variable and log whether we got a real value or the default."""
    try:
        value = Variable.get(key)
        log.info(
            "[CONFIG] Airflow Variable '%s' = '%s' (loaded from Variables store)",
            key,
            value,
        )
        return value
    except KeyError:
        log.warning(
            "[CONFIG] Airflow Variable '%s' NOT FOUND — falling back to default: '%s'. "
            "Set this variable in Admin → Variables if the default is wrong.",
            key,
            default,
        )
        return default


# Database and schema are read from the Airflow *Connection* (snowflake_default)
# that you configured in Admin → Connections.  We do NOT override them with
# Variables so the connection remains the single source of truth.
# If you ever need to override them, set these Variables and uncomment the hook
# overrides in extract_new_entries().
# SNOWFLAKE_DATABASE = _load_variable("SNOWFLAKE_DATABASE", "MY_DB")
# SNOWFLAKE_SCHEMA   = _load_variable("SNOWFLAKE_SCHEMA",   "MY_SCHEMA")

SNOWFLAKE_TABLE = _load_variable("SNOWFLAKE_TABLE", "test")
TIMESTAMP_COL = _load_variable("SNOWFLAKE_TIMESTAMP_COL", "CREATED_AT")
GCP_PROJECT_ID = _load_variable("GCP_PROJECT_ID", "my-gcp-project")
GCP_REGION = _load_variable("GCP_REGION", "us-central1")

log.info(
    "[CONFIG] Table target: %s  (database & schema come from the '%s' connection)",
    SNOWFLAKE_TABLE,
    SNOWFLAKE_CONN_ID,
)

# Airflow Variable that persists the high-water-mark between runs.
HWM_VARIABLE_KEY = "snowflake_pipeline_hwm"


# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

# ---------------------------------------------------------------------------
# Task implementations
# ---------------------------------------------------------------------------


def _get_last_hwm() -> str:
    """Return the stored high-water-mark, or epoch if first run."""
    try:
        hwm = Variable.get(HWM_VARIABLE_KEY)
        log.info("[HWM] Loaded existing high-water-mark: %s", hwm)
        return hwm
    except KeyError:
        log.warning(
            "[HWM] No high-water-mark variable '%s' found — "
            "this appears to be the FIRST RUN. "
            "Defaulting to epoch (1970-01-01T00:00:00Z) to fetch all rows.",
            HWM_VARIABLE_KEY,
        )
        return "1970-01-01T00:00:00Z"


def extract_new_entries(**context) -> None:
    """
    Task 1 – Query Snowflake for rows where TIMESTAMP_COL > last HWM.
    Pushes the result set and the new HWM to XCom.
    """
    log.info("=" * 70)
    log.info("[EXTRACT] ▶ Starting extract_new_entries task")
    log.info("=" * 70)

    # -- 1. Get high-water-mark -------------------------------------------
    hwm = _get_last_hwm()

    # -- 2. Log the query we're about to run ------------------------------
    # Use just the table name — the database & schema context is set by
    # the Airflow Connection (snowflake_default), so no need to fully-qualify.
    query = f"""
        SELECT *
        FROM   {SNOWFLAKE_TABLE}
        WHERE  {TIMESTAMP_COL} > %(hwm)s
        ORDER  BY {TIMESTAMP_COL} ASC
    """
    log.info("[EXTRACT] Target table          : %s", SNOWFLAKE_TABLE)
    log.info("[EXTRACT] Timestamp column      : %s", TIMESTAMP_COL)
    log.info("[EXTRACT] High-water-mark (HWM) : %s", hwm)
    log.info(
        "[EXTRACT] Database & schema are taken from the Airflow connection '%s'. "
        "Check Admin → Connections if they are wrong.",
        SNOWFLAKE_CONN_ID,
    )
    log.info("[EXTRACT] SQL query:\n%s", query.strip())
    log.info("[EXTRACT] Query parameter       : hwm = '%s'", hwm)

    # -- 3. Connect to Snowflake ------------------------------------------
    log.info("[EXTRACT] Creating SnowflakeHook with conn_id='%s'", SNOWFLAKE_CONN_ID)
    log.info(
        "[EXTRACT] Database & schema will come from the '%s' connection "
        "(NOT overridden by Variables).",
        SNOWFLAKE_CONN_ID,
    )
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    log.info("[EXTRACT] Establishing Snowflake connection …")
    try:
        conn = hook.get_conn()
        log.info("[EXTRACT] ✅ Snowflake connection established successfully.")
    except Exception as exc:
        log.error("[EXTRACT] ❌ FAILED to connect to Snowflake!")
        log.error("[EXTRACT] Connection ID: '%s'", SNOWFLAKE_CONN_ID)
        log.error("[EXTRACT] Error type: %s", type(exc).__name__)
        log.error("[EXTRACT] Error details: %s", exc)
        log.error("[EXTRACT] Traceback:\n%s", traceback.format_exc())
        log.error(
            "[EXTRACT] 💡 TIP: Check that the Airflow connection 'snowflake_default' "
            "is configured correctly in Admin → Connections. Verify account, user, "
            "password, warehouse, database, schema, and role."
        )
        raise

    cursor = conn.cursor()

    # -- 4. Execute the query ---------------------------------------------
    try:
        # Ensure Snowflake session uses UTC so timestamps match Airflow's UTC.
        log.info("[EXTRACT] Setting Snowflake session timezone to UTC …")
        cursor.execute("ALTER SESSION SET TIMEZONE = 'UTC'")
        log.info("[EXTRACT] ✅ Session timezone set to UTC.")

        log.info("[EXTRACT] Executing query against Snowflake …")
        cursor.execute(query, {"hwm": hwm})
        columns = [desc[0] for desc in cursor.description]
        log.info("[EXTRACT] Result columns: %s", columns)

        rows = cursor.fetchall()
        log.info("[EXTRACT] ✅ Query returned %d row(s).", len(rows))
    except Exception as exc:
        log.error("[EXTRACT] ❌ FAILED to execute Snowflake query!")
        log.error("[EXTRACT] Query was:\n%s", query.strip())
        log.error("[EXTRACT] Error type: %s", type(exc).__name__)
        log.error("[EXTRACT] Error details: %s", exc)
        log.error("[EXTRACT] Traceback:\n%s", traceback.format_exc())

        # --- Diagnostic: try to list actual columns so the user can see them ---
        try:
            diag_cursor = conn.cursor()
            diag_cursor.execute(f"DESCRIBE TABLE {SNOWFLAKE_TABLE}")
            table_columns = [row[0] for row in diag_cursor.fetchall()]
            diag_cursor.close()
            log.error(
                "[EXTRACT] 🔎 DIAGNOSTIC — Actual columns in table '%s': %s",
                SNOWFLAKE_TABLE,
                table_columns,
            )
            log.error(
                "[EXTRACT] 💡 The query used column '%s' but it was not found. "
                "Set the Airflow Variable SNOWFLAKE_TIMESTAMP_COL to one of "
                "the columns listed above.",
                TIMESTAMP_COL,
            )
        except Exception:
            log.warning(
                "[EXTRACT] Could not run DESCRIBE TABLE %s for diagnostics.",
                SNOWFLAKE_TABLE,
            )

        log.error(
            "[EXTRACT] 💡 TIP: Common causes:\n"
            "  - Table '%s' does not exist in the database/schema set in the "
            "'%s' connection (check Admin → Connections)\n"
            "  - Column '%s' does not exist (check SNOWFLAKE_TIMESTAMP_COL variable)\n"
            "  - The role/user does not have SELECT permission on this table\n"
            "  - Database or schema in the connection is incorrect",
            SNOWFLAKE_TABLE,
            SNOWFLAKE_CONN_ID,
            TIMESTAMP_COL,
        )
        raise
    finally:
        log.info("[EXTRACT] Closing cursor and connection.")
        cursor.close()
        conn.close()

    # -- 5. Process results -----------------------------------------------
    records = [dict(zip(columns, row)) for row in rows]
    log.info("[EXTRACT] Fetched %d new record(s) from Snowflake.", len(records))

    if records:
        # Log a preview of the first few records (up to 3)
        preview_count = min(3, len(records))
        for i, rec in enumerate(records[:preview_count]):
            log.info("[EXTRACT] Record %d/%d preview: %s", i + 1, len(records), rec)
        if len(records) > preview_count:
            log.info("[EXTRACT] … and %d more record(s).", len(records) - preview_count)

    # -- 6. Determine new HWM ---------------------------------------------
    if records:
        new_hwm = max(str(r[TIMESTAMP_COL]) for r in records)
        log.info("[EXTRACT] New HWM determined from batch: %s", new_hwm)
    else:
        new_hwm = hwm
        log.info("[EXTRACT] No new records — HWM stays at: %s", new_hwm)

    # -- 7. Push data downstream via XCom ---------------------------------
    log.info("[EXTRACT] Pushing %d record(s) and HWM='%s' to XCom.", len(records), new_hwm)
    context["ti"].xcom_push(key="new_records", value=json.dumps(records, default=str))
    context["ti"].xcom_push(key="new_hwm", value=new_hwm)
    context["ti"].xcom_push(key="record_count", value=len(records))
    log.info("[EXTRACT] ✅ extract_new_entries task completed successfully.")


def call_vertex_ai_agent(**context) -> None:
    """
    Task 2 – Send extracted entries to a Vertex AI (Gemini) agent
    that replies with "Hi".
    """
    log.info("=" * 70)
    log.info("[VERTEX-AI] ▶ Starting call_vertex_ai_agent task")
    log.info("=" * 70)

    ti = context["ti"]

    # -- 1. Pull records from XCom ----------------------------------------
    log.info("[VERTEX-AI] Pulling records from XCom (task: extract_new_entries) …")
    records_json = ti.xcom_pull(task_ids="extract_new_entries", key="new_records")

    if records_json is None:
        log.warning(
            "[VERTEX-AI] ⚠ xcom_pull returned None — this usually means the "
            "upstream task 'extract_new_entries' did not push 'new_records'. "
            "Check if that task completed successfully."
        )
        records = []
    else:
        records = json.loads(records_json)
        log.info("[VERTEX-AI] Received %d record(s) from XCom.", len(records))

    if not records:
        log.info("[VERTEX-AI] No new entries — skipping Vertex AI call.")
        ti.xcom_push(key="agent_response", value="No new entries to process.")
        log.info("[VERTEX-AI] ✅ Task finished (no-op: zero records).")
        return

    # -- 2. Initialise Vertex AI SDK --------------------------------------
    log.info(
        "[VERTEX-AI] Initialising Vertex AI SDK — project='%s', location='%s'",
        GCP_PROJECT_ID,
        GCP_REGION,
    )
    try:
        aiplatform.init(project=GCP_PROJECT_ID, location=GCP_REGION)
        log.info("[VERTEX-AI] ✅ Vertex AI SDK initialised.")
    except Exception as exc:
        log.error("[VERTEX-AI] ❌ FAILED to initialise Vertex AI SDK!")
        log.error("[VERTEX-AI] Error type: %s", type(exc).__name__)
        log.error("[VERTEX-AI] Error details: %s", exc)
        log.error("[VERTEX-AI] Traceback:\n%s", traceback.format_exc())
        log.error(
            "[VERTEX-AI] 💡 TIP: Verify GCP_PROJECT_ID and GCP_REGION Airflow "
            "Variables are correct, and that Vertex AI API is enabled."
        )
        raise

    # -- 3. Prepare prompt ------------------------------------------------
    model_name = "gemini-1.5-flash-002"
    log.info("[VERTEX-AI] Loading model: %s", model_name)
    model = GenerativeModel(model_name)

    prompt = (
        "You are a data-processing agent. You will receive a batch of new "
        "database entries in JSON format. For each entry, simply respond "
        'with "Hi".  Here are the entries:\n\n'
        f"{json.dumps(records, indent=2, default=str)}"
    )
    log.info("[VERTEX-AI] Prompt length: %d characters", len(prompt))
    log.info(
        "[VERTEX-AI] Sending %d record(s) to Vertex AI (%s) …",
        len(records),
        model_name,
    )

    # -- 4. Call the model ------------------------------------------------
    try:
        response = model.generate_content(prompt)
        agent_reply = response.text
        log.info("[VERTEX-AI] ✅ Received response from Vertex AI.")
        log.info("[VERTEX-AI] Agent response (first 500 chars): %s", agent_reply[:500])
    except Exception as exc:
        log.error("[VERTEX-AI] ❌ Vertex AI call FAILED!")
        log.error("[VERTEX-AI] Model: %s", model_name)
        log.error("[VERTEX-AI] Error type: %s", type(exc).__name__)
        log.error("[VERTEX-AI] Error details: %s", exc)
        log.error("[VERTEX-AI] Traceback:\n%s", traceback.format_exc())
        log.error(
            "[VERTEX-AI] 💡 TIP: Common causes:\n"
            "  - Vertex AI API not enabled (run setup_vertex_ai.sh)\n"
            "  - Service account missing 'aiplatform.user' IAM role\n"
            "  - Invalid GCP_PROJECT_ID or GCP_REGION\n"
            "  - Quota exceeded"
        )
        agent_reply = "Hi (fallback — agent unreachable)"
        log.warning("[VERTEX-AI] Using fallback response: '%s'", agent_reply)

    ti.xcom_push(key="agent_response", value=agent_reply)
    log.info("[VERTEX-AI] ✅ call_vertex_ai_agent task completed.")


def update_watermark(**context) -> None:
    """
    Task 3 – Persist the new high-water-mark so subsequent runs
    only pick up entries newer than the ones just processed.
    """
    log.info("=" * 70)
    log.info("[WATERMARK] ▶ Starting update_watermark task")
    log.info("=" * 70)

    ti = context["ti"]

    # Pull values from upstream tasks
    new_hwm = ti.xcom_pull(task_ids="extract_new_entries", key="new_hwm")
    record_count = ti.xcom_pull(task_ids="extract_new_entries", key="record_count")
    agent_response = ti.xcom_pull(task_ids="call_vertex_ai_agent", key="agent_response")

    log.info("[WATERMARK] Data from upstream tasks:")
    log.info("[WATERMARK]   new_hwm        = %s", new_hwm)
    log.info("[WATERMARK]   record_count   = %s", record_count)
    log.info("[WATERMARK]   agent_response = %s", agent_response)

    if new_hwm is None:
        log.error(
            "[WATERMARK] ❌ new_hwm is None — cannot update the watermark! "
            "The extract task may not have completed successfully."
        )
        raise ValueError("new_hwm is None — cannot update watermark.")

    old_hwm = _get_last_hwm()
    log.info("[WATERMARK] Updating HWM: '%s' → '%s'", old_hwm, new_hwm)
    Variable.set(HWM_VARIABLE_KEY, new_hwm)
    log.info("[WATERMARK] ✅ HWM Variable '%s' updated successfully.", HWM_VARIABLE_KEY)

    log.info(
        "[WATERMARK] ══ Pipeline Summary ══\n"
        "  Records processed : %s\n"
        "  Old HWM           : %s\n"
        "  New HWM           : %s\n"
        "  Agent response    : %s",
        record_count,
        old_hwm,
        new_hwm,
        agent_response,
    )
    log.info("[WATERMARK] ✅ update_watermark task completed.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="snowflake_vertex_ai_pipeline",
    default_args=default_args,
    description=(
        "Incrementally extract new Snowflake rows and greet them "
        "via a Vertex AI agent."
    ),
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["snowflake", "vertex-ai", "incremental"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_new_entries",
        python_callable=extract_new_entries,
    )

    t_agent = PythonOperator(
        task_id="call_vertex_ai_agent",
        python_callable=call_vertex_ai_agent,
    )

    t_watermark = PythonOperator(
        task_id="update_watermark",
        python_callable=update_watermark,
    )

    t_extract >> t_agent >> t_watermark
