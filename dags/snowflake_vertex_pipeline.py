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
    SNOWFLAKE_TABLE          – table name              (e.g. ERROR_LOGS)
    SNOWFLAKE_TIMESTAMP_COL  – column used for incremental extraction
                               (default: CREATED_AT)
    GCP_PROJECT_ID           – your GCP project ID
    GCP_REGION               – region for Vertex AI (default: us-central1)
"""

from __future__ import annotations

import json
import logging
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
SNOWFLAKE_DATABASE = Variable.get("SNOWFLAKE_DATABASE", default_var="MY_DB")
SNOWFLAKE_SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default_var="MY_SCHEMA")
SNOWFLAKE_TABLE = Variable.get("SNOWFLAKE_TABLE", default_var="ERROR_LOGS")
TIMESTAMP_COL = Variable.get("SNOWFLAKE_TIMESTAMP_COL", default_var="CREATED_AT")
GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var="my-gcp-project")
GCP_REGION = Variable.get("GCP_REGION", default_var="us-central1")

# Fully-qualified table reference: DATABASE.SCHEMA.TABLE
FQ_TABLE = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{SNOWFLAKE_TABLE}"

# Airflow Variable that persists the high-water-mark between runs.
HWM_VARIABLE_KEY = "snowflake_pipeline_hwm"

log = logging.getLogger(__name__)

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
        return Variable.get(HWM_VARIABLE_KEY)
    except KeyError:
        return "1970-01-01T00:00:00Z"


def extract_new_entries(**context) -> None:
    """
    Task 1 – Query Snowflake for rows where TIMESTAMP_COL > last HWM.
    Pushes the result set and the new HWM to XCom.
    """
    hwm = _get_last_hwm()
    log.info("High-water-mark from last run: %s", hwm)

    query = f"""
        SELECT *
        FROM   {FQ_TABLE}
        WHERE  {TIMESTAMP_COL} > %(hwm)s
        ORDER  BY {TIMESTAMP_COL} ASC
    """

    hook = SnowflakeHook(
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
    )
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(query, {"hwm": hwm})
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        cursor.close()
        conn.close()

    records = [dict(zip(columns, row)) for row in rows]
    log.info("Fetched %d new record(s) from Snowflake.", len(records))

    # Determine new HWM (max timestamp in this batch, or keep old one).
    if records:
        new_hwm = max(str(r[TIMESTAMP_COL]) for r in records)
    else:
        new_hwm = hwm

    # Push data downstream via XCom.
    context["ti"].xcom_push(key="new_records", value=json.dumps(records, default=str))
    context["ti"].xcom_push(key="new_hwm", value=new_hwm)
    context["ti"].xcom_push(key="record_count", value=len(records))


def call_vertex_ai_agent(**context) -> None:
    """
    Task 2 – Send extracted entries to a Vertex AI (Gemini) agent
    that replies with "Hi".
    """
    ti = context["ti"]
    records_json = ti.xcom_pull(task_ids="extract_new_entries", key="new_records")
    records = json.loads(records_json) if records_json else []

    if not records:
        log.info("No new entries — skipping Vertex AI call.")
        ti.xcom_push(key="agent_response", value="No new entries to process.")
        return

    # Initialise Vertex AI SDK
    aiplatform.init(project=GCP_PROJECT_ID, location=GCP_REGION)

    model = GenerativeModel("gemini-1.5-flash-002")

    prompt = (
        "You are a data-processing agent. You will receive a batch of new "
        "database entries in JSON format. For each entry, simply respond "
        'with "Hi".  Here are the entries:\n\n'
        f"{json.dumps(records, indent=2, default=str)}"
    )

    log.info("Sending %d record(s) to Vertex AI agent …", len(records))

    try:
        response = model.generate_content(prompt)
        agent_reply = response.text
    except Exception:
        log.exception("Vertex AI call failed — using fallback response.")
        agent_reply = "Hi (fallback — agent unreachable)"

    log.info("Agent response: %s", agent_reply)
    ti.xcom_push(key="agent_response", value=agent_reply)


def update_watermark(**context) -> None:
    """
    Task 3 – Persist the new high-water-mark so subsequent runs
    only pick up entries newer than the ones just processed.
    """
    ti = context["ti"]
    new_hwm = ti.xcom_pull(task_ids="extract_new_entries", key="new_hwm")
    record_count = ti.xcom_pull(task_ids="extract_new_entries", key="record_count")
    agent_response = ti.xcom_pull(task_ids="call_vertex_ai_agent", key="agent_response")

    Variable.set(HWM_VARIABLE_KEY, new_hwm)
    log.info(
        "Pipeline complete — processed %s record(s). "
        "New HWM set to %s. Agent said: %s",
        record_count,
        new_hwm,
        agent_response,
    )


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
