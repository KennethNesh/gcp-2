#!/usr/bin/env bash
# ============================================================================
# setup_composer.sh — Provision a Cloud Composer 2 environment and configure
# it with the Snowflake + Vertex AI dependencies needed by the pipeline.
# ============================================================================
set -euo pipefail

# ── Required env vars ─────────────────────────────────────────────────────
: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:=us-central1}"
: "${COMPOSER_ENV_NAME:=snowflake-vertex-pipeline}"
: "${COMPOSER_IMAGE_VERSION:=composer-2.9.7-airflow-2.9.3}"

echo "━━━ Creating Cloud Composer 2 environment ━━━"
gcloud composer environments create "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --image-version="${COMPOSER_IMAGE_VERSION}" \
    --environment-size=small

echo ""
echo "━━━ Installing PyPI packages ━━━"
gcloud composer environments update "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --update-pypi-packages-from-file=requirements.txt

echo ""
echo "━━━ Setting Airflow Variables ━━━"
gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_DATABASE "MY_DB"

gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_SCHEMA "MY_SCHEMA"

gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_TABLE "ERROR_LOGS"

gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_TIMESTAMP_COL "CREATED_AT"

gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    variables set -- GCP_PROJECT_ID "${GCP_PROJECT_ID}"

gcloud composer environments run "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    variables set -- GCP_REGION "${GCP_REGION}"

echo ""
echo "━━━ Uploading DAG ━━━"
DAGS_FOLDER=$(gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --format="value(config.dagGcsPrefix)")

gsutil cp dags/snowflake_vertex_pipeline.py "${DAGS_FOLDER}/"

echo ""
echo "✅  Composer environment '${COMPOSER_ENV_NAME}' is ready."
echo "    DAG uploaded to ${DAGS_FOLDER}/"
echo ""
echo "Next steps:"
echo "  1. Configure the 'snowflake_default' Airflow connection in the Composer UI."
echo "  2. Verify the DAG appears in the Airflow web UI."
