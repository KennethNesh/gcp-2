#!/usr/bin/env bash
# ============================================================================
# setup_vertex_ai.sh — Enable Vertex AI APIs and grant the Composer
# service account the permissions it needs to call Gemini.
# ============================================================================
set -euo pipefail

: "${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
: "${GCP_REGION:=us-central1}"
: "${COMPOSER_ENV_NAME:=snowflake-vertex-pipeline}"

echo "━━━ Enabling Vertex AI API ━━━"
gcloud services enable aiplatform.googleapis.com \
    --project="${GCP_PROJECT_ID}"

echo ""
echo "━━━ Fetching Composer service account ━━━"
COMPOSER_SA=$(gcloud composer environments describe "${COMPOSER_ENV_NAME}" \
    --project="${GCP_PROJECT_ID}" \
    --location="${GCP_REGION}" \
    --format="value(config.nodeConfig.serviceAccount)")

echo "Composer SA: ${COMPOSER_SA}"

echo ""
echo "━━━ Granting Vertex AI User role ━━━"
gcloud projects add-iam-policy-binding "${GCP_PROJECT_ID}" \
    --member="serviceAccount:${COMPOSER_SA}" \
    --role="roles/aiplatform.user" \
    --condition=None

echo ""
echo "✅  Vertex AI is enabled and the Composer service account"
echo "    (${COMPOSER_SA}) has the aiplatform.user role."
