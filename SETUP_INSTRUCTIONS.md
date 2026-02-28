# Snowflake → Vertex AI Pipeline — Setup Instructions

> **Last updated:** 28 Feb 2026  
> Follow these steps on a fresh system to get the pipeline running end-to-end.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Clone / Copy the Project](#2-clone--copy-the-project)
3. [Project Structure](#3-project-structure)
4. [GCP Setup](#4-gcp-setup)
5. [Deploy Infrastructure](#5-deploy-infrastructure)
6. [Configure Snowflake Connection in Airflow](#6-configure-snowflake-connection-in-airflow)
7. [Set Airflow Variables](#7-set-airflow-variables)
8. [Upload the DAG](#8-upload-the-dag)
9. [Verify & Unpause](#9-verify--unpause)
10. [How the Pipeline Works](#10-how-the-pipeline-works)
11. [Troubleshooting](#11-troubleshooting)

---

## 1. Prerequisites

Make sure the following are installed / available on the new system:

| Tool | Minimum Version | Purpose |
|------|-----------------|---------|
| **Google Cloud SDK (`gcloud`)** | Latest | CLI access to GCP |
| **Terraform** *(optional)* | ≥ 1.5 | Infrastructure-as-Code alternative |
| **Python** | ≥ 3.9 | Local testing only (not required for deployment) |
| **A GCP Project** | — | With billing enabled |
| **Snowflake Account** | — | With a warehouse, database, schema, and a user/role that can `SELECT` from the target table |

### GCP APIs that will be enabled

The setup scripts / Terraform will enable these automatically:

- `composer.googleapis.com` (Cloud Composer)
- `aiplatform.googleapis.com` (Vertex AI)

---

## 2. Clone / Copy the Project

Copy the entire `gcp-2` folder to the new system. The folder should look like this:

```
gcp-2/
├── dags/
│   └── snowflake_vertex_pipeline.py    ← Airflow DAG
├── scripts/
│   ├── setup_composer.sh               ← Composer provisioning
│   └── setup_vertex_ai.sh              ← Vertex AI + IAM setup
├── terraform/
│   └── main.tf                         ← Terraform IaC (alternative)
├── requirements.txt                    ← PyPI deps for Composer
└── SETUP_INSTRUCTIONS.md               ← This file
```

---

## 3. Project Structure

| File | What it does |
|------|--------------|
| `dags/snowflake_vertex_pipeline.py` | The Airflow DAG — 3 tasks: extract from Snowflake, call Vertex AI, update watermark |
| `requirements.txt` | Python packages installed into the Composer environment |
| `scripts/setup_composer.sh` | Creates Composer 2 env, installs packages, sets Airflow variables, uploads DAG |
| `scripts/setup_vertex_ai.sh` | Enables Vertex AI API and grants IAM role to Composer's service account |
| `terraform/main.tf` | Full Terraform config (alternative to the shell scripts) |

---

## 4. GCP Setup

### 4.1 — Authenticate

```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

### 4.2 — Set Environment Variables

```bash
export GCP_PROJECT_ID="your-gcp-project-id"
export GCP_REGION="us-central1"              # change if needed
```

---

## 5. Deploy Infrastructure

Pick **one** of the two options below:

### Option A — Shell Scripts (quicker)

```bash
cd gcp-2

# Step 1: Create Composer env + install deps + set variables + upload DAG
bash scripts/setup_composer.sh

# Step 2: Enable Vertex AI API + grant IAM
bash scripts/setup_vertex_ai.sh
```

> **Note:** Composer environment creation takes ~20–25 minutes.

### Option B — Terraform (production-grade)

```bash
cd gcp-2/terraform

terraform init

terraform plan \
  -var="project_id=${GCP_PROJECT_ID}" \
  -var="region=${GCP_REGION}" \
  -var="snowflake_database=MY_DB" \
  -var="snowflake_schema=MY_SCHEMA" \
  -var="snowflake_table=test"

terraform apply \
  -var="project_id=${GCP_PROJECT_ID}" \
  -var="region=${GCP_REGION}" \
  -var="snowflake_database=MY_DB" \
  -var="snowflake_schema=MY_SCHEMA" \
  -var="snowflake_table=test"
```

After Terraform finishes, upload the DAG:

```bash
gsutil cp ../dags/snowflake_vertex_pipeline.py \
    $(terraform output -raw composer_dag_gcs_prefix)/
```

---

## 6. Configure Snowflake Connection in Airflow

This is a **manual step** done in the Airflow Web UI.

### 6.1 — Open the Airflow UI

```bash
# Get the Airflow UI URL
gcloud composer environments describe snowflake-vertex-pipeline \
    --location="${GCP_REGION}" \
    --format="value(config.airflowUri)"
```

### 6.2 — Create the Connection

1. Go to **Admin → Connections** in the Airflow UI.
2. Click **+ Add a new record**.
3. Fill in:

| Field | Value |
|-------|-------|
| **Connection Id** | `snowflake_default` |
| **Connection Type** | `Snowflake` |
| **Host** | `<your-account>.snowflakecomputing.com` |
| **Login** | Your Snowflake username |
| **Password** | Your Snowflake password |
| **Schema** | Your schema name (e.g. `MY_SCHEMA`) |
| **Extra** (JSON) | See below |

**Extra field (JSON):**

```json
{
  "account": "<your-snowflake-account>",
  "warehouse": "COMPUTE_WH",
  "database": "MY_DB",
  "role": "PIPELINE_ROLE"
}
```

> Replace `<your-snowflake-account>` with your Snowflake account identifier  
> (e.g. `xy12345` or `xy12345.us-east-1`).

4. Click **Save**.

> ⚠️ **Production tip:** Instead of storing the password here, use  
> [Google Secret Manager as an Airflow Secret Backend](https://cloud.google.com/composer/docs/secret-manager).

---

## 7. Set Airflow Variables

If you used the shell scripts (`setup_composer.sh`), the variables are **already set** with defaults. Otherwise, set them manually:

### Via the Airflow UI

Go to **Admin → Variables** and create:

| Variable | Value | Description |
|----------|-------|-------------|
| `SNOWFLAKE_DATABASE` | `MY_DB` | Your Snowflake database name |
| `SNOWFLAKE_SCHEMA` | `MY_SCHEMA` | Your Snowflake schema name |
| `SNOWFLAKE_TABLE` | `test` | The table to extract from |
| `SNOWFLAKE_TIMESTAMP_COL` | `CREATED_AT` | Column used for incremental extraction |
| `GCP_PROJECT_ID` | `your-project-id` | Your GCP project |
| `GCP_REGION` | `us-central1` | Region for Vertex AI |

### Via CLI (alternative)

```bash
COMPOSER_ENV="snowflake-vertex-pipeline"

gcloud composer environments run ${COMPOSER_ENV} \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_DATABASE  "MY_DB"

gcloud composer environments run ${COMPOSER_ENV} \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_SCHEMA   "MY_SCHEMA"

gcloud composer environments run ${COMPOSER_ENV} \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_TABLE    "test"

gcloud composer environments run ${COMPOSER_ENV} \
    --location="${GCP_REGION}" \
    variables set -- SNOWFLAKE_TIMESTAMP_COL "CREATED_AT"

gcloud composer environments run ${COMPOSER_ENV} \
    --location="${GCP_REGION}" \
    variables set -- GCP_PROJECT_ID     "${GCP_PROJECT_ID}"

gcloud composer environments run ${COMPOSER_ENV} \
    --location="${GCP_REGION}" \
    variables set -- GCP_REGION         "${GCP_REGION}"
```

---

## 8. Upload the DAG

If you **didn't** use `setup_composer.sh` (which uploads it for you), upload manually:

```bash
# Get the DAGs GCS bucket
DAGS_FOLDER=$(gcloud composer environments describe snowflake-vertex-pipeline \
    --location="${GCP_REGION}" \
    --format="value(config.dagGcsPrefix)")

# Upload
gsutil cp dags/snowflake_vertex_pipeline.py "${DAGS_FOLDER}/"
```

---

## 9. Verify & Unpause

1. Open the **Airflow Web UI** (see Step 6.1).
2. Look for the DAG named **`snowflake_vertex_ai_pipeline`**.
3. It will be **paused** by default. Toggle it **on** to activate.
4. Trigger a manual run to test:
   - Click the DAG name → click **Trigger DAG** (▶ button).
5. Check the **task logs** for each task:
   - `extract_new_entries` — should show the number of rows fetched.
   - `call_vertex_ai_agent` — should show the "Hi" response from Vertex AI.
   - `update_watermark` — should confirm the new high-water-mark was saved.

---

## 10. How the Pipeline Works

```
┌───────────────────────────────────────────────────────────────┐
│                    DAG: Every 10 minutes                      │
│                                                               │
│  ┌──────────────────┐   ┌──────────────────┐   ┌───────────┐ │
│  │  extract_new_    │──▶│ call_vertex_ai_  │──▶│  update_  │ │
│  │  entries         │   │ agent            │   │  watermark│ │
│  └────────┬─────────┘   └────────┬─────────┘   └───────────┘ │
│           │                      │                             │
│   Query Snowflake         Send entries to              Save   │
│   WHERE ts > HWM          Gemini 1.5 Flash             new    │
│                           → returns "Hi"               HWM    │
└───────────────────────────────────────────────────────────────┘
```

- **High-Water-Mark (HWM):** Stored as Airflow Variable `snowflake_pipeline_hwm`. On the first run it defaults to `1970-01-01`, fetching all rows. Each subsequent run only fetches rows newer than the previous max timestamp.
- **Vertex AI Agent:** Uses Gemini 1.5 Flash. The prompt sends the new entries as JSON, and the model is instructed to respond with "Hi".
- **Fault tolerance:** If Vertex AI is unreachable, the task logs a warning and returns a fallback "Hi" response so the pipeline doesn't block.
- **`max_active_runs = 1`:** Prevents overlapping runs from duplicating extractions.

---

## 11. Troubleshooting

### DAG not appearing in Airflow UI

- Wait 2–3 minutes after uploading — Composer syncs DAGs from GCS periodically.
- Check for Python syntax errors:
  ```bash
  gcloud composer environments run snowflake-vertex-pipeline \
      --location="${GCP_REGION}" \
      dags list
  ```

### Snowflake connection errors

- Verify the `snowflake_default` connection in **Admin → Connections**.
- Ensure the **account identifier** in the `Extra` JSON matches your Snowflake account.
- Confirm the user/role has `SELECT` privileges on the target table.
- Test connectivity from the Composer environment:
  ```bash
  gcloud composer environments run snowflake-vertex-pipeline \
      --location="${GCP_REGION}" \
      test -- snowflake_vertex_ai_pipeline extract_new_entries 2025-01-01
  ```

### Vertex AI "Permission Denied"

- Run `setup_vertex_ai.sh` again to ensure the `aiplatform.user` role is granted.
- Verify the service account:
  ```bash
  gcloud composer environments describe snowflake-vertex-pipeline \
      --location="${GCP_REGION}" \
      --format="value(config.nodeConfig.serviceAccount)"
  ```

### No new entries being extracted

- Check the current HWM in **Admin → Variables → `snowflake_pipeline_hwm`**.
- If needed, reset it to `1970-01-01T00:00:00Z` to re-fetch everything.
- Verify data exists in the Snowflake table with timestamps after the HWM.

---

> **Questions?** Refer to the source files for inline documentation, or check  
> the [Cloud Composer docs](https://cloud.google.com/composer/docs) and  
> [Snowflake Airflow provider docs](https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html).
