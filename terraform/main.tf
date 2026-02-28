# ============================================================================
# Terraform — Cloud Composer 2 + Vertex AI for Snowflake pipeline
# ============================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0"
    }
  }
}

# ── Variables ───────────────────────────────────────────────────────────────

variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "composer_env_name" {
  description = "Cloud Composer environment name"
  type        = string
  default     = "snowflake-vertex-pipeline"
}

variable "composer_image_version" {
  description = "Composer image version"
  type        = string
  default     = "composer-2.9.7-airflow-2.9.3"
}

variable "snowflake_database" {
  description = "Snowflake database name"
  type        = string
  default     = "MY_DB"
}

variable "snowflake_schema" {
  description = "Snowflake schema name"
  type        = string
  default     = "MY_SCHEMA"
}

variable "snowflake_table" {
  description = "Snowflake table name"
  type        = string
  default     = "ERROR_LOGS"
}

variable "snowflake_timestamp_col" {
  description = "Column used for incremental extraction"
  type        = string
  default     = "CREATED_AT"
}

# ── Provider ────────────────────────────────────────────────────────────────

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── Enable APIs ─────────────────────────────────────────────────────────────

resource "google_project_service" "composer" {
  service            = "composer.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "aiplatform" {
  service            = "aiplatform.googleapis.com"
  disable_on_destroy = false
}

# ── Service Account ─────────────────────────────────────────────────────────

resource "google_service_account" "composer" {
  account_id   = "composer-snowflake-vtx"
  display_name = "Composer – Snowflake + Vertex AI pipeline"
}

resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_project_iam_member" "vertex_user" {
  project = var.project_id
  role    = "roles/aiplatform.user"
  member  = "serviceAccount:${google_service_account.composer.email}"
}

# ── Cloud Composer 2 ────────────────────────────────────────────────────────

resource "google_composer_environment" "pipeline" {
  name     = var.composer_env_name
  region   = var.region
  provider = google

  depends_on = [
    google_project_service.composer,
    google_project_service.aiplatform,
  ]

  config {
    software_config {
      image_version = var.composer_image_version

      pypi_packages = {
        "apache-airflow-providers-snowflake" = ">=5.3.0"
        "snowflake-connector-python"         = ">=3.6.0"
        "google-cloud-aiplatform"            = ">=1.60.0"
      }

      env_variables = {
        GCP_PROJECT_ID = var.project_id
        GCP_REGION     = var.region
      }

      airflow_config_overrides = {
        "core-dags_are_paused_at_creation" = "True"
      }
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    node_config {
      service_account = google_service_account.composer.email
    }
  }
}

# ── Outputs ─────────────────────────────────────────────────────────────────

output "composer_dag_gcs_prefix" {
  description = "GCS path to upload DAGs"
  value       = google_composer_environment.pipeline.config[0].dag_gcs_prefix
}

output "composer_airflow_uri" {
  description = "Airflow web UI URL"
  value       = google_composer_environment.pipeline.config[0].airflow_uri
}

output "composer_service_account" {
  description = "Composer service account"
  value       = google_service_account.composer.email
}
