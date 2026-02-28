-- ============================================================================
-- Snowflake Setup for the Vertex AI Pipeline
-- ============================================================================
-- Run these queries in your Snowflake worksheet (using ACCOUNTADMIN or a role
-- with sufficient privileges).
--
-- This script creates:
--   1. A warehouse (COMPUTE_WH)
--   2. A database (MY_DB) and schema (MY_SCHEMA)
--   3. The target table (ERROR_LOGS) with sample data
--   4. A dedicated role (PIPELINE_ROLE) and user (PIPELINE_USER)
--   5. Grants so the pipeline can SELECT from the table
-- ============================================================================

-- ── Step 1: Use ACCOUNTADMIN to create resources ────────────────────────────
USE ROLE ACCOUNTADMIN;

-- ── Step 2: Create Warehouse ────────────────────────────────────────────────
-- (Skip if you already have a warehouse you want to use)
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60          -- seconds of idle before auto-suspend
    AUTO_RESUME    = TRUE
    INITIALLY_SUSPENDED = TRUE;  -- don't start billing immediately

-- ── Step 3: Create Database and Schema ──────────────────────────────────────
CREATE DATABASE IF NOT EXISTS MY_DB;
CREATE SCHEMA IF NOT EXISTS MY_DB.MY_SCHEMA;

-- ── Step 4: Create the ERROR_LOGS table ─────────────────────────────────────
USE DATABASE MY_DB;
USE SCHEMA MY_SCHEMA;

CREATE TABLE IF NOT EXISTS ERROR_LOGS (
    ID              INT AUTOINCREMENT PRIMARY KEY,
    ERROR_MESSAGE   VARCHAR(1000)      NOT NULL,
    ERROR_SEVERITY  VARCHAR(20)        NOT NULL DEFAULT 'INFO',
    SOURCE_SYSTEM   VARCHAR(100),
    CREATED_AT      TIMESTAMP_NTZ      NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- Add a comment for documentation
COMMENT ON TABLE ERROR_LOGS IS 
    'Error log entries consumed by the Vertex AI pipeline via Cloud Composer.';

-- ── Step 5: Insert sample data ──────────────────────────────────────────────
-- (So the pipeline has something to process on its first run)
INSERT INTO ERROR_LOGS (ERROR_MESSAGE, ERROR_SEVERITY, SOURCE_SYSTEM, CREATED_AT)
VALUES
    ('Connection timeout to payment gateway',      'ERROR',   'payments',     DATEADD(MINUTE, -30, CURRENT_TIMESTAMP())),
    ('Disk usage exceeded 90% on node-3',          'WARNING', 'infra',        DATEADD(MINUTE, -25, CURRENT_TIMESTAMP())),
    ('User login failed: invalid credentials',     'INFO',    'auth',         DATEADD(MINUTE, -20, CURRENT_TIMESTAMP())),
    ('API rate limit exceeded for /v2/search',     'ERROR',   'api-gateway',  DATEADD(MINUTE, -15, CURRENT_TIMESTAMP())),
    ('Memory allocation failed in worker-7',       'CRITICAL','compute',      DATEADD(MINUTE, -10, CURRENT_TIMESTAMP())),
    ('SSL certificate expires in 7 days',          'WARNING', 'security',     DATEADD(MINUTE, -5,  CURRENT_TIMESTAMP())),
    ('Database replication lag > 5 seconds',       'ERROR',   'database',     CURRENT_TIMESTAMP());

-- Verify the data
SELECT * FROM ERROR_LOGS ORDER BY CREATED_AT DESC;

-- ── Step 6: Create a dedicated Role for the pipeline ────────────────────────
CREATE ROLE IF NOT EXISTS PIPELINE_ROLE;

-- Grant usage on warehouse, database, schema
GRANT USAGE ON WAREHOUSE COMPUTE_WH       TO ROLE PIPELINE_ROLE;
GRANT USAGE ON DATABASE MY_DB             TO ROLE PIPELINE_ROLE;
GRANT USAGE ON SCHEMA MY_DB.MY_SCHEMA     TO ROLE PIPELINE_ROLE;

-- Grant SELECT on the target table (and future tables in this schema)
GRANT SELECT ON TABLE MY_DB.MY_SCHEMA.ERROR_LOGS TO ROLE PIPELINE_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA MY_DB.MY_SCHEMA TO ROLE PIPELINE_ROLE;

-- ── Step 7: Create a dedicated User for the pipeline ────────────────────────
-- ⚠️  CHANGE THE PASSWORD BELOW before running!
CREATE USER IF NOT EXISTS PIPELINE_USER
    PASSWORD           = 'ChangeMe_Str0ng!Pass'   -- ← CHANGE THIS!
    DEFAULT_ROLE       = PIPELINE_ROLE
    DEFAULT_WAREHOUSE  = COMPUTE_WH
    DEFAULT_NAMESPACE  = MY_DB.MY_SCHEMA
    MUST_CHANGE_PASSWORD = FALSE;

-- Assign the role to the user
GRANT ROLE PIPELINE_ROLE TO USER PIPELINE_USER;

-- ── Step 8: Verify everything works ─────────────────────────────────────────
-- Switch to the pipeline user's role and test
USE ROLE PIPELINE_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MY_DB;
USE SCHEMA MY_SCHEMA;

-- This is the same query the DAG will run (with a very old HWM to get all rows)
SELECT *
FROM   MY_DB.MY_SCHEMA.ERROR_LOGS
WHERE  CREATED_AT > '1970-01-01T00:00:00Z'
ORDER  BY CREATED_AT ASC;

-- ============================================================================
-- 🔍  VERIFICATION QUERIES — SELECT / SHOW for everything
-- ============================================================================

-- ── Your Account Identifier (needed for Airflow "Account" field) ────────────
SELECT CURRENT_ACCOUNT()            AS account_locator;
SELECT CURRENT_ORGANIZATION_NAME()  AS org_name;
SELECT CURRENT_ACCOUNT_NAME()       AS account_name;
-- Use this full identifier for the Airflow Account field:
SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME() AS full_account_id;

-- ── Warehouse ───────────────────────────────────────────────────────────────
SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SELECT "name", "size", "state", "auto_suspend", "auto_resume"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ── Database ────────────────────────────────────────────────────────────────
SHOW DATABASES LIKE 'MY_DB';
SELECT "name", "owner", "created_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ── Schema ──────────────────────────────────────────────────────────────────
SHOW SCHEMAS IN DATABASE MY_DB;
SELECT "name", "database_name", "owner"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = 'MY_SCHEMA';

-- ── Table ───────────────────────────────────────────────────────────────────
SHOW TABLES IN SCHEMA MY_DB.MY_SCHEMA;
SELECT "name", "database_name", "schema_name", "rows", "created_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
WHERE "name" = 'ERROR_LOGS';

-- ── Columns in ERROR_LOGS ───────────────────────────────────────────────────
DESCRIBE TABLE MY_DB.MY_SCHEMA.ERROR_LOGS;

-- ── All data in the table ───────────────────────────────────────────────────
SELECT * FROM MY_DB.MY_SCHEMA.ERROR_LOGS ORDER BY CREATED_AT DESC;

-- ── Row count ───────────────────────────────────────────────────────────────
SELECT COUNT(*) AS total_rows FROM MY_DB.MY_SCHEMA.ERROR_LOGS;

-- ── Role ────────────────────────────────────────────────────────────────────
SHOW ROLES LIKE 'PIPELINE_ROLE';
SELECT "name", "owner", "created_on"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ── User ────────────────────────────────────────────────────────────────────
SHOW USERS LIKE 'PIPELINE_USER';
SELECT "name", "login_name", "default_role", "default_warehouse", "default_namespace"
FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

-- ── Grants on the role (what can PIPELINE_ROLE access?) ─────────────────────
SHOW GRANTS TO ROLE PIPELINE_ROLE;

-- ── Grants on the user (what roles does PIPELINE_USER have?) ────────────────
SHOW GRANTS TO USER PIPELINE_USER;

-- ── Grants on the table (who can access ERROR_LOGS?) ────────────────────────
SHOW GRANTS ON TABLE MY_DB.MY_SCHEMA.ERROR_LOGS;

-- ============================================================================
-- ✅  Done! Now use these values in the Airflow connection:
--
--   Account:    (use the full_account_id from the first query above)
--   Warehouse:  COMPUTE_WH
--   Database:   MY_DB
--   Schema:     MY_SCHEMA
--   Role:       PIPELINE_ROLE
--   Login:      PIPELINE_USER
--   Password:   (whatever you set above)
-- ============================================================================
