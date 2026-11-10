-- =============================================================================
-- Claims Processing ETL - PRODUCTION environment setup (Unity Catalog)
-- =============================================================================
-- !! PRODUCTION DATA IS PERSISTENT AND PROTECTED !!
-- * Every statement uses IF NOT EXISTS: re-running never recreates, over-
--   writes, drops or deletes an existing object.
-- * The `landing` volume and ALL FILES ALREADY IN IT are preserved exactly
--   as they are. This script only creates objects that do not yet exist.
-- * This script contains no DROP, TRUNCATE, DELETE or UPDATE statements.
-- =============================================================================
-- HOW TO RUN (no Databricks CLI needed): same as setup_dev.sql - paste into
-- a SQL query in the PRODUCTION workspace, select a warehouse, and Run.
-- Run this ONCE before the first production deployment.
-- =============================================================================

-- ---------------------------------------------------------------- Catalog --
CREATE CATALOG IF NOT EXISTS claims_prod
  COMMENT 'PROD catalog for the Claims Processing ETL pipeline';

-- ----------------------------------------------------------------- Schema --
CREATE SCHEMA IF NOT EXISTS claims_prod.bronze
  COMMENT 'Bronze layer: raw ingested claims data (production)';

-- ----------------------------------------------------------------- Volume --
-- If this volume already exists (with files in it), this statement is a
-- NO-OP. Existing volumes, their contents and their permissions are kept.
CREATE VOLUME IF NOT EXISTS claims_prod.bronze.landing
  COMMENT 'Landing area for raw claims CSV files (production)';

-- ------------------------------------------------------- Target Delta table --
CREATE TABLE IF NOT EXISTS claims_prod.bronze.claims_raw (
  claim_id        STRING         COMMENT 'Business key of the claim',
  policy_number   STRING         COMMENT 'Policy the claim was filed against',
  claim_date      DATE           COMMENT 'Date the claim was filed',
  claim_amount    DECIMAL(12,2)  COMMENT 'Approved claim amount',
  status          STRING         COMMENT 'Normalized: SUBMITTED/PENDING/APPROVED/DENIED/UNKNOWN',
  claimant_name   STRING,
  region          STRING         COMMENT 'Normalized to uppercase',
  claim_year      INT            COMMENT 'Derived: year of claim_date',
  _ingested_at    TIMESTAMP      COMMENT 'Pipeline processing time',
  _source_file    STRING         COMMENT 'Landing file the record came from'
)
USING DELTA
COMMENT 'Validated claims ingested from the landing volume (idempotent MERGE target)';

-- --------------------------------------------------------- Quarantine table --
CREATE TABLE IF NOT EXISTS claims_prod.bronze.claims_quarantine (
  quarantine_key   STRING    COMMENT 'Stable key (claim_id, or row hash when claim_id is unusable)',
  claim_id         STRING,
  policy_number    STRING,
  claim_date       STRING    COMMENT 'Raw (unparsed) date value as it arrived',
  claim_amount     STRING    COMMENT 'Raw (unparsed) amount value as it arrived',
  status           STRING,
  claimant_name    STRING,
  region           STRING,
  invalid_reason   STRING    COMMENT 'Semicolon-joined list of validation failures',
  _quarantined_at  TIMESTAMP
)
USING DELTA
COMMENT 'Records rejected by claims ingestion validation (idempotent MERGE target)';

-- ----------------------------------------------------------------- Grants --
-- PRODUCTION PERMISSIONS - deliberately locked down.
--
-- Principle of least privilege:
--   * The deployment SERVICE PRINCIPAL owns (or co-owns) the catalog.
--   * The job's run-as service principal can read the volume + write tables.
--   * Humans get read-only; nobody but the pipeline writes to bronze.
--
-- Replace the placeholders below with your real principals, then uncomment.
-- A service principal is referenced by its APPLICATION ID (a UUID), e.g.
--   <deploy_sp_application_id>  ->  9f1c2b34-5678-4abc-9def-0123456789ab

-- 1) Deployment service principal (runs `bundle deploy -t prod` from CI):
-- GRANT USE CATALOG ON CATALOG claims_prod TO `<deploy_sp_application_id>`;
-- GRANT USE SCHEMA ON SCHEMA claims_prod.bronze TO `<deploy_sp_application_id>`;
-- GRANT CREATE TABLE ON SCHEMA claims_prod.bronze TO `<deploy_sp_application_id>`;

-- 2) Run-as service principal (the job's identity, i.e. job_owner in the bundle):
-- GRANT READ VOLUME ON VOLUME claims_prod.bronze.landing TO `<run_as_sp_application_id>`;
-- GRANT USE SCHEMA ON SCHEMA claims_prod.bronze TO `<run_as_sp_application_id>`;
-- GRANT MODIFY, SELECT ON TABLE claims_prod.bronze.claims_raw TO `<run_as_sp_application_id>`;
-- GRANT MODIFY, SELECT ON TABLE claims_prod.bronze.claims_quarantine TO `<run_as_sp_application_id>`;

-- 3) Humans: read-only
-- GRANT USE CATALOG ON CATALOG claims_prod TO `data_platform_team`;
-- GRANT USE SCHEMA ON SCHEMA claims_prod.bronze TO `data_platform_team`;
-- GRANT SELECT ON TABLE claims_prod.bronze.claims_raw TO `data_platform_team`;
-- GRANT SELECT ON TABLE claims_prod.bronze.claims_quarantine TO `data_platform_team`;

-- Ownership: to hand the catalog to the deployment service principal, run
-- (requires you to be the current owner or a metastore admin):
-- ALTER CATALOG claims_prod OWNER TO `<deploy_sp_application_id>`;
