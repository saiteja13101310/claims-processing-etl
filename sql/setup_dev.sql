-- =============================================================================
-- Claims Processing ETL - DEV environment setup (Unity Catalog)
-- =============================================================================
-- Safe to re-run any number of times: every statement uses IF NOT EXISTS.
-- Nothing here ever drops, overwrites or deletes data.
--
-- HOW TO RUN (no Databricks CLI needed):
--   1. In the Databricks workspace sidebar:  Edit > Queries
--   2. Create a new query, paste one statement block at a time (or the whole
--      file - the SQL editor runs statements sequentially), select the
--      "Serverless Starter Warehouse", and click Run.
-- =============================================================================

-- ---------------------------------------------------------------- Catalog --
CREATE CATALOG IF NOT EXISTS claims_dev
  COMMENT 'DEV catalog for the Claims Processing ETL pipeline';

-- ----------------------------------------------------------------- Schema --
CREATE SCHEMA IF NOT EXISTS claims_dev.bronze
  COMMENT 'Bronze layer: raw ingested claims data';

-- ----------------------------------------------------------------- Volume --
-- Managed volume for raw landing files. IF NOT EXISTS guarantees an EXISTING
-- volume and all files already in it are left completely untouched.
-- Volumes are intentionally managed HERE, outside the Asset Bundle, so a
-- bundle deploy/destroy can never affect landed production data.
CREATE VOLUME IF NOT EXISTS claims_dev.bronze.landing
  COMMENT 'Landing area for raw claims CSV files';

-- ------------------------------------------------------- Target Delta table --
CREATE TABLE IF NOT EXISTS claims_dev.bronze.claims_raw (
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
CREATE TABLE IF NOT EXISTS claims_dev.bronze.claims_quarantine (
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

-- ------------------------------------------------------------- Run audit --
-- One row per pipeline run: what was read, where it landed, why it failed.
-- The ingestion job appends here at the end of every run (SUCCESS or
-- FAILED), giving a queryable run history without digging through job logs.
CREATE TABLE IF NOT EXISTS claims_dev.bronze.claims_run_audit (
  run_id                    STRING     COMMENT 'Unique id for this pipeline execution',
  started_at                TIMESTAMP  COMMENT 'When the run started',
  finished_at               TIMESTAMP  COMMENT 'When the run finished',
  landing_path              STRING     COMMENT 'Landing glob processed',
  rows_read                 BIGINT     COMMENT 'Rows found in the landing files',
  valid_rows                BIGINT     COMMENT 'Rows passing validation (post-dedup)',
  duplicates_dropped        BIGINT     COMMENT 'Rows dropped as duplicates (latest kept)',
  invalid_rows              BIGINT     COMMENT 'Rows quarantined by validation',
  raw_rows_inserted         BIGINT     COMMENT 'MERGE: new claims added to claims_raw',
  raw_rows_updated          BIGINT     COMMENT 'MERGE: existing claims refreshed',
  quarantine_rows_inserted  BIGINT     COMMENT 'MERGE: new quarantine records',
  status                    STRING     COMMENT 'SUCCESS or FAILED',
  error_message             STRING     COMMENT 'Exception text when status = FAILED'
)
USING DELTA
COMMENT 'Per-run audit trail for the claims ingestion pipeline (append-only)';

-- ----------------------------------------------------------------- Grants --
-- Unity Catalog permissions for development: share read access with a
-- group once your workspace has one. The built-in `users` group does NOT
-- exist on trial / Free Edition workspaces (the GRANTs fail there with
-- PRINCIPAL_DOES_NOT_EXIST) - replace `data_team` with a real group and
-- uncomment. The creator of these objects is their owner and keeps full
-- control either way.

-- GRANT USE CATALOG ON CATALOG claims_dev TO `data_team`;
-- GRANT USE SCHEMA ON SCHEMA claims_dev.bronze TO `data_team`;
-- GRANT SELECT ON TABLE claims_dev.bronze.claims_raw TO `data_team`;
-- GRANT SELECT ON TABLE claims_dev.bronze.claims_quarantine TO `data_team`;
-- GRANT READ VOLUME ON VOLUME claims_dev.bronze.landing TO `data_team`;

-- Write access stays with the pipeline owner (you). To grant a teammate the
-- ability to upload files into the landing volume, run:
-- GRANT WRITE VOLUME ON VOLUME claims_dev.bronze.landing TO `<user-or-group>`;
