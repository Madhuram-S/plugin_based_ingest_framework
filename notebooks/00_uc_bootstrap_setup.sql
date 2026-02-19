-- Databricks notebook source
-- Set environment parameter for use in the script
CREATE WIDGET TEXT ENV DEFAULT 'dev';


-- COMMAND ----------

-- ============================================================================
-- Unity Catalog setup bootstrap script (post-deployment)
-- Target: Databricks SQL (Query Editor) or a SQL cell in a Notebook
--
-- What it does:
--  1) Creates catalogs + schemas (if not exist)
--  2) Applies baseline grants to your groups ("roles")
--  3) Optionally sets catalog/schema owners to an admin group
--
-- What it does NOT do (by design / platform limitation):
--  - Create account-level groups (you create groups in Entra )
--  - Create external locations / storage credentials (separate bootstrap)
--
-- Prereqs:
--  - You are a metastore admin OR have CREATE CATALOG + ownership rights
--  - Groups already exist in your IdP / Databricks account:
--      dbx-uc-metastore-admins, dbx-uc-catalog-owners-nonprod, dbx-uc-catalog-owners-prod
--      dbx-uc-data-engineers_prod, dbx-uc-data-engineers_nonprod, dbx-uc-data-analysts, 

--
-- Usage:
--  - Replace :ENV with dev / test / prod (or your env token)
--  - Replace the catalog list + schema list to match your naming standard
-- ============================================================================

-- ----------------------------
-- 0) Environment token
-- ----------------------------
-- If running in Databricks SQL, you can use a query parameter named ENV.
-- Otherwise, just replace :ENV manually.
-- Example: ENV = dev
-- ----------------------------

-- ----------------------------
-- 1) Define "roles" (groups)
-- ----------------------------
-- Convention: env-prefixed groups to avoid cross-env leakage.
-- Adjust names to match your org.
-- ----------------------------
-- Admins
--   dbx-uc-metastore-admins : metastore admins (account-level grant typically)
--   dbx-uc-catalog-owners   : catalog owners/admins
--
-- Builders
--   dbx-uc-data-engineers-prod or nonprod   : create tables/views, write data

--
-- Consumers
--   dbx-uc-data-analysts    : read, create views in curated
--   dbx-uc-readonly         : read-only across curated

-- ----------------------------
-- 2) Create catalogs (edit list)
-- ----------------------------
-- Example pattern: <env>_<domain>
-- ----------------------------

CREATE CATALOG IF NOT EXISTS :ENV_bronze;
CREATE CATALOG IF NOT EXISTS :ENV_silver;
CREATE CATALOG IF NOT EXISTS :ENV_edw;

-- Optional: set catalog owners (requires privileges; if it fails, run as admin)
-- (Databricks supports ALTER ... OWNER to a principal.)
ALTER CATALOG :ENV_bronze OWNER TO `dbx-uc-catalog-owners-${case when ':ENV' = 'prod' then 'prod' else 'nonprod' end}`;
ALTER CATALOG :ENV_silver OWNER TO `dbx-uc-catalog-owners-${case when ':ENV' = 'prod' then 'prod' else 'nonprod' end}`;
ALTER CATALOG :ENV_edw OWNER TO `dbx-uc-catalog-owners-${case when ':ENV' = 'prod' then 'prod' else 'nonprod' end}`;

-- ----------------------------
-- 3) Create schemas (edit list)
-- ----------------------------
-- Recommend consistent layers per catalog; adjust as needed.
-- ----------------------------

-- Foundation (often contains raw/landing/bronze + technical staging)
CREATE SCHEMA IF NOT EXISTS :ENV_foundation.bronze;
CREATE SCHEMA IF NOT EXISTS :ENV_foundation.stage;

-- Shared (cross-domain curated, reference, conformed)
CREATE SCHEMA IF NOT EXISTS :ENV_shared.reference;
CREATE SCHEMA IF NOT EXISTS :ENV_shared.curated;

-- EDW (gold marts)
CREATE SCHEMA IF NOT EXISTS :ENV_edw.mart_finance;
CREATE SCHEMA IF NOT EXISTS :ENV_edw.mart_sales;

-- Optional: set schema owners
ALTER SCHEMA :ENV_foundation.bronze      OWNER TO `:ENV_uc_catalog_admins`;
ALTER SCHEMA :ENV_foundation.stage       OWNER TO `:ENV_uc_catalog_admins`;
ALTER SCHEMA :ENV_shared.reference       OWNER TO `:ENV_uc_catalog_admins`;
ALTER SCHEMA :ENV_shared.curated         OWNER TO `:ENV_uc_catalog_admins`;
ALTER SCHEMA :ENV_edw.mart_finance       OWNER TO `:ENV_uc_catalog_admins`;
ALTER SCHEMA :ENV_edw.mart_sales         OWNER TO `:ENV_uc_catalog_admins`;

-- ----------------------------
-- 4) Baseline grants (catalog-level)
-- ----------------------------
-- Principles:
--  - Keep CREATE CATALOG limited (usually only metastore admins)
--  - Catalog USE is needed for visibility/access
--  - Prefer schema-level CREATE/WRITE control, not catalog-wide
-- ----------------------------

-- Foundation catalog
GRANT USE CATALOG ON CATALOG :ENV_foundation TO `:ENV_uc_data_engineers`;
GRANT USE CATALOG ON CATALOG :ENV_foundation TO `:ENV_uc_data_scientists`;
GRANT USE CATALOG ON CATALOG :ENV_foundation TO `:ENV_uc_catalog_admins`;

-- Shared catalog
GRANT USE CATALOG ON CATALOG :ENV_shared TO `:ENV_uc_data_engineers`;
GRANT USE CATALOG ON CATALOG :ENV_shared TO `:ENV_uc_data_analysts`;
GRANT USE CATALOG ON CATALOG :ENV_shared TO `:ENV_uc_data_scientists`;
GRANT USE CATALOG ON CATALOG :ENV_shared TO `:ENV_uc_readonly`;
GRANT USE CATALOG ON CATALOG :ENV_shared TO `:ENV_uc_catalog_admins`;

-- EDW catalog
GRANT USE CATALOG ON CATALOG :ENV_edw TO `:ENV_uc_data_engineers`;
GRANT USE CATALOG ON CATALOG :ENV_edw TO `:ENV_uc_data_analysts`;
GRANT USE CATALOG ON CATALOG :ENV_edw TO `:ENV_uc_readonly`;
GRANT USE CATALOG ON CATALOG :ENV_edw TO `:ENV_uc_catalog_admins`;

-- ----------------------------
-- 5) Baseline grants (schema-level)
-- ----------------------------
-- You can tighten/expand per schema.
-- Recommended:
--  - bronze/stage: engineers build/write; others read only if needed
--  - curated/reference/marts: analysts read; engineers write; readonly read
-- ----------------------------

-- Foundation.bronze
GRANT USE SCHEMA ON SCHEMA :ENV_foundation.bronze TO `:ENV_uc_data_engineers`;
GRANT CREATE TABLE, CREATE VIEW, MODIFY ON SCHEMA :ENV_foundation.bronze TO `:ENV_uc_data_engineers`;

-- Optional: allow read for analysts/scientists (often yes for debugging)
GRANT USE SCHEMA ON SCHEMA :ENV_foundation.bronze TO `:ENV_uc_data_scientists`;
GRANT SELECT ON SCHEMA :ENV_foundation.bronze TO `:ENV_uc_data_scientists`;

-- Foundation.stage
GRANT USE SCHEMA ON SCHEMA :ENV_foundation.stage TO `:ENV_uc_data_engineers`;
GRANT CREATE TABLE, CREATE VIEW, MODIFY ON SCHEMA :ENV_foundation.stage TO `:ENV_uc_data_engineers`;

-- Shared.reference
GRANT USE SCHEMA ON SCHEMA :ENV_shared.reference TO `:ENV_uc_data_engineers`;
GRANT CREATE TABLE, CREATE VIEW, MODIFY ON SCHEMA :ENV_shared.reference TO `:ENV_uc_data_engineers`;
GRANT USE SCHEMA ON SCHEMA :ENV_shared.reference TO `:ENV_uc_data_analysts`;
GRANT SELECT ON SCHEMA :ENV_shared.reference TO `:ENV_uc_data_analysts`;
GRANT USE SCHEMA ON SCHEMA :ENV_shared.reference TO `:ENV_uc_readonly`;
GRANT SELECT ON SCHEMA :ENV_shared.reference TO `:ENV_uc_readonly`;

-- Shared.curated
GRANT USE SCHEMA ON SCHEMA :ENV_shared.curated TO `:ENV_uc_data_engineers`;
GRANT CREATE TABLE, CREATE VIEW, MODIFY ON SCHEMA :ENV_shared.curated TO `:ENV_uc_data_engineers`;
GRANT USE SCHEMA ON SCHEMA :ENV_shared.curated TO `:ENV_uc_data_analysts`;
GRANT SELECT, CREATE VIEW ON SCHEMA :ENV_shared.curated TO `:ENV_uc_data_analysts`;
GRANT USE SCHEMA ON SCHEMA :ENV_shared.curated TO `:ENV_uc_readonly`;
GRANT SELECT ON SCHEMA :ENV_shared.curated TO `:ENV_uc_readonly`;

-- EDW marts
GRANT USE SCHEMA ON SCHEMA :ENV_edw.mart_finance TO `:ENV_uc_data_engineers`;
GRANT CREATE TABLE, CREATE VIEW, MODIFY ON SCHEMA :ENV_edw.mart_finance TO `:ENV_uc_data_engineers`;
GRANT USE SCHEMA ON SCHEMA :ENV_edw.mart_finance TO `:ENV_uc_data_analysts`;
GRANT SELECT, CREATE VIEW ON SCHEMA :ENV_edw.mart_finance TO `:ENV_uc_data_analysts`;
GRANT USE SCHEMA ON SCHEMA :ENV_edw.mart_finance TO `:ENV_uc_readonly`;
GRANT SELECT ON SCHEMA :ENV_edw.mart_finance TO `:ENV_uc_readonly`;

GRANT USE SCHEMA ON SCHEMA :ENV_edw.mart_sales TO `:ENV_uc_data_engineers`;
GRANT CREATE TABLE, CREATE VIEW, MODIFY ON SCHEMA :ENV_edw.mart_sales TO `:ENV_uc_data_engineers`;
GRANT USE SCHEMA ON SCHEMA :ENV_edw.mart_sales TO `:ENV_uc_data_analysts`;
GRANT SELECT, CREATE VIEW ON SCHEMA :ENV_edw.mart_sales TO `:ENV_uc_data_analysts`;
GRANT USE SCHEMA ON SCHEMA :ENV_edw.mart_sales TO `:ENV_uc_readonly`;
GRANT SELECT ON SCHEMA :ENV_edw.mart_sales TO `:ENV_uc_readonly`;

-- ----------------------------
-- 6) Optional: Default privileges for future objects (strongly recommended)
-- ----------------------------
-- This ensures that when engineers create new tables, analysts/readonly
-- automatically get SELECT, etc. Tune to your governance.
-- ----------------------------

-- Shared.curated: new tables get readable by analysts/readonly
ALTER SCHEMA :ENV_shared.curated
  SET DEFAULT PRIVILEGES FOR PRINCIPAL `:ENV_uc_readonly`
  GRANT SELECT ON TABLES;

ALTER SCHEMA :ENV_shared.curated
  SET DEFAULT PRIVILEGES FOR PRINCIPAL `:ENV_uc_data_analysts`
  GRANT SELECT ON TABLES;

-- EDW marts: same pattern
ALTER SCHEMA :ENV_edw.mart_finance
  SET DEFAULT PRIVILEGES FOR PRINCIPAL `:ENV_uc_readonly`
  GRANT SELECT ON TABLES;

ALTER SCHEMA :ENV_edw.mart_sales
  SET DEFAULT PRIVILEGES FOR PRINCIPAL `:ENV_uc_readonly`
  GRANT SELECT ON TABLES;

-- ----------------------------
-- 7) (Optional) Grant ability to create schemas within catalogs
-- ----------------------------
-- Use sparingly; most orgs limit schema creation to platform/admins.
-- ----------------------------
-- GRANT CREATE SCHEMA ON CATALOG :ENV_shared TO `:ENV_uc_data_engineers`;

-- ----------------------------
-- 8) Verification (read-only checks)
-- ----------------------------
SHOW CATALOGS;
SHOW SCHEMAS IN :ENV_shared;
SHOW GRANTS ON CATALOG :ENV_shared;
SHOW GRANTS ON SCHEMA :ENV_shared.curated;

-- ============================================================================
-- End
-- ============================================================================
