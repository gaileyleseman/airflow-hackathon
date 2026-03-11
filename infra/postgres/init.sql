-- =============================================================================
-- PostgreSQL initialization script for data hackathon
-- Creates schemas following the medallion architecture pattern
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Schema: raw
-- Stores raw JSON payloads exactly as received, no transformation
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS raw;
GRANT ALL PRIVILEGES ON SCHEMA raw TO hackathon;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO hackathon;
ALTER DEFAULT PRIVILEGES IN SCHEMA raw GRANT ALL PRIVILEGES ON TABLES TO hackathon;
COMMENT ON SCHEMA raw IS 'Stores raw JSON payloads exactly as received, no transformation';

-- -----------------------------------------------------------------------------
-- Schema: staging
-- dbt views that unpack JSONB into typed columns
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging;
GRANT ALL PRIVILEGES ON SCHEMA staging TO hackathon;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO hackathon;
ALTER DEFAULT PRIVILEGES IN SCHEMA staging GRANT ALL PRIVILEGES ON TABLES TO hackathon;
COMMENT ON SCHEMA staging IS 'dbt views that unpack JSONB into typed columns';

-- -----------------------------------------------------------------------------
-- Schema: intermediate
-- dbt views with business logic and joins across sources
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS intermediate;
GRANT ALL PRIVILEGES ON SCHEMA intermediate TO hackathon;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA intermediate TO hackathon;
ALTER DEFAULT PRIVILEGES IN SCHEMA intermediate GRANT ALL PRIVILEGES ON TABLES TO hackathon;
COMMENT ON SCHEMA intermediate IS 'dbt views with business logic and joins across sources';

-- -----------------------------------------------------------------------------
-- Schema: marts
-- dbt tables, final consumption layer
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS marts;
GRANT ALL PRIVILEGES ON SCHEMA marts TO hackathon;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO hackathon;
ALTER DEFAULT PRIVILEGES IN SCHEMA marts GRANT ALL PRIVILEGES ON TABLES TO hackathon;
COMMENT ON SCHEMA marts IS 'dbt tables, final consumption layer';

-- =============================================================================
-- Raw tables for ingestion
-- These tables store raw JSON payloads as-is from source files.
-- Staging models are responsible for unpacking the JSONB into typed columns.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Table: raw.citizens
-- Stores citizen data as raw JSON payloads
-- The payload column contains the full JSON object as-is from the source file.
-- Staging models are responsible for unpacking it into typed columns.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.citizens (
    id          SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source_file VARCHAR NOT NULL,
    payload     JSONB NOT NULL
);
COMMENT ON TABLE raw.citizens IS 'Stores citizen data as raw JSON payloads. The payload column contains the full JSON object as-is from the source file. Staging models are responsible for unpacking it.';

-- -----------------------------------------------------------------------------
-- Table: raw.logins
-- Stores login event data as raw JSON payloads
-- The payload column contains the full JSON object as-is from the source file.
-- Staging models are responsible for unpacking it into typed columns.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.logins (
    id          SERIAL PRIMARY KEY,
    ingested_at TIMESTAMP NOT NULL DEFAULT NOW(),
    source_file VARCHAR NOT NULL,
    payload     JSONB NOT NULL
);
COMMENT ON TABLE raw.logins IS 'Stores login event data as raw JSON payloads. The payload column contains the full JSON object as-is from the source file. Staging models are responsible for unpacking it.';
