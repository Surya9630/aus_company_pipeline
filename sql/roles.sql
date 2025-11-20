-- sql/roles.sql
-- Database roles and permissions for security

-- ====================================================
-- Role 1: Read-Only Analyst
-- Purpose: For analysts who need to query data but not modify it
-- ====================================================

CREATE ROLE IF NOT EXISTS analyst_readonly;

-- Grant connection to database
GRANT CONNECT ON DATABASE {{ database_name }} TO analyst_readonly;

-- Grant usage on schema
GRANT USAGE ON SCHEMA public TO analyst_readonly;

-- Grant SELECT on all existing tables
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analyst_readonly;

-- Grant SELECT on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT ON TABLES TO analyst_readonly;

-- Grant usage on sequences (for viewing IDs)
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO analyst_readonly;

COMMENT ON ROLE analyst_readonly IS 
'Read-only access for business analysts and data analysts';

-- ====================================================
-- Role 2: ETL Pipeline
-- Purpose: For the ETL pipeline to load and transform data
-- ====================================================

CREATE ROLE IF NOT EXISTS etl_pipeline;

-- Grant connection
GRANT CONNECT ON DATABASE {{ database_name }} TO etl_pipeline;

-- Grant schema usage
GRANT USAGE, CREATE ON SCHEMA public TO etl_pipeline;

-- Grant full DML permissions on tables
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_pipeline;

-- Grant permissions on future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl_pipeline;

-- Grant sequence permissions (for SERIAL IDs)
GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA public TO etl_pipeline;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO etl_pipeline;

-- Grant TRUNCATE for staging table cleanup
GRANT TRUNCATE ON stg_abr, stg_common_crawl, stg_matched_companies TO etl_pipeline;

COMMENT ON ROLE etl_pipeline IS 
'Full DML access for ETL pipeline processes';

-- ====================================================
-- Role 3: Data Engineer (Admin)
-- Purpose: For data engineers who need to modify schema
-- ====================================================

CREATE ROLE IF NOT EXISTS data_engineer;

-- Grant connection
GRANT CONNECT ON DATABASE {{ database_name }} TO data_engineer;

-- Grant all privileges on schema
GRANT ALL PRIVILEGES ON SCHEMA public TO data_engineer;

-- Grant all privileges on tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO data_engineer;

-- Grant on future objects
ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL PRIVILEGES ON TABLES TO data_engineer;

ALTER DEFAULT PRIVILEGES IN SCHEMA public 
    GRANT ALL PRIVILEGES ON SEQUENCES TO data_engineer;

-- Allow creating/dropping tables
GRANT CREATE ON SCHEMA public TO data_engineer;

COMMENT ON ROLE data_engineer IS 
'Full administrative access for data engineering team';

-- ====================================================
-- Example Users (commented out - create as needed)
-- ====================================================

-- Create specific users and assign to roles:
-- CREATE USER analyst_user1 WITH PASSWORD 'secure_password';
-- GRANT analyst_readonly TO analyst_user1;

-- CREATE USER etl_service WITH PASSWORD 'secure_password';
-- GRANT etl_pipeline TO etl_service;

-- CREATE USER data_eng1 WITH PASSWORD 'secure_password';
-- GRANT data_engineer TO data_eng1;

-- ====================================================
-- Revoke public access (optional, for security)
-- ====================================================

-- Uncomment these lines to restrict public access:
-- REVOKE ALL ON SCHEMA public FROM PUBLIC;
-- REVOKE ALL ON ALL TABLES IN SCHEMA public FROM PUBLIC;
