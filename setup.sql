
-- =============================================================================
-- Cortex Project Intel – Setup Script
-- Creates the projects table and cortex_complete function.
-- =============================================================================

-- Use ACCOUNTADMIN to have sufficient privileges
USE ROLE ACCOUNTADMIN;

-- Create the database and schema (if they don't exist)
CREATE DATABASE IF NOT EXISTS COCO_HACKATHON_DB;
CREATE SCHEMA IF NOT EXISTS COCO_HACKATHON_DB.CORE_DATA;

-- Set the context
USE DATABASE COCO_HACKATHON_DB;
USE SCHEMA CORE_DATA;
USE WAREHOUSE COMPUTE_WH;   -- or any warehouse you have

-- =============================================================================
-- 1. Projects table – stores completed projects for institutional memory
-- =============================================================================
CREATE OR REPLACE TABLE projects (
    project_id         INT AUTOINCREMENT,
    project_name       STRING,
    description        STRING,
    start_date         DATE,
    end_date           DATE,
    status             STRING,                     -- 'Completed', 'In Progress', etc.
    lead_architect     STRING,
    technologies_used  STRING,
    wbs_summary        STRING,
    risk_log_summary   STRING,
    test_cases_summary STRING,
    deployment_plan    STRING
);

-- Insert sample completed projects
INSERT INTO projects (project_name, description, start_date, end_date, status, lead_architect, technologies_used, wbs_summary, risk_log_summary, test_cases_summary, deployment_plan) VALUES
('Legacy Data Migration 2022',
 'Migrated on-prem Oracle data warehouse to Snowflake.',
 '2022-01-15', '2022-09-20', 'Completed',
 'Alice Chen',
 'Snowflake, AWS S3, DBT',
 '1. Discovery & Assessment (2 wks)\n2. Schema Design (4 wks)\n3. Data Transfer (8 wks)\n4. Validation (4 wks)\n5. Cutover (2 wks)',
 'Risks: Data validation delays (mitigated by automated scripts).\nIssues: Unexpected PII in source (added column masking).\nAssumptions: Source data would be clean.\nDependencies: Network team for VPN.',
 'Test data completeness, PII masking validation, performance benchmarks',
 'Phased cutover with rollback option; weekend switch'),

('Customer 360 Analytics Platform',
 'Real-time customer view from Salesforce, Zendesk, web events.',
 '2023-03-01', '2023-11-15', 'Completed',
 'David Lee',
 'Snowflake, Streamlit, Kafka, dbt',
 '1. Requirements (3 wks)\n2. Data Ingestion (6 wks)\n3. Modeling (5 wks)\n4. Streamlit App (5 wks)\n5. UAT (2 wks)',
 'Risks: Real-time latency (optimized Kafka connectors).\nIssues: Scope creep (change control process).\nAssumptions: APIs stable.\nDependencies: Salesforce team.',
 'Streaming data validation, dashboard load testing, end-to-end latency checks',
 'Blue-green deployment with feature flags'),

('Marketing Attribution Revamp',
 'Multi-touch attribution model.',
 '2024-05-10', NULL, 'In Progress',
 'Maria Garcia',
 'Snowflake, Python, Airflow',
 '1. Data Audit (4 wks)\n2. Model Dev (8 wks)\n3. Back-testing (3 wks)\n4. Deployment (2 wks)',
 'Risks: Model accuracy low (parallel testing).\nIssues: Data quality (profiling UDFs).\nAssumptions: Historical data available.\nDependencies: Marketing ops.',
 'Attribution accuracy tests, data quality checks, performance benchmarks',
 'Shadow mode for one month before full switch');

-- =============================================================================
-- 2. Cortex function – used for AI prompts
-- =============================================================================
CREATE OR REPLACE FUNCTION cortex_complete(prompt_text STRING)
RETURNS STRING
AS
$$
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',   -- model available in many regions
        prompt_text
    )
$$;

-- =============================================================================
-- 3. (Optional) Table metadata view – not used by this app, but can be present
-- =============================================================================
-- (No need to create; app uses INFORMATION_SCHEMA if needed)

-- Verify counts
SELECT 'Setup complete' AS status;
SELECT COUNT(*) AS project_count FROM projects;
