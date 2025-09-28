
-- Synapse External Tables for Court Proceedings Gold Layer
-- Creates external tables pointing to Delta files in ADLS for Power BI consumption

-- Step 1: Create Master Key (if not exists)
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Temp@123';

-- Step 2: Create Database Scoped Credential for ADLS access
CREATE DATABASE SCOPED CREDENTIAL [ADLSCredential]
WITH 
    IDENTITY = 'Managed Identity';

-- Step 3: Create External Data Source pointing to ADLS Gold container
CREATE EXTERNAL DATA SOURCE [GoldLayerDataSource]
WITH (
    LOCATION = 'abfss://gold@courtproceedingsstorage.dfs.core.windows.net/',
    CREDENTIAL = [ADLSCredential]
);

-- Step 4: Create External File Format for Delta files
CREATE EXTERNAL FILE FORMAT [DeltaFormat]
WITH (
    FORMAT_TYPE = PARQUET
);

-- ============================================
-- EXTERNAL TABLE 1: Fact Court Proceedings
-- ============================================
CREATE EXTERNAL TABLE [dbo].[fact_court_proceedings]
(
    [proceeding_sk] BIGINT,
    [defendant_sk] BIGINT,
    [case_details_sk] BIGINT,
    [court_sk] BIGINT,
    [case_id] NVARCHAR(50),
    [defendant_id] NVARCHAR(50),
    [case_filed_date] DATETIME2,
    [hearing_date] DATETIME2,
    [days_to_hearing] INT,
    [case_duration_days] INT,
    [needs_representation] BIT,
    [representation_priority_score] INT,
    [alert_triggered] BIT,
    [alert_priority] NVARCHAR(20),
    [intervention_needed] BIT,
    [estimated_case_cost] DECIMAL(10,2),
    [hearing_urgency_flag] NVARCHAR(20),
    [eventhub_timestamp] DATETIME2,
    [silver_processed_timestamp] DATETIME2,
    [gold_processed_timestamp] DATETIME2
)
WITH (
    LOCATION = 'fact_court_proceedings/',
    DATA_SOURCE = [GoldLayerDataSource],
    FILE_FORMAT = [DeltaFormat]
);

-- ============================================
-- EXTERNAL TABLE 2: Defendant Dimension
-- ============================================
CREATE EXTERNAL TABLE [dbo].[dim_defendant]
(
    [defendant_sk] BIGINT,
    [defendant_id] NVARCHAR(50),
    [defendant_age] INT,
    [defendant_age_group] NVARCHAR(10),
    [defendant_gender] NVARCHAR(10),
    [income_level] NVARCHAR(10),
    [defendant_zip_code] INT,
    [attorney_status] NVARCHAR(50),
    [public_defender_eligible] BIT,
    [record_effective_date] DATETIME2,
    [record_end_date] DATETIME2,
    [is_current] BIT,
    [record_version] INT,
    [created_timestamp] DATETIME2
)
WITH (
    LOCATION = 'dim_defendant/',
    DATA_SOURCE = [GoldLayerDataSource],
    FILE_FORMAT = [DeltaFormat]
);

-- ============================================
-- EXTERNAL TABLE 3: Case Details Dimension
-- ============================================
CREATE EXTERNAL TABLE [dbo].[dim_case_details]
(
    [case_details_sk] BIGINT,
    [case_id] NVARCHAR(50),
    [charge_type] NVARCHAR(50),
    [charge_severity] NVARCHAR(20),
    [case_status] NVARCHAR(30),
    [hearing_type] NVARCHAR(30),
    [record_effective_date] DATETIME2,
    [record_end_date] DATETIME2,
    [is_current] BIT,
    [record_version] INT,
    [created_timestamp] DATETIME2
)
WITH (
    LOCATION = 'dim_case_details/',
    DATA_SOURCE = [GoldLayerDataSource],
    FILE_FORMAT = [DeltaFormat]
);

-- ============================================
-- EXTERNAL TABLE 4: Court Dimension
-- ============================================
CREATE EXTERNAL TABLE [dbo].[dim_court]
(
    [court_sk] BIGINT,
    [court_district] NVARCHAR(50),
    [court_district_code] NVARCHAR(5),
    [assigned_judge] NVARCHAR(50),
    [created_timestamp] DATETIME2
)
WITH (
    LOCATION = 'dim_court/',
    DATA_SOURCE = [GoldLayerDataSource],
    FILE_FORMAT = [DeltaFormat]
);

-- ============================================
-- CREATE VIEWS FOR POWER BI
-- ============================================

-- Comprehensive view joining all dimensions for easy Power BI consumption
CREATE VIEW [dbo].[vw_court_proceedings_analytics] AS
SELECT 
    -- Fact table measures
    f.proceeding_sk,
    f.case_id,
    f.defendant_id,
    f.case_filed_date,
    f.hearing_date,
    f.days_to_hearing,
    f.case_duration_days,
    f.needs_representation,
    f.representation_priority_score,
    f.alert_triggered,
    f.alert_priority,
    f.intervention_needed,
    f.estimated_case_cost,
    f.hearing_urgency_flag,
    
    -- Defendant dimension
    dd.defendant_age,
    dd.defendant_age_group,
    dd.defendant_gender,
    dd.income_level,
    dd.defendant_zip_code,
    dd.attorney_status,
    dd.public_defender_eligible,
    
    -- Case dimension
    cd.charge_type,
    cd.charge_severity,
    cd.case_status,
    cd.hearing_type,
    
    -- Court dimension
    dc.court_district,
    dc.court_district_code,
    dc.assigned_judge,
    
    -- Calculated fields for Power BI
    CASE 
        WHEN f.representation_priority_score >= 8 THEN 'Critical'
        WHEN f.representation_priority_score >= 6 THEN 'High'
        WHEN f.representation_priority_score >= 4 THEN 'Medium'
        ELSE 'Low'
    END AS priority_category,
    
    CASE 
        WHEN f.days_to_hearing <= 3 THEN 'Urgent'
        WHEN f.days_to_hearing <= 7 THEN 'Soon' 
        ELSE 'Scheduled'
    END AS hearing_urgency_category
    
FROM [dbo].[fact_court_proceedings] f
INNER JOIN [dbo].[dim_defendant] dd ON f.defendant_sk = dd.defendant_sk
INNER JOIN [dbo].[dim_case_details] cd ON f.case_details_sk = cd.case_details_sk  
INNER JOIN [dbo].[dim_court] dc ON f.court_sk = dc.court_sk
WHERE dd.is_current = 1 AND cd.is_current = 1;

-- Summary view for executive dashboard
CREATE VIEW [dbo].[vw_representation_gap_summary] AS
SELECT 
    dc.court_district,
    dc.court_district_code,
    cd.charge_severity,
    dd.income_level,
    COUNT(*) as total_cases,
    SUM(CASE WHEN f.needs_representation = 1 THEN 1 ELSE 0 END) as cases_needing_representation,
    SUM(CASE WHEN f.alert_triggered = 1 THEN 1 ELSE 0 END) as alerts_triggered,
    AVG(CAST(f.representation_priority_score AS FLOAT)) as avg_priority_score,
    SUM(f.estimated_case_cost) as total_estimated_cost
FROM [dbo].[fact_court_proceedings] f
INNER JOIN [dbo].[dim_defendant] dd ON f.defendant_sk = dd.defendant_sk
INNER JOIN [dbo].[dim_case_details] cd ON f.case_details_sk = cd.case_details_sk  
INNER JOIN [dbo].[dim_court] dc ON f.court_sk = dc.court_sk
WHERE dd.is_current = 1 AND cd.is_current = 1
GROUP BY dc.court_district, dc.court_district_code, cd.charge_severity, dd.income_level;

-- ============================================
-- VALIDATION QUERIES
-- ============================================

-- Test external table connectivity
SELECT TOP 10 * FROM [dbo].[fact_court_proceedings];
SELECT TOP 10 * FROM [dbo].[dim_defendant];
SELECT TOP 10 * FROM [dbo].[dim_case_details];
SELECT TOP 10 * FROM [dbo].[dim_court];

-- Check record counts
SELECT * FROM [dbo].[fact_court_proceedings]
UNION ALL
SELECT * FROM [dbo].[dim_defendant]
UNION ALL  
SELECT * FROM [dbo].[dim_case_details]
UNION ALL
SELECT * FROM [dbo].[dim_court];

-- Test the analytics view
SELECT TOP 10 * FROM [dbo].[vw_court_proceedings_analytics] 
WHERE needs_representation = 1
ORDER BY representation_priority_score DESC;

-- Summary statistics for Power BI validation
SELECT 
    COUNT(*) as total_proceedings,
    SUM(CASE WHEN needs_representation = 1 THEN 1 ELSE 0 END) as needs_representation_count,
    AVG(CAST(representation_priority_score AS FLOAT)) as avg_priority_score,
    COUNT(DISTINCT court_district) as unique_courts,
    COUNT(DISTINCT defendant_id) as unique_defendants
FROM [dbo].[vw_court_proceedings_analytics];
