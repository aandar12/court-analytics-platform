
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

FROM [dbo].[vw_court_proceedings_analytics];
