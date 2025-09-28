
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
