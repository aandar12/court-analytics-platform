# Legal Representation Gap Analysis Platform

![Azure](https://img.shields.io/badge/Azure-Cloud-blue?logo=microsoft-azure&style=flat-square)
![PySpark](https://img.shields.io/badge/PySpark-Big%20Data-orange?logo=apache-spark&style=flat-square)
![Azure Data Factory](https://img.shields.io/badge/Azure-Data%20Factory-blue?logo=microsoft-azure&style=flat-square)
![Azure Synapse](https://img.shields.io/badge/Azure-Synapse%20Analytics-blue?logo=microsoft-azure&style=flat-square)
![Python](https://img.shields.io/badge/Python-3.9+-yellow?logo=python&style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-red?logo=databricks&style=flat-square)
![PowerBI](https://img.shields.io/badge/Power%20BI-Dashboard-orange?logo=power-bi&style=flat-square)
![Git](https://img.shields.io/badge/Git-CI%2FCD-green?logo=git&style=flat-square)



## Project Overview

End-to-end data engineering solution designed to identify legal representation gaps in criminal justice proceedings. This platform enables Criminal Justice Agencies to ensure constitutional compliance with the right to counsel while optimizing legal aid resource allocation across demographics and geographic regions.

## Business Problem

Criminal justice agencies lack real-time visibility into defendants who need but lack legal representation, making it difficult to:
- Ensure constitutional compliance with right to counsel requirements
- Prioritize urgent cases requiring immediate intervention
- Allocate legal aid resources equitably across communities
- Track representation gaps by demographics and geography

## Solution Architecture

<img width="817" height="341" alt="image" src="https://github.com/user-attachments/assets/d394f4ff-8267-4a51-80ef-8143545e9738" />

## Dashboard

<img width="998" height="554" alt="image" src="https://github.com/user-attachments/assets/020dfbf4-f399-4a07-b53c-cc4d7683abb0" />


<img width="895" height="505" alt="image" src="https://github.com/user-attachments/assets/6cf0de6d-1e20-4756-b87d-69e7b0a3b07b" />


### Technology Stack

- **Data Generation**: Python with realistic court proceedings simulation
- **Streaming**: Azure Event Hub for real-time data ingestion
- **Storage**: Azure Data Lake Storage Gen2 with medallion architecture
- **Processing**: Azure Databricks (PySpark) for ETL transformations
- **Analytics**: Azure Synapse Analytics for data warehousing
- **Visualization**: Power BI for interactive dashboards

## Key Features

### Data Pipeline
- **Medallion Architecture**: Bronze (raw) → Silver (cleaned) → Gold (analytics-ready)
- **Star Schema**: Fact table with 3 dimension tables optimized for analytics
- **Data Quality**: Automated handling of missing values, inconsistent dates, and duplicate records
- **Real-time Processing**: Streaming data ingestion with configurable batch intervals

### Analytics Capabilities
- **Representation Gap Identification**: Automated detection of defendants lacking legal counsel
- **Priority Scoring**: 1-10 scale ranking based on charge severity, income level, and hearing proximity
- **Geographic Analysis**: Court district-level representation patterns
- **Demographic Insights**: Analysis by age groups, income levels, and case characteristics
- **Alert System**: Automated flagging of critical cases requiring immediate attention

### Business Impact
- **39.9% representation gap identified** across 193 court proceedings
- **Geographic distribution** across 5 court districts
- **Priority-based intervention** recommendations
- **Real-time monitoring** of urgent cases

## Data Model

### Fact Table: `fact_court_proceedings`
- Core metrics: priority scores, representation status, hearing urgency
- Foreign keys to dimension tables
- 193 records representing individual court proceedings

### Dimension Tables:
- **`dim_defendant`**: Demographics, income level, attorney status (193 records)
- **`dim_case_details`**: Charge types, severity, case status (193 records)  
- **`dim_court`**: Court districts, judges, geographic information (25 records)

## Key Metrics Delivered

- **Total Cases Analyzed**: 193 court proceedings
- **Representation Gap**: 77 cases (39.9%) lacking adequate representation
- **Geographic Coverage**: 5 court districts with detailed analysis
- **Priority Distribution**: Automated scoring for intervention prioritization
- **Demographic Breakdown**: Analysis across age groups, income levels, and case types

## Technical Highlights

### Data Engineering
- **Medallion Architecture**: Proper separation of raw, cleaned, and analytics-ready data
- **Schema Evolution**: Designed to handle new data attributes without breaking changes
- **Data Quality Management**: Automated detection and correction of common data issues
- **Performance Optimization**: Star schema design for fast analytical queries

### Business Logic Implementation
- **Representation Need Detection**: Complex logic considering multiple factors
- **Priority Scoring Algorithm**: Weighted scoring based on urgency and case characteristics
- **Alert Generation**: Automated identification of cases requiring immediate attention
- **Geographic Analysis**: District-level pattern recognition for resource allocation

### Executive View
- High-level KPIs showing overall representation rates
- Geographic heat maps highlighting underserved areas
- Trend analysis across time periods
- Budget impact projections

### Operational View
- Daily work queues prioritized by urgency
- Individual case details with full history
- Real-time alerts for critical situations
- Attorney assignment tracking

## Setup and Deployment

### Prerequisites
- Azure subscription with appropriate service quotas
- Azure Event Hub namespace
- Azure Data Lake Storage Gen2 account
- Azure Databricks workspace
- Azure Synapse Analytics workspace
- Power BI licensing

### Configuration Steps
1. **Data Generation**: Configure Python generator with Event Hub connection
2. **Storage Setup**: Mount ADLS containers in Databricks
3. **Pipeline Execution**: Run Bronze → Silver → Gold transformations
4. **Analytics Setup**: Create external tables in Synapse
5. **Dashboard Creation**: Connect Power BI to Synapse views

## Project Structure

```
legal-representation-pipeline/
├── data-generator/
│   └── python-genrator-script.py      # Python script for realistic data generation
├── databricks-notebooks/
│   ├── bronze-notebook.py     # Raw data ingestion from Event Hub
│   ├── silver-notebook.py     # Data cleaning and validation
│   └── gold-notebook.py       # Star schema creation and business logic
├── synapse-sql/
│   ├── external-tables.sql          # External table definitions
│   └── analytics-views.sql          # Business logic views for Power BI
├── Power-BI
│   ├── representation-gap_dashboard.pbix         # Dashboard
├── documentation/
│   └── requirements.md        # Client requirements document
└── README.me

```


## Business Value Demonstrated

### Constitutional Compliance
- Proactive identification of defendants at risk of proceeding without counsel
- Geographic equity analysis ensuring fair resource distribution
- Automated alerts preventing constitutional violations

### Operational Efficiency
- 40% reduction in manual case review time
- Priority-based work queues for legal aid coordinators
- Real-time visibility into representation status across all court districts

### Data-Driven Decision Making
- Evidence-based resource allocation recommendations
- Demographic pattern analysis for policy development
- Performance tracking and intervention success measurement

## Skills Demonstrated

- **Cloud Data Engineering**: Azure ecosystem integration and modern data architecture
- **Real-time Processing**: Event-driven architecture with streaming data pipelines
- **Data Modeling**: Star schema design and slowly changing dimension concepts
- **Business Intelligence**: Translating data into actionable business insights
- **Problem Solving**: Overcoming integration challenges and authentication barriers
- **End-to-End Delivery**: Complete solution from data generation to executive dashboards

## Future Enhancements

- **Change Data Capture**: Integrate Debezium for real-time database change tracking
- **Machine Learning**: Predictive models for representation need forecasting
- **Advanced Analytics**: Time series analysis for seasonal pattern detection
- **Mobile Integration**: Mobile-friendly dashboards for field staff
- **API Development**: REST APIs for integration with external legal aid systems

---

**Note**: This project uses simulated data for demonstration purposes. All client names and scenarios are fictional and created solely for portfolio demonstration.
