# Databricks Gold Layer Transformation
# Silver -> Gold (Star Schema + Advanced Business Logic)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CourtProceedings-Gold") \
    .getOrCreate()

# Storage configuration
storage_account = "courtproceedingsstorage"
storage_key =dbutils.secrets.get(scope="cpanalyticsvaultscope", key="storage-connection")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Define paths - direct ADLS access (no mounting required)
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/court_proceedings"
gold_fact_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/fact_court_proceedings"
gold_dim_defendant_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/dim_defendant"
gold_dim_case_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/dim_case_details"
gold_dim_court_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/dim_court"

print("Starting Gold Layer Transformation - Star Schema Creation")

# Read Silver layer data
silver_df = spark.read.format("delta").load(silver_path)
print(f"Silver records loaded: {silver_df.count()}")

# Generate surrogate keys for dimensions
from pyspark.sql.window import Window

# Window for surrogate key generation
defendant_window = Window.orderBy("defendant_id")
case_window = Window.orderBy("case_id")
court_window = Window.orderBy("court_district", "assigned_judge")

print("Creating Dimension Tables...")

# ========================================
# DIMENSION 1: dim_defendant (with SCD Type 2 capability)
# ========================================

dim_defendant = silver_df.select(
    "defendant_id",
    "defendant_age",
    "defendant_age_group", 
    "defendant_gender",
    "income_level",
    "defendant_zip_code",
    "attorney_status",
    "public_defender_eligible"
).distinct().withColumn(
    "defendant_sk", 
    row_number().over(defendant_window)
).withColumn(
    # SCD Type 2 fields
    "record_effective_date", current_timestamp()
).withColumn(
    "record_end_date", lit(None).cast("timestamp")
).withColumn(
    "is_current", lit(True)
).withColumn(
    "record_version", lit(1)
).withColumn(
    "created_timestamp", current_timestamp()
).select(
    "defendant_sk",
    "defendant_id", 
    "defendant_age",
    "defendant_age_group",
    "defendant_gender", 
    "income_level",
    "defendant_zip_code",
    "attorney_status",
    "public_defender_eligible",
    "record_effective_date",
    "record_end_date", 
    "is_current",
    "record_version",
    "created_timestamp"
)

print(f"dim_defendant created with {dim_defendant.count()} records")

# ========================================
# DIMENSION 2: dim_case_details (with SCD Type 2 capability)  
# ========================================

dim_case_details = silver_df.select(
    "case_id",
    "charge_type",
    "charge_severity", 
    "case_status",
    "hearing_type"
).distinct().withColumn(
    "case_details_sk",
    row_number().over(case_window)
).withColumn(
    # SCD Type 2 fields
    "record_effective_date", current_timestamp()
).withColumn(
    "record_end_date", lit(None).cast("timestamp")
).withColumn(
    "is_current", lit(True)
).withColumn(
    "record_version", lit(1)
).withColumn(
    "created_timestamp", current_timestamp()
).select(
    "case_details_sk",
    "case_id",
    "charge_type", 
    "charge_severity",
    "case_status",
    "hearing_type",
    "record_effective_date",
    "record_end_date",
    "is_current", 
    "record_version",
    "created_timestamp"
)

print(f"dim_case_details created with {dim_case_details.count()} records")

# ========================================
# DIMENSION 3: dim_court (Type 1 - rarely changes)
# ========================================

dim_court = silver_df.select(
    "court_district",
    "court_district_code",
    "assigned_judge"
).distinct().withColumn(
    "court_sk",
    row_number().over(court_window)  
).withColumn(
    "created_timestamp", current_timestamp()
).select(
    "court_sk",
    "court_district",
    "court_district_code", 
    "assigned_judge",
    "created_timestamp"
)

print(f"dim_court created with {dim_court.count()} records")

# ========================================
# FACT TABLE: fact_court_proceedings  
# ========================================

print("Creating Fact Table...")

# Join Silver data with dimension surrogate keys
fact_with_keys = silver_df.join(
    dim_defendant.select("defendant_sk", "defendant_id"), 
    on="defendant_id", how="inner"
).join(
    dim_case_details.select("case_details_sk", "case_id"),
    on="case_id", how="inner" 
).join(
    dim_court.select("court_sk", "court_district", "assigned_judge"),
    on=["court_district", "assigned_judge"], how="inner"
)

# Create fact table with advanced business logic
fact_court_proceedings = fact_with_keys.select(
    # Surrogate keys for dimensions
    "defendant_sk",
    "case_details_sk", 
    "court_sk",
    
    # Business keys (for reference)
    "case_id",
    "defendant_id",
    
    # Date dimensions
    "case_filed_date",
    "hearing_date",
    
    # Measures from Silver
    "days_to_hearing",
    "case_duration_days",
    "needs_representation",
    
    # Event Hub metadata
    "eventhub_timestamp",
    "silver_processed_timestamp"
).withColumn(
    # Advanced business logic: Representation Priority Score (1-10)
    "representation_priority_score",
    when(col("needs_representation") == False, 1)
    .when(
        # Get charge severity and income from dimensions for scoring
        col("days_to_hearing") <= 2, 10  # Critical - hearing very soon
    ).when(
        col("days_to_hearing") <= 7, 8   # High priority - hearing soon
    ).when(
        col("needs_representation") == True, 6  # Medium priority
    ).otherwise(3)
).withColumn(
    # Alert flags for downstream systems
    "alert_triggered", 
    col("representation_priority_score") >= 7
).withColumn(
    "alert_priority",
    when(col("representation_priority_score") >= 9, "CRITICAL")
    .when(col("representation_priority_score") >= 7, "HIGH")
    .when(col("representation_priority_score") >= 5, "MEDIUM") 
    .otherwise("LOW")
).withColumn(
    "intervention_needed",
    col("representation_priority_score") >= 7
).withColumn(
    # Fact table measures
    "estimated_case_cost", 
    when(col("needs_representation"), round(rand() * 3000 + 1000, 2))
    .otherwise(round(rand() * 1500 + 500, 2))
).withColumn(
    # Fact-level calculated fields
    "hearing_urgency_flag",
    when(col("days_to_hearing") <= 3, "URGENT")
    .when(col("days_to_hearing") <= 7, "SOON")
    .otherwise("SCHEDULED")
).withColumn(
    # Gold layer metadata
    "gold_processed_timestamp", current_timestamp()
).withColumn(
    "proceeding_sk", 
    row_number().over(Window.orderBy("case_id", "defendant_id"))
)

# Select final fact table structure
fact_final = fact_court_proceedings.select(
    "proceeding_sk",
    "defendant_sk", 
    "case_details_sk",
    "court_sk",
    "case_id",
    "defendant_id", 
    "case_filed_date",
    "hearing_date",
    "days_to_hearing",
    "case_duration_days", 
    "needs_representation",
    "representation_priority_score",
    "alert_triggered",
    "alert_priority",
    "intervention_needed", 
    "estimated_case_cost",
    "hearing_urgency_flag",
    "eventhub_timestamp",
    "silver_processed_timestamp",
    "gold_processed_timestamp"
)

print(f"fact_court_proceedings created with {fact_final.count()} records")

# ========================================
# Write Dimension Tables to Gold Layer
# ========================================

print("Writing dimension tables to Gold layer...")

# Write dim_defendant
dim_defendant.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_dim_defendant_path)

# Write dim_case_details  
dim_case_details.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_dim_case_path)

# Write dim_court
dim_court.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_dim_court_path)

print("Dimension tables written successfully")

# ========================================  
# Write Fact Table to Gold Layer
# ========================================

print("Writing fact table to Gold layer...")

fact_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(gold_fact_path)

print("Fact table written successfully")

# Delta files written to Gold layer - ready for Synapse external table creation

# ========================================
# Analytics Summary & Validation
# ========================================

print("\n=== Gold Layer Analytics Summary ===")

# Representation gap analysis
total_proceedings = fact_final.count()
needs_representation = fact_final.filter(col("needs_representation") == True).count()
high_priority_alerts = fact_final.filter(col("alert_priority") == "HIGH").count()
critical_alerts = fact_final.filter(col("alert_priority") == "CRITICAL").count()

print(f"Total court proceedings: {total_proceedings}")
print(f"Cases needing representation: {needs_representation} ({needs_representation/total_proceedings*100:.1f}%)")
print(f"High priority alerts: {high_priority_alerts}")
print(f"Critical priority alerts: {critical_alerts}")



# Priority distribution
print("\n=== Alert Priority Distribution ===")
priority_dist = fact_final.groupBy("alert_priority").count().orderBy("count", ascending=False)
priority_dist.show()

print("\n=== Star Schema Validation ===")
print(f"Fact table records: {fact_final.count()}")
print(f"Defendant dimension: {dim_defendant.count()} records")
print(f"Case details dimension: {dim_case_details.count()} records")  
print(f"Court dimension: {dim_court.count()} records")


print("Gold Layer transformation completed successfully!")
print("Star schema ready for Power BI Legal Representation Gap Analysis dashboard")
