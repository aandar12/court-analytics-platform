# Databricks Silver Layer Transformation
# Bronze (Raw JSON) -> Silver (Cleaned Data + Core Business Logic)

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName("CourtProceedings-Silver") \
    .getOrCreate()

# Storage configuration
storage_account = "courtproceedingsstorage"
storage_key = dbutils.secrets.get(scope="cpanalyticsvaultscope", key="storage-connection")

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_key
)

# Define paths
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/court_proceedings"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/court_proceedings"

print("Starting Silver Layer Transformation...")
print(f"Reading from Bronze: {bronze_path}")
print(f"Writing to Silver: {silver_path}")

# Read Bronze layer data (raw JSON)
bronze_df = spark.read.format("delta").load(bronze_path)

print(f"Bronze records loaded: {bronze_df.count()}")

# Define schema for JSON parsing
court_schema = StructType([
    StructField("case_id", StringType(), True),
    StructField("defendant_id", StringType(), True),
    StructField("charge_type", StringType(), True),
    StructField("charge_severity", StringType(), True),
    StructField("case_status", StringType(), True),
    StructField("attorney_status", StringType(), True),
    StructField("defendant_age", IntegerType(), True),
    StructField("defendant_gender", StringType(), True),
    StructField("income_level", StringType(), True),
    StructField("defendant_zip_code", IntegerType(), True),
    StructField("court_district", StringType(), True),
    StructField("assigned_judge", StringType(), True),
    StructField("hearing_type", StringType(), True),
    StructField("case_filed_date", StringType(), True),
    StructField("hearing_date", StringType(), True),
    StructField("record_timestamp", StringType(), True)
])

# Parse JSON and flatten structure
parsed_df = bronze_df.select(
    from_json(col("raw_json"), court_schema).alias("court_data"),
    col("eventhub_timestamp"),
    col("eventhub_partition"), 
    col("eventhub_offset"),
    col("bronze_ingestion_timestamp")
).select("court_data.*", "eventhub_timestamp", "bronze_ingestion_timestamp")

print("JSON parsing completed")

# Data Type Conversions and Cleaning
cleaned_df = parsed_df.select(
    col("case_id"),
    col("defendant_id"),
    col("charge_type"),
    col("charge_severity"),
    col("case_status"),
    col("attorney_status"),
    col("defendant_age").cast("integer"),
    col("defendant_gender"),
    col("income_level"),
    col("defendant_zip_code").cast("integer"),
    col("court_district"),
    col("assigned_judge"),
    col("hearing_type"),
    # Convert string dates to timestamps
    to_timestamp(col("case_filed_date")).alias("case_filed_date"),
    to_timestamp(col("hearing_date")).alias("hearing_date"),
    to_timestamp(col("record_timestamp")).alias("record_timestamp"),
    col("eventhub_timestamp"),
    col("bronze_ingestion_timestamp")
)

print("Data type conversions completed")

# Data Quality Fixes and Essential Business Logic
silver_df = cleaned_df.select(
    col("case_id"),
    col("defendant_id"),
    col("charge_type"),
    col("charge_severity"),
    col("case_status"),
    col("attorney_status"),
    col("defendant_age"),
    col("defendant_gender"),
    col("income_level"),
    
    # Fix missing zip codes
    when(col("defendant_zip_code").isNull(), 99999)
    .otherwise(col("defendant_zip_code")).alias("defendant_zip_code"),
    
    col("court_district"),
    col("assigned_judge"),
    col("hearing_type"),
    col("case_filed_date"),
    
    # Fix future hearing dates (data quality issue)
    when(col("hearing_date") > current_timestamp(), 
         col("case_filed_date") + expr("INTERVAL 14 DAYS"))
    .otherwise(col("hearing_date")).alias("hearing_date"),
    
    col("record_timestamp"),
    col("eventhub_timestamp"),
    col("bronze_ingestion_timestamp")
).withColumn(
    # Calculate days to hearing (operational metric)
    "days_to_hearing", 
    datediff(col("hearing_date"), current_timestamp())
).withColumn(
    # Calculate case duration (operational metric)
    "case_duration_days",
    datediff(current_timestamp(), col("case_filed_date"))
).withColumn(
    # Core business flag: needs_representation
    # This is fundamental enough to belong in Silver layer
    "needs_representation",
    when(
        # Already has representation
        col("attorney_status").isin(["Private Attorney", "Public Defender Assigned"]), False
    ).when(
        # High priority scenarios - needs representation
        (col("charge_severity") == "Felony") & 
        (col("attorney_status").isin(["Self-Represented", "Attorney Pending"])), True
    ).when(
        # Medium priority - low income without attorney
        (col("income_level") == "Low") & 
        (col("attorney_status").isin(["Self-Represented", "Public Defender Requested", "Attorney Pending"])), True
    ).when(
        # Urgent cases - hearing soon without attorney
        (col("days_to_hearing") <= 7) & 
        (col("attorney_status").isin(["Self-Represented", "Attorney Pending"])), True
    ).otherwise(False)
).withColumn(
    # Basic eligibility flag
    "public_defender_eligible", 
    col("income_level") == "Low"
).withColumn(
    # Derived age groups for analytics (from raw age)
    "defendant_age_group",
    when(col("defendant_age") <= 25, "18-25")
    .when(col("defendant_age") <= 35, "26-35")
    .when(col("defendant_age") <= 45, "36-45") 
    .when(col("defendant_age") <= 55, "46-55")
    .when(col("defendant_age") <= 65, "56-65")
    .otherwise("65+")
).withColumn(
    # Derived court district code
    "court_district_code",
    when(col("court_district").contains("Manhattan"), "MN")
    .when(col("court_district").contains("Brooklyn"), "BK") 
    .when(col("court_district").contains("Queens"), "QN")
    .when(col("court_district").contains("Bronx"), "BX")
    .when(col("court_district").contains("Staten Island"), "SI")
    .otherwise("Unknown")
).withColumn(
    # Add Silver layer metadata
    "silver_processed_timestamp", current_timestamp()
).withColumn(
    # Data quality tracking
    "data_quality_flag",
    when(col("defendant_zip_code") == 99999, "MISSING_ZIP_CODE")
    .otherwise("CLEAN")
)

print("Data cleaning and core business logic completed")

# Data Quality Validation Report
print("\n=== Silver Layer Data Quality Report ===")
total_records = silver_df.count()
print(f"Total records processed: {total_records}")

# Check data quality metrics
null_case_ids = silver_df.filter(col("case_id").isNull()).count()
null_defendant_ids = silver_df.filter(col("defendant_id").isNull()).count()
missing_zip_codes = silver_df.filter(col("defendant_zip_code") == 99999).count()
needs_representation_count = silver_df.filter(col("needs_representation") == True).count()

print(f"Records with missing case IDs: {null_case_ids}")
print(f"Records with missing defendant IDs: {null_defendant_ids}")  
print(f"Records with missing zip codes (fixed): {missing_zip_codes}")
print(f"Defendants needing representation: {needs_representation_count} ({needs_representation_count/total_records*100:.1f}%)")

# Show sample of cleaned data
print("\n=== Sample Silver Layer Data ===")
silver_df.select(
    "case_id", "attorney_status", "charge_severity", "income_level", 
    "days_to_hearing", "needs_representation", "data_quality_flag"
).show(10, truncate=False)

# Write to Silver layer
print(f"\nWriting cleaned data to Silver layer: {silver_path}")

silver_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(silver_path)

print("Silver layer transformation completed!")



# Summary statistics
print("\n=== Silver Layer Summary ===")
attorney_status_summary = silver_df.groupBy("attorney_status", "needs_representation").count().orderBy("count", ascending=False)
attorney_status_summary.show()

print("\n=== Court District Summary ===")
district_summary = silver_df.groupBy("court_district_code").agg(
    count("*").alias("total_cases"),
    sum(when(col("needs_representation"), 1).otherwise(0)).alias("needs_representation_count")
).withColumn(
    "representation_gap_rate",
    round(col("needs_representation_count") / col("total_cases") * 100, 1)
).orderBy("needs_representation_count", ascending=False)

district_summary.show()

print("Silver layer ready for Gold layer dimensional modeling!")
