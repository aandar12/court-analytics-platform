# Databricks Bronze Layer - Raw Data Ingestion
# Event Hub -> ADLS Bronze Container (Minimal Processing)

from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = "analytics-adarsh.servicebus.windows.net"
event_hub_name = "court-proceedings-eh"  
event_hub_conn_str = dbutils.secrets.get(scope = "cpanalyticsvaultscope", key = "eventhub-connection")

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# Read from Event Hub
print("Connecting to Event Hub for raw data ingestion...")
raw_df = (spark.readStream
          .format("kafka")
          .options(**kafka_options)
          .load()
          )

# Cast data to string and add minimal Bronze layer metadata
bronze_df = raw_df.select(
    col("value").cast("string").alias("raw_json"),
    col("timestamp").alias("eventhub_timestamp"),
    col("partition").alias("eventhub_partition"),
    col("offset").alias("eventhub_offset"),
    current_timestamp().alias("bronze_ingestion_timestamp")
)

# ADLS configuration 
storage_account = "courtproceedingsstorage"
storage_key = dbutils.secrets.get(scope = "cpanalyticsvaultscope", key ="storage-connection")  # Replace with actual key

spark.conf.set(
  f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
  storage_key
)

# Bronze layer paths
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/court_proceedings"
checkpoint_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/_checkpoints/court_proceedings"

print(f"Writing raw data to Bronze layer: {bronze_path}")

# Write stream to Bronze layer (raw JSON preserved)
query = (
    bronze_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime="30 seconds")
    .start(bronze_path)
)

print("Bronze layer ingestion started!")
print("Storing raw JSON data with Event Hub metadata")
print("Data will be parsed and structured in Silver layer")

# Monitor the stream
query.awaitTermination()

# Optional: View sample data (uncomment to monitor)
"""
# Display sample raw data for monitoring
display_query = (
    bronze_df
    .writeStream
    .format("console")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .option("truncate", "false")
    .start()
)
"""
