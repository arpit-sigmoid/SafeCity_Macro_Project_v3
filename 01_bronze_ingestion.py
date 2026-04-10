# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Bronze Ingestion
# MAGIC **Reads:** Public S3 via spark.read | **Writes:** Managed Delta (Unity Catalog)

# COMMAND ----------

S3_RAW_PATH       = "s3://your-safecity360-bucket/safecity360/raw/"        # TODO
S3_REFERENCE_PATH = "/Volumes/workspace/safecity/reference/"  # TODO
CATALOG = "workspace"
SCHEMA_BRONZE = "safecity_bronze"

dbutils.widgets.text("batch_num", "01")
batch_num = dbutils.widgets.get("batch_num")

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql 
# MAGIC create catalog if not exists safecity

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_BRONZE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Raw Data from S3

# COMMAND ----------

S3_RAW_PATH = "/Volumes/workspace/safecity/raw"

# COMMAND ----------

# TODO: Define schema and read CSV
df_raw = spark.read.csv(f"{S3_RAW_PATH}/batch_{batch_num}/", header=True, inferSchema=True)
print(f"Raw rows: {df_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add Bronze Metadata + Write

# COMMAND ----------

# MAGIC %sql
# MAGIC show catalogs
# MAGIC ;
# MAGIC use catalog workspace;
# MAGIC show databases;
# MAGIC

# COMMAND ----------

df_bronze = df_raw

# COMMAND ----------

df_bronze = df_raw \
    .withColumn("_bronze_load_timestamp", F.current_timestamp()) \
    .withColumn("_bronze_batch_num", F.lit(batch_num)) \
    .withColumn("_bronze_source_file", F.lit(f"{S3_RAW_PATH}/batch_{batch_num}/"))

df_bronze.write.format("delta").mode("append").option("mergeSchema","true") \
    .saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.crime")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Reference Data (Batch 1 only)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_BRONZE}.districts")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_BRONZE}.iucr_codes")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_BRONZE}.location_types")
spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA_BRONZE}.district_name_changes")

# COMMAND ----------

# TODO: Run once

if batch_num == "01":
    spark.read.csv(f"{S3_REFERENCE_PATH}districts.csv", header=True, inferSchema=True) \
        .withColumn("_bronze_load_timestamp", F.current_timestamp()) \
        .withColumn("_bronze_source_file", F.lit(f"{S3_REFERENCE_PATH}districts.csv")) \
        .write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.districts")
    spark.read.csv(f"{S3_REFERENCE_PATH}iucr_codes.csv", header=True, inferSchema=True) \
        .withColumn("_bronze_load_timestamp", F.current_timestamp()) \
        .withColumn("_bronze_source_file", F.lit(f"{S3_REFERENCE_PATH}iucr_codes.csv")) \
        .write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.iucr_codes")
    spark.read.csv(f"{S3_REFERENCE_PATH}location_types.csv", header=True, inferSchema=True) \
        .withColumn("_bronze_load_timestamp", F.current_timestamp()) \
        .withColumn("_bronze_source_file", F.lit(f"{S3_REFERENCE_PATH}location_types.csv")) \
        .write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.location_types")
    spark.read.csv(f"{S3_REFERENCE_PATH}district_name_changes.csv", header=True, inferSchema=True) \
        .withColumn("_bronze_load_timestamp", F.current_timestamp()) \
        .withColumn("_bronze_source_file", F.lit(f"{S3_REFERENCE_PATH}district_name_changes.csv")) \
        .write.format("delta").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA_BRONZE}.district_name_changes")
    print("Reference data loaded.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify

# COMMAND ----------

# TODO: Print row counts for all tables
