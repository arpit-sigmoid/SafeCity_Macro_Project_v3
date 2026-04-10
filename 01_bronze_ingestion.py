# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Bronze Ingestion
# MAGIC **Layer:** Bronze (Raw)
# MAGIC **Purpose:** Read raw crime CSVs from S3, add metadata, write as Delta.

# COMMAND ----------

S3_LANDING_PATH   = "s3://your-safecity360-bucket/safecity360/landing/"
S3_REFERENCE_PATH = "s3://your-safecity360-bucket/safecity360/reference/"
BRONZE_PATH       = "s3://your-safecity360-bucket/safecity360/bronze/"

dbutils.widgets.text("batch_num", "01")
batch_num = dbutils.widgets.get("batch_num")

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Raw Crime Data

# COMMAND ----------

crime_schema = StructType([
    StructField("CASE_NUMBER", StringType(), True),
    StructField("DATE", StringType(), True),
    StructField("IUCR_CODE", StringType(), True),
    StructField("DISTRICT", IntegerType(), True),
    StructField("BEAT", IntegerType(), True),
    StructField("BLOCK", StringType(), True),
    StructField("LOCATION_TYPE", StringType(), True),
    StructField("ARREST", StringType(), True),
    StructField("DOMESTIC", StringType(), True),
    StructField("LATITUDE", StringType(), True),
    StructField("LONGITUDE", StringType(), True),
    StructField("YEAR", IntegerType(), True),
])

# TODO: Read batch CSV
# df_raw = spark.read.csv(f"{S3_LANDING_PATH}batch_{batch_num}/", header=True, schema=crime_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Add Bronze Metadata

# COMMAND ----------

# TODO:
# df_bronze = df_raw \
#     .withColumn("_bronze_load_timestamp", F.current_timestamp()) \
#     .withColumn("_bronze_batch_num", F.lit(batch_num)) \
#     .withColumn("_bronze_source_file", F.input_file_name())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Bronze Delta

# COMMAND ----------

# TODO:
# df_bronze.write.format("delta").mode("append").option("mergeSchema","true").save(f"{BRONZE_PATH}crimes/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Reference Data (Batch 1 only)

# COMMAND ----------

# TODO: Load reference CSVs (run once during Batch 1)
# if batch_num == "01":
#     for ref in ["districts", "iucr_codes", "location_types", "district_name_changes"]:
#         df = spark.read.csv(f"{S3_REFERENCE_PATH}{ref}.csv", header=True, inferSchema=True)
#         df.write.format("delta").mode("overwrite").save(f"{BRONZE_PATH}{ref}/")
#         print(f"Loaded {ref}: {df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify

# COMMAND ----------

# TODO: Print row count and sample
# df_v = spark.read.format("delta").load(f"{BRONZE_PATH}crimes/")
# print(f"Total Bronze crimes: {df_v.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Screenshot Checklist
# MAGIC - [ ] Bronze row count after each batch
# MAGIC - [ ] Sample rows with _bronze_* metadata
# MAGIC - [ ] Reference tables loaded (districts, iucr_codes, location_types)
