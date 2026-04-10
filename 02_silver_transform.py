# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Silver Transformation
# MAGIC **Layer:** Silver (Cleaned & Enriched)
# MAGIC **Purpose:** Parse dates, clean invalid records, enrich with crime type and district lookups.

# COMMAND ----------

BRONZE_PATH = "s3://your-safecity360-bucket/safecity360/bronze/"
SILVER_PATH = "s3://your-safecity360-bucket/safecity360/silver/"

dbutils.widgets.text("batch_num", "01")
batch_num = dbutils.widgets.get("batch_num")

from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze (Current Batch)

# COMMAND ----------

# TODO:
# df_bronze = spark.read.format("delta").load(f"{BRONZE_PATH}crimes/") \
#     .filter(F.col("_bronze_batch_num") == batch_num)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Parse Date Column
# MAGIC Chicago crime dates are in "MM/dd/yyyy hh:mm:ss a" format (12-hour with AM/PM).

# COMMAND ----------

# TODO: Parse the DATE string to proper timestamp
# df_typed = df_bronze \
#     .withColumn("CRIME_DATETIME", F.to_timestamp("DATE", "MM/dd/yyyy hh:mm:ss a")) \
#     .withColumn("LATITUDE", F.col("LATITUDE").cast(DoubleType())) \
#     .withColumn("LONGITUDE", F.col("LONGITUDE").cast(DoubleType()))
#
# # Rows with unparseable dates will have CRIME_DATETIME = null
# bad_dates = df_typed.filter(F.col("CRIME_DATETIME").isNull()).count()
# print(f"Unparseable dates: {bad_dates}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deduplication

# COMMAND ----------

# TODO:
# before = df_typed.count()
# df_deduped = df_typed.dropDuplicates(["CASE_NUMBER"])
# print(f"Removed {before - df_deduped.count()} duplicates")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Data Cleaning Rules

# COMMAND ----------

# TODO:
# Rule 1: Remove rows with null CRIME_DATETIME (bad date format)
# df_cleaned = df_deduped.filter(F.col("CRIME_DATETIME").isNotNull())
#
# Rule 2: Remove invalid IUCR codes (not in reference)
# df_iucr = spark.read.format("delta").load(f"{BRONZE_PATH}iucr_codes/")
# valid_iucr = [r.IUCR_CODE for r in df_iucr.select("IUCR_CODE").collect()]
# df_cleaned = df_cleaned.filter(F.col("IUCR_CODE").isin(valid_iucr))
#
# Rule 3: Normalize ARREST flag (must be "true" or "false")
# df_cleaned = df_cleaned.filter(F.col("ARREST").isin("true", "false"))
#
# Rule 4: Handle null lat/lon — KEEP rows but flag them
# df_cleaned = df_cleaned.withColumn("HAS_LOCATION",
#     F.when(F.col("LATITUDE").isNotNull() & F.col("LONGITUDE").isNotNull(), 1).otherwise(0))
#
# print(f"Rows after cleaning: {df_cleaned.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Enrich with Reference Data

# COMMAND ----------

# TODO:
# df_districts = spark.read.format("delta").load(f"{BRONZE_PATH}districts/")
# df_iucr = spark.read.format("delta").load(f"{BRONZE_PATH}iucr_codes/")
#
# df_enriched = df_cleaned \
#     .join(df_districts.select(
#         F.col("DISTRICT_NUM").alias("DISTRICT"),
#         F.col("DISTRICT_NAME")
#     ), "DISTRICT", "left") \
#     .join(df_iucr, "IUCR_CODE", "left")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Derived Columns

# COMMAND ----------

# TODO:
# df_silver = df_enriched \
#     .withColumn("CRIME_DATE", F.to_date("CRIME_DATETIME")) \
#     .withColumn("CRIME_HOUR", F.hour("CRIME_DATETIME")) \
#     .withColumn("CRIME_DAY_OF_WEEK", F.dayofweek("CRIME_DATETIME")) \
#     .withColumn("IS_ARREST", F.when(F.col("ARREST") == "true", 1).otherwise(0)) \
#     .withColumn("IS_DOMESTIC", F.when(F.col("DOMESTIC") == "true", 1).otherwise(0)) \
#     .withColumn("TIME_BLOCK",
#         F.concat(F.lpad(F.hour("CRIME_DATETIME"),2,"0"), F.lit("00-"),
#                  F.lpad(F.hour("CRIME_DATETIME"),2,"0"), F.lit("59"))) \
#     .withColumn("SEVERITY",
#         F.when(F.col("PRIMARY_TYPE").isin("HOMICIDE","ASSAULT","ROBBERY","BATTERY"), "VIOLENT")
#          .when(F.col("PRIMARY_TYPE").isin("BURGLARY","THEFT","MOTOR VEHICLE THEFT","ARSON"), "PROPERTY")
#          .otherwise("OTHER")) \
#     .withColumn("_silver_load_timestamp", F.current_timestamp()) \
#     .withColumn("_silver_batch_num", F.lit(batch_num))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write Silver

# COMMAND ----------

# TODO:
# df_silver.write.format("delta").mode("append").option("mergeSchema","true") \
#     .save(f"{SILVER_PATH}crimes_enriched/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Screenshot Checklist
# MAGIC - [ ] Bad date count, invalid IUCR count, bad arrest flag count
# MAGIC - [ ] Duplicate removal count
# MAGIC - [ ] Sample enriched rows with DISTRICT_NAME, PRIMARY_TYPE, SEVERITY
# MAGIC - [ ] Silver total row count
