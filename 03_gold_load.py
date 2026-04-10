# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Gold Layer Load
# MAGIC **2 Facts:** FACT_CRIME_INCIDENTS + FACT_MONTHLY_DISTRICT_SUMMARY
# MAGIC **7 Dims:** DIM_DATE, DIM_DISTRICT (SCD2), DIM_CRIME_TYPE, DIM_TIME_BLOCK, DIM_LOCATION_TYPE, DIM_SEVERITY, DIM_ARREST_STATUS

# COMMAND ----------

SILVER_PATH = "s3://your-safecity360-bucket/safecity360/silver/"
GOLD_PATH   = "s3://your-safecity360-bucket/safecity360/gold/"
BRONZE_PATH = "s3://your-safecity360-bucket/safecity360/bronze/"

dbutils.widgets.text("batch_num", "01")
batch_num = dbutils.widgets.get("batch_num")

from pyspark.sql import functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# TODO: df_silver = spark.read.format("delta").load(f"{SILVER_PATH}crimes_enriched/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM_DATE (Type 0)

# COMMAND ----------

# TODO: Generate date dimension for 2020
# Same pattern as SkyLens — explode sequence, extract year/month/day/quarter/weekend

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM_DISTRICT (SCD Type 2) — tracks district name changes

# COMMAND ----------

# TODO: Implement SCD2
# First run: Create initial dimension from districts reference
# Subsequent: Apply changes from district_name_changes.csv
# Columns: DISTRICT_SK, DISTRICT_NUM, DISTRICT_NAME, EFFECTIVE_DATE, END_DATE, IS_CURRENT

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM_CRIME_TYPE (Type 1)

# COMMAND ----------

# TODO: Build from IUCR codes
# df_dim_crime = df_iucr.withColumn("CRIME_TYPE_SK", F.monotonically_increasing_id())

# COMMAND ----------

# MAGIC %md
# MAGIC ## DIM_TIME_BLOCK, DIM_LOCATION_TYPE, DIM_SEVERITY, DIM_ARREST_STATUS (Type 0)

# COMMAND ----------

# TODO: Build remaining dimensions from Silver distinct values

# COMMAND ----------

# MAGIC %md
# MAGIC ## FACT_CRIME_INCIDENTS (Transactional — per-incident grain)

# COMMAND ----------

# TODO:
# df_fact = df_silver.select(
#     F.col("CASE_NUMBER"),
#     F.date_format("CRIME_DATE","yyyyMMdd").cast(IntegerType()).alias("DATE_KEY"),
#     F.col("DISTRICT"),
#     F.col("IUCR_CODE"),
#     F.col("LOCATION_TYPE"),
#     F.col("TIME_BLOCK"),
#     F.col("SEVERITY"),
#     F.col("IS_ARREST"),
#     F.col("IS_DOMESTIC"),
#     F.col("HAS_LOCATION"),
#     F.col("LATITUDE"),
#     F.col("LONGITUDE"),
#     F.col("BEAT"),
# )
# df_fact.write.format("delta").mode("append").save(f"{GOLD_PATH}fact_crime_incidents/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FACT_MONTHLY_DISTRICT_SUMMARY (Aggregate)

# COMMAND ----------

# TODO:
# df_agg = df_silver.groupBy(
#     F.date_format("CRIME_DATE","yyyyMM").alias("YEAR_MONTH"),
#     F.col("DISTRICT"),
#     F.col("PRIMARY_TYPE"),
#     F.col("SEVERITY"),
# ).agg(
#     F.count("*").alias("TOTAL_INCIDENTS"),
#     F.sum("IS_ARREST").alias("TOTAL_ARRESTS"),
#     F.sum("IS_DOMESTIC").alias("TOTAL_DOMESTIC"),
#     F.sum("HAS_LOCATION").alias("GEOCODED_COUNT"),
#     F.round(F.avg("IS_ARREST"), 3).alias("ARREST_RATE"),
# )
# df_agg.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}fact_monthly_district_summary/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Screenshot Checklist
# MAGIC - [ ] All dimension tables with counts
# MAGIC - [ ] DIM_DISTRICT SCD2 columns
# MAGIC - [ ] FACT_CRIME_INCIDENTS count + sample
# MAGIC - [ ] FACT_MONTHLY_DISTRICT_SUMMARY count
