# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Gold Layer Load
# MAGIC **Star Schema:** FACT_CRIME_INCIDENTS (per-incident) + FACT_MONTHLY_DISTRICT_SUMMARY (per-month-district)
# MAGIC **SCD2 on:** DIM_DISTRICT (Wentworth → Prairie (reorganization), Near West → Monroe)

# COMMAND ----------

CATALOG = "main"
SCHEMA_SILVER = "safecity_silver"
SCHEMA_GOLD = "safecity_gold"
dbutils.widgets.text("batch_num", "01")
batch_num = dbutils.widgets.get("batch_num")
from pyspark.sql import functions as F
from delta.tables import DeltaTable
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_GOLD}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensions

# COMMAND ----------

# TODO: Build all dimension tables as managed Delta
# DIM_DATE (Type 0) — generate 2020 date dimension
# DIM_DISTRICT (SCD Type 2) — Wentworth → Prairie (reorganization), Near West → Monroe
# Other dimensions (Type 0/1)
# Save each as: f"{CATALOG}.{SCHEMA_GOLD}.dim_xxx"

# COMMAND ----------

# MAGIC %md
# MAGIC ## FACT_CRIME_INCIDENTS (per-incident)

# COMMAND ----------

# TODO: Build transactional fact from Silver
# .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fact_crime_incidents")

# COMMAND ----------

# MAGIC %md
# MAGIC ## FACT_MONTHLY_DISTRICT_SUMMARY (per-month-district)

# COMMAND ----------

# TODO: Build aggregate fact using groupBy
# .saveAsTable(f"{CATALOG}.{SCHEMA_GOLD}.fact_monthly_district_summary")
