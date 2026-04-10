# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Silver Transformation
# MAGIC **Reads:** Bronze managed tables | **Writes:** Silver managed tables
# MAGIC **Business logic:** Type casting, dedup, cleaning, enrichment, derived columns

# COMMAND ----------

CATALOG = "main"
SCHEMA_BRONZE = "safecity_bronze"
SCHEMA_SILVER = "safecity_silver"
dbutils.widgets.text("batch_num", "01")
batch_num = dbutils.widgets.get("batch_num")
from pyspark.sql import functions as F
from pyspark.sql.types import *
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read Bronze (current batch)

# COMMAND ----------

# TODO: df_bronze = spark.table(f"{CATALOG}.{SCHEMA_BRONZE}.crimes").filter(F.col("_bronze_batch_num") == batch_num)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Type Casting

# COMMAND ----------

# TODO: Cast string columns to proper types

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Deduplication

# COMMAND ----------

# TODO: dropDuplicates on primary key

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Cleaning Rules

# COMMAND ----------

# TODO: Apply domain-specific cleaning rules (see understanding_your_data.txt for details)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Enrich with Reference Data

# COMMAND ----------

# TODO: Join with reference tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Derived Columns

# COMMAND ----------

# TODO: Add derived columns + _silver_load_timestamp + _silver_batch_num

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Write Silver

# COMMAND ----------

# TODO: .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.crimes_enriched")
