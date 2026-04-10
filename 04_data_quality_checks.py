# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Data Quality Checks (26 Tests)

# COMMAND ----------

BRONZE_PATH = "s3://your-safecity360-bucket/safecity360/bronze/"
SILVER_PATH = "s3://your-safecity360-bucket/safecity360/silver/"
GOLD_PATH   = "s3://your-safecity360-bucket/safecity360/gold/"
DQ_LOG_PATH = "s3://your-safecity360-bucket/safecity360/dq_logs/"

dbutils.widgets.text("batch_num", "01")
batch_num = dbutils.widgets.get("batch_num")

from pyspark.sql import functions as F
from datetime import datetime
dq_results = []

def run_test(test_id, test_name, category, layer, expected, actual):
    status = "PASS" if actual == expected else "FAIL"
    dq_results.append({"test_id":test_id,"test_name":test_name,"category":category,
        "layer":layer,"expected":str(expected),"actual":str(actual),"status":status,
        "batch_num":batch_num,"run_timestamp":datetime.now().isoformat()})
    print(f"{'✅' if status=='PASS' else '❌'} {test_id}: {test_name} — Exp:{expected} Got:{actual}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completeness (DQ-01 to DQ-05)

# COMMAND ----------

# DQ-01: Bronze not empty | DQ-02: Silver not empty
# DQ-03: No null CASE_NUMBER in Bronze
# DQ-04: No null CRIME_DATETIME in Silver
# DQ-05: No null DISTRICT in Silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Uniqueness (DQ-06 to DQ-08)

# COMMAND ----------

# DQ-06: No duplicate CASE_NUMBER in Silver
# DQ-07: No duplicate CASE_NUMBER in Gold fact
# DQ-08: DIM_CRIME_TYPE unique IUCR_CODE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validity (DQ-09 to DQ-14)

# COMMAND ----------

# DQ-09: All IUCR_CODEs in fact exist in DIM_CRIME_TYPE
# DQ-10: All DISTRICT nums exist in DIM_DISTRICT
# DQ-11: ARREST values only "true"/"false" in Silver
# DQ-12: CRIME_DATETIME within 2020 in Silver
# DQ-13: LATITUDE between 41.6 and 42.1 where not null (Chicago bounds)
# DQ-14: LONGITUDE between -87.9 and -87.5 where not null

# COMMAND ----------

# MAGIC %md
# MAGIC ## Consistency (DQ-15 to DQ-18)

# COMMAND ----------

# DQ-15: Silver <= Bronze row count
# DQ-16: Gold fact == Silver count
# DQ-17: IS_ARREST=1 only when ARREST="true"
# DQ-18: SEVERITY correctly mapped from PRIMARY_TYPE

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referential Integrity (DQ-19 to DQ-21)

# COMMAND ----------

# DQ-19: All DATE_KEYs in fact exist in DIM_DATE
# DQ-20: SCD2 — one IS_CURRENT=True per DISTRICT
# DQ-21: Aggregate TOTAL_INCIDENTS matches transactional count

# COMMAND ----------

# MAGIC %md
# MAGIC ## Freshness (DQ-22 to DQ-23)

# COMMAND ----------

# DQ-22: Bronze has current batch data
# DQ-23: Silver timestamp within 24 hours

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accuracy (DQ-24 to DQ-26)

# COMMAND ----------

# DQ-24: ARREST_RATE in aggregate matches manual calculation
# DQ-25: TOTAL_INCIDENTS in aggregate matches COUNT(*)
# DQ-26: No district has 100% arrest rate (sanity)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Results

# COMMAND ----------

# TODO: df_dq = spark.createDataFrame(dq_results)
# df_dq.write.format("delta").mode("append").save(DQ_LOG_PATH)
