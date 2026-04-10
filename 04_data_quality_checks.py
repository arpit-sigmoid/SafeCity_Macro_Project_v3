# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Data Quality Checks (26 Tests)
# MAGIC **Runs on:** Managed Delta tables | **Purpose:** DQ is the GATE before S3 export

# COMMAND ----------

CATALOG = "main"
SCHEMA_BRONZE = "safecity_bronze"; SCHEMA_SILVER = "safecity_silver"; SCHEMA_GOLD = "safecity_gold"
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
    print(f"{\"✅\" if status==\"PASS\" else \"❌\"} {test_id}: {test_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Completeness (DQ-01 to DQ-05) | Uniqueness (DQ-06 to DQ-08)
# MAGIC ## Validity (DQ-09 to DQ-14) | Consistency (DQ-15 to DQ-18)
# MAGIC ## Referential Integrity (DQ-19 to DQ-21) | Freshness (DQ-22 to DQ-23) | Accuracy (DQ-24 to DQ-26)

# COMMAND ----------

# TODO: Implement all 26 tests (see understanding_your_data.txt for DQ details)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log Results

# COMMAND ----------

# spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.safecity_dq")
# spark.createDataFrame(dq_results).write.format("delta").mode("append") \
#     .saveAsTable(f"{CATALOG}.safecity_dq.dq_results")
# passed = len([r for r in dq_results if r["status"]=="PASS"])
# print(f"DQ Complete: {passed}/{len(dq_results)} PASSED")
