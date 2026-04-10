# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Silver Transformation
# MAGIC **Reads:** Bronze managed tables | **Writes:** Silver managed tables
# MAGIC **Business logic:** Type casting, dedup, cleaning, enrichment, derived columns

# COMMAND ----------


bronze_schema = f"workspace.safecity_bronze"
silver_schema = f"workspace.safecity_silver"
spark.sql(f"create schema if not exists {silver_schema}")

# COMMAND ----------

from pyspark.sql import functions

# COMMAND ----------

df_loc = spark.table(f"{bronze_schema}.location_types") \
    .withColumn("LOCATION_TYPE_ID", col("LOCATION_TYPE_ID").cast("int")) \
    .withColumn("LOCATION_TYPE", trim(col("LOCATION_TYPE"))) \
    .dropDuplicates(["LOCATION_TYPE_ID"])

# COMMAND ----------

df_districts = spark.table(f"{bronze_schema}.districts")
df_districts.show(10)
df_districts.printSchema()


# COMMAND ----------

df_districts = spark.table(f"{bronze_schema}.districts") \
    .withColumn("DISTRICT_NUM", col("DISTRICT_NUM").cast("int")) \
    .withColumn("DISTRICT_NAME", trim(col("DISTRICT_NAME"))) \
    .dropDuplicates(["DISTRICT_NUM"])

# COMMAND ----------

df_districts.show(10)

# COMMAND ----------

df_district_name_change = spark.table("workspace.safecity_bronze.district_name_changes")

# COMMAND ----------

from pyspark.sql.functions import to_date as to_date_fn

df_district_name_change = df_district_name_change.withColumn("DISTRICT_NUM", col("DISTRICT_NUM").cast("int")) \
    .withColumn("OLD_NAME", trim(col("OLD_NAME"))) \
    .withColumn("NEW_NAME", trim(col("NEW_NAME"))) \
    .withColumn("CHANGE_DATE", to_date_fn(col("CHANGE_DATE"), "yyyy-MM-dd")) \
    .dropDuplicates(["DISTRICT_NUM","CHANGE_DATE"])


# COMMAND ----------

df_district_name_change.show(10)

# COMMAND ----------

df_iucr = spark.table(f"{bronze_schema}.iucr_codes")
df_iucr.show(10)

# COMMAND ----------

df_iucr = spark.table(f"{bronze_schema}.iucr_codes") \
    .withColumn("PRIMARY_TYPE", trim(upper(col("PRIMARY_TYPE")))) \
    .withColumn("DESCRIPTION", trim(col("DESCRIPTION"))) \
    .dropDuplicates(["IUCR_CODE"])

# COMMAND ----------

df_iucr.printSchema()

# COMMAND ----------

df_iucr = df_iucr.withColumn("PRIMARY_TYPE", trim(upper(col("PRIMARY_TYPE")))) \
    .withColumn("DESCRIPTION", trim(col("DESCRIPTION")))
    

# COMMAND ----------

df_iucr.show(10)

# COMMAND ----------

from pyspark.sql.functions import (
    col, trim, upper, to_timestamp, to_date, when, lit, row_number, hour
)
from pyspark.sql.window import Window

df_raw = spark.table(f"{bronze_schema}.crime")
df_crimes = df_raw
df_crimes = df_raw \
    .withColumn("CASE_NUMBER", trim(upper(col("CASE_NUMBER")))) \
    .withColumn("DATE", to_timestamp(col("DATE"), "MM/dd/yyyy hh:mm:ss a"))\
    .withColumn("IUCR_CODE", trim(upper(col("IUCR_CODE")))) \
    .withColumn("IUCR_CODE", col("IUCR_CODE").cast("int")) \
    .withColumn("LOCATION_TYPE", trim(col("LOCATION_TYPE"))) \
    .withColumn("ARREST", when(upper(col("ARREST")).isin("TRUE","Y","1"), 1).otherwise(0)) \
    .withColumn("DOMESTIC", when(upper(col("DOMESTIC")).isin("TRUE","Y","1"), 1).otherwise(0)) \
    .withColumn("DISTRICT_ID", col("DISTRICT").cast("int")) \
    .withColumn("BEAT", col("BEAT").cast("int")) \
    .withColumn("COMMUNITY_AREA",trim(upper(col("BLOCK"))))\
    .withColumn("LATITUDE", col("LATITUDE").cast("double")) \
    .withColumn("LONGITUDE", col("LONGITUDE").cast("double"))

# Deduplicate on CASE_NUMBER



# df_crimes.show()
# df_crimes.write.mode("overwrite").saveAsTable(f"{silver_schema}.crimes")
# print(f"silver.crimes: {spark.table(f'{silver_schema}.crimes').count()} rows")

# COMMAND ----------


# Derived columns — KEEP null lat/lon, flag them
df_crimes = df_crimes \
    .withColumn("CRIME_DATE", to_date(col("DATE"))) \
    .withColumn("CRIME_HOUR", hour(col("DATE"))) \
    .withColumn("HAS_LOCATION", when(col("LATITUDE").isNotNull() & col("LONGITUDE").isNotNull(), 1).otherwise(0)) \
    .withColumn("TIME_BLOCK",
        when(col("CRIME_HOUR").between(5,8), "EARLY_MORNING")
        .when(col("CRIME_HOUR").between(9,11), "MORNING")
        .when(col("CRIME_HOUR").between(12,14), "AFTERNOON")
        .when(col("CRIME_HOUR").between(15,17), "EVENING")
        .when(col("CRIME_HOUR").between(18,20), "NIGHT")
        .otherwise("LATE_NIGHT")
    ) 

# COMMAND ----------

df_iucr = df_iucr.withColumn("SEVERITY",
        when(col("PRIMARY_TYPE").isin("HOMICIDE","ASSAULT","ROBBERY","KIDNAPPING"), "VIOLENT")
        .when(col("PRIMARY_TYPE").isin("THEFT","BURGLARY","MOTOR VEHICLE THEFT"), "PROPERTY")
        .otherwise("OTHER")
    )

# COMMAND ----------



# COMMAND ----------

df_iucr.show(10)

# COMMAND ----------

df_crimes.show(10)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import *
# Deduplicate on CASE_NUMBER, keeping the latest record by _loaded_at
window_dedup = Window.partitionBy("CASE_NUMBER").orderBy(col("_bronze_load_timestamp").desc())
df_crimes = df_crimes.withColumn("_rn", row_number().over(window_dedup)) \
     .filter(col("_rn") == 1).drop("_rn")

# COMMAND ----------

df_districts.groupBy("DISTRICT_NUM").agg(count("*").alias("cnt")).filter(col("cnt")>1).show()

# COMMAND ----------

df_base = df_districts.select(
    "DISTRICT_NUM",
    "DISTRICT_NAME"
).withColumn("CHANGE_DATE", lit(None))

# COMMAND ----------

df_changes = df_district_name_change.select(
    "DISTRICT_NUM",
    col("old_name").alias("DISTRICT_NAME"),
    "CHANGE_DATE"
)

# COMMAND ----------

df_all = df_base.unionByName(df_changes)

# COMMAND ----------

from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window

window = Window.partitionBy("DISTRICT_NUM") \
    .orderBy(col("CHANGE_DATE").desc_nulls_last())

df_final = df_all.withColumn(
    "rn",
    row_number().over(window)
).withColumn(
    "IS_CURRENT",
    (col("rn") == 1).cast("int")
).drop("rn")

# COMMAND ----------

from pyspark.sql.functions import col

df_district_geo = df_districts.select(
    "DISTRICT_NUM",
    col("LATITUDE"),
    col("LONGITUDE")
)

df_final = df_final.join(
    df_district_geo,
    "DISTRICT_NUM",
    "left"
)

# COMMAND ----------

df_final.show()

# COMMAND ----------



# COMMAND ----------

df_iucr.printSchema()
df_final.printSchema()
df_crimes.printSchema()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df_joined = df_crimes.join(
    df_iucr,
    "IUCR_CODE",
    "left"
)

df_joined.show(10)

# COMMAND ----------

df_joined = df_joined.join(
    df_final.filter(col("is_current") == 1),
    df_joined["DISTRICT_ID"] == df_final["DISTRICT_NUM"],
    "left"
)
df_joined.printSchema()

# COMMAND ----------

df_joined = df_joined.drop("_bronze_load_timestamp", "CHANGE_DATE", "IS_CURRENT","_bronze_batch_num","_bronze_source_file")

# COMMAND ----------

df_joined.show(5,truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC df_joined is crimes silver 
# MAGIC df_final is district 
# MAGIC df_

# COMMAND ----------



# COMMAND ----------

# TODO: Add derived columns + _silver_load_timestamp + _silver_batch_num

# COMMAND ----------

# TODO: .saveAsTable(f"{CATALOG}.{SCHEMA_SILVER}.crimes_enriched")
