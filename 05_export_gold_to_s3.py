# Databricks notebook source
# MAGIC %md
# MAGIC # SafeCity 360 — Export Gold Tables to S3 via boto3
# MAGIC **Runs AFTER DQ passes.** Reads managed Delta tables, writes Parquet to S3.
# MAGIC **SECURITY:** AWS keys hardcoded for training. In production, use Databricks Secrets.

# COMMAND ----------

import boto3, io, pandas as pd

AWS_ACCESS_KEY = "YOUR_AWS_ACCESS_KEY"      # TODO
AWS_SECRET_KEY = "YOUR_AWS_SECRET_KEY"      # TODO
AWS_REGION     = "us-east-1"                # TODO
S3_BUCKET      = "your-safecity360-bucket"   # TODO
S3_GOLD_PREFIX = "safecity360/gold"

CATALOG = "main"
SCHEMA_GOLD = "safecity_gold"

s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY, region_name=AWS_REGION)

# COMMAND ----------

def export_table(table_name):
    pdf = spark.table(f"{CATALOG}.{SCHEMA_GOLD}.{table_name}").toPandas()
    buf = io.BytesIO()
    pdf.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    key = f"{S3_GOLD_PREFIX}/{table_name}/{table_name}.parquet"
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    print(f"✅ {table_name}: {len(pdf)} rows → s3://{S3_BUCKET}/{key}")
    return len(pdf)

# COMMAND ----------

gold_tables = ['fact_crime_incidents', 'fact_monthly_district_summary', 'dim_district', 'dim_crime_type', 'dim_date', 'dim_time_block', 'dim_location_type']
total = 0
for t in gold_tables:
    total += export_table(t)
print(f"\nExport complete: {len(gold_tables)} tables, {total} rows")
