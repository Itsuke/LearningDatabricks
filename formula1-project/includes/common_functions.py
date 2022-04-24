# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_sdf):
    output_sdf = input_sdf.withColumn("ingestion_date", current_timestamp())
    return output_sdf

# COMMAND ----------


