# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest pitstops.json file from Ergast Developer DB 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file with Spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

pit_stops_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType()),
    StructField("stop", StringType()),
    StructField("lap", IntegerType()),
    StructField("time", StringType()),
    StructField("duration", StringType()),
    StructField("milliseconds", IntegerType())
])

# COMMAND ----------

pit_stops_sdf = spark.read \
    .schema(pit_stops_schema) \
    .json("/mnt/formula1datalakestudy/raw/pit_stops.json", multiLine=True)

# COMMAND ----------

display(pit_stops_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_stops_final_sdf = pit_stops_sdf \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("Ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write ouput to the parquet file

# COMMAND ----------

pit_stops_final_sdf.write.parquet("/mnt/formula1datalakestudy/processed/pit_stops", mode="overwrite")

# COMMAND ----------


