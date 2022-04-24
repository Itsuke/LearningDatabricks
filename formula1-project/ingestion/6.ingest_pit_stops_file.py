# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest pitstops.json file from Ergast Developer DB 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "No source input")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
    .json(f"{raw_catalog_path}/pit_stops.json", multiLine=True)

# COMMAND ----------

display(pit_stops_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename and add new columns
# MAGIC 1. Rename raceId, driverId
# MAGIC 2. Add data_source from input param
# MAGIC 3. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pit_stops_modified_sdf = pit_stops_sdf \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

pit_stops_final_sdf = add_ingestion_date(pit_stops_modified_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write ouput to the processed container in parquet fromat

# COMMAND ----------

pit_stops_final_sdf.write.parquet(f"{processed_catalog_path}/pit_stops", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("succes")
