# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest pitstops.json file from Ergast Developer DB 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "No source input")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

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
    .json(f"{raw_incr_load_catalog_path}/{v_file_date}/pit_stops.json", multiLine=True)

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
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

pit_stops_final_sdf = add_ingestion_date(pit_stops_modified_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write ouput to the processed container in parquet fromat

# COMMAND ----------

pit_stops_final_sdf = pit_stops_final_sdf.select(reorder_columns_with_partition_param_at_the_end(pit_stops_final_sdf, "race_id"))

# COMMAND ----------

increment_load_data(pit_stops_final_sdf, "f1_processed", "pit_stops", "race_id")

# COMMAND ----------

dbutils.notebook.exit("succes")

# COMMAND ----------


