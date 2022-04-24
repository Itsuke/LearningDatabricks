# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest results.json file from Ergast Developer DB 

# COMMAND ----------

dbutils.widgets.text("p_data_source", "No source input")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file with Spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

results_schema = StructType([
    StructField("resultId", IntegerType(), False),
    StructField("raceId", IntegerType()),
    StructField("driverId", IntegerType()),
    StructField("constructorId", IntegerType()),
    StructField("number", IntegerType()),
    StructField("grid", IntegerType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("positionOrder", IntegerType()),
    StructField("points", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("time", StringType()),
    StructField("milliseconds", IntegerType()),
    StructField("fastestLap", IntegerType()),
    StructField("rank", IntegerType()),
    StructField("fastestLapTime", StringType()),
    StructField("fastestLapSpeed", StringType()),
    StructField("statusId", IntegerType())
])

# COMMAND ----------

results_sdf = spark.read \
    .schema(results_schema) \
    .json(f"{raw_catalog_path}/results.json")

# COMMAND ----------

display(results_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename and add new columns
# MAGIC 1. Rename resultId, raceId, driverId, constructorId, positionText, positionOrder, fastestLap, fastestLapTime, fastestLapSpeed
# MAGIC 2. Add data_source from input param
# MAGIC 3. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_modified_sdf = results_sdf \
    .withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")  \
    .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

results_modified_sdf = add_ingestion_date(results_modified_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns

# COMMAND ----------

results_final_sdf = results_modified_sdf.drop(results_modified_sdf["statusId"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write ouput to the processed container in parquet fromat partitioned by race_id

# COMMAND ----------

results_final_sdf.write.parquet(f"{processed_catalog_path}/results", mode="overwrite", partitionBy="race_id")

# COMMAND ----------

dbutils.notebook.exit("succes")
