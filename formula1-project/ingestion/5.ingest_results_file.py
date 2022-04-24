# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest results.json file from Ergast Developer DB 

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
    .json("/mnt/formula1datalakestudy/raw/results.json")

# COMMAND ----------

display(results_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

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
    .withColumn("Ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns 

# COMMAND ----------

results_final_sdf = results_modified_sdf.drop(results_modified_sdf["statusId"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write ouput to the parquet file

# COMMAND ----------

results_final_sdf.write.parquet("/mnt/formula1datalakestudy/processed/results", mode="overwrite", partitionBy="race_id")

# COMMAND ----------


