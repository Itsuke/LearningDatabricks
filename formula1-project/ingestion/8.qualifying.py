# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest JSON files from catalog from Ergast Developer DB 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON files with Spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

qualifying_schema = StructType([ 
    StructField("qualifyId", IntegerType(), False),
    StructField("raceId", IntegerType()),
    StructField("driverId", IntegerType()),
    StructField("constructorId", IntegerType()),
    StructField("number", IntegerType()),
    StructField("position", IntegerType()),
    StructField("q1", StringType()),
    StructField("q2", StringType()),
    StructField("q3", StringType())
])

# COMMAND ----------

qualifying_sdf = spark.read \
    .schema(qualifying_schema) \
    .json("/mnt/formula1datalakestudy/raw/qualifying/qualifying_split_*.json", multiLine=True)

# COMMAND ----------

display(qualifying_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_final_sdf = qualifying_sdf \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("Ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write ouput to the parquet file

# COMMAND ----------

qualifying_final_sdf.write.parquet("/mnt/formula1datalakestudy/processed/qualifying", mode="overwrite")

# COMMAND ----------


