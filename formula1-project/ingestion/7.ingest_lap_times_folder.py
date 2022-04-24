# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest CSV files from Ergast Developer DB 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV files with Spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# COMMAND ----------

lap_times_schema = StructType([ 
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), False),
    StructField("lap", IntegerType()),
    StructField("position", IntegerType()),
    StructField("time", StringType()),
    StructField("milliseconds", IntegerType())
])

# COMMAND ----------

lap_times_sdf = spark.read \
    .schema(lap_times_schema) \
    .csv("/mnt/formula1datalakestudy/raw/lap_times/lap_times_split_*.csv")

# COMMAND ----------

display(lap_times_sdf)

# COMMAND ----------

lap_times_sdf.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_times_final_sdf = lap_times_sdf \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("Ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write ouput to the parquet file

# COMMAND ----------

lap_times_final_sdf.write.parquet("/mnt/formula1datalakestudy/processed/lap_times", mode="overwrite")

# COMMAND ----------


