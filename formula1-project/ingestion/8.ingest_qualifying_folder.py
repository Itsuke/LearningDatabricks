# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest JSON files from catalog from Ergast Developer DB 

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
    .json(f"{raw_incr_load_catalog_path}/{v_file_date}/qualifying/qualifying_split_*.json", multiLine=True)

# COMMAND ----------

display(qualifying_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename and add new columns
# MAGIC 1. Rename qualifyId, raceId, driverId, constructorId
# MAGIC 2. Add data_source from input param
# MAGIC 3. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_modified_sdf = qualifying_sdf \
    .withColumnRenamed("qualifyId", "qualify_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

qualifying_final_sdf = add_ingestion_date(qualifying_modified_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write ouput to the processed container in parquet fromat

# COMMAND ----------

qualifying_final_sdf = qualifying_final_sdf.select(reorder_columns_with_partition_param_at_the_end(qualifying_final_sdf, "race_id"))

# COMMAND ----------

increment_load_data(qualifying_final_sdf, "f1_processed", "qualifying", "race_id")

# COMMAND ----------

dbutils.notebook.exit("succes")
