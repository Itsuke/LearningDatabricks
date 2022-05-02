# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest CSV files from Ergast Developer DB 

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
    .csv(f"{raw_catalog_path}/lap_times/lap_times_split_*.csv")

# COMMAND ----------

display(lap_times_sdf)

# COMMAND ----------

lap_times_sdf.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename and add columns
# MAGIC 1. Rename raceId, driverId
# MAGIC 2. Add data_source from input param
# MAGIC 3. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_modified_sdf = lap_times_sdf \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

lap_times_final_sdf = add_ingestion_date(lap_times_modified_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write ouput to the processed container in parquet fromat

# COMMAND ----------

lap_times_final_sdf = lap_times_final_sdf.select(reorder_columns_with_partition_param_at_the_end(lap_times_final_sdf, "race_id"))

# COMMAND ----------

increment_load_data(lap_times_final_sdf, "f1_processed", "lap_times", "race_id")

# COMMAND ----------

dbutils.notebook.exit("succes")

# COMMAND ----------


