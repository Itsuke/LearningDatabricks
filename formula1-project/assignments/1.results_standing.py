# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Produce driver standings as on https://www.bbc.com/sport/formula1/results

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the data

# COMMAND ----------

races_sdf = spark.read.parquet(f"{processed_catalog_path}/races") \
    .select("race_id", "circuit_id", "race_year", "name", "race_timestamp") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

circuits_sdf = spark.read.parquet(f"{processed_catalog_path}/circuits") \
    .select("circuit_id", "name") \
    .withColumnRenamed("name", "circuit_location")

# COMMAND ----------

drivers_sdf = spark.read.parquet(f"{processed_catalog_path}/drivers") \
    .select("driver_id", "name", "number", "nationality") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_sdf = spark.read.parquet(f"{processed_catalog_path}/constructors") \
    .select("constructor_id", "name") \
    .withColumnRenamed("name", "constructors_name")

# COMMAND ----------

results_sdf = spark.read.parquet(f"{processed_catalog_path}/results") \
    .select("race_id", "driver_id", "constructor_id", "grid", "fastest_lap_time", "time", "laps", "points", "position", "file_date") \
    .filter(f"file_date = '{v_file_date}'") \
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 - Join required data

# COMMAND ----------

result_combined_sdf = results_sdf.join(races_sdf, "race_id", "inner")

# COMMAND ----------

result_combined_sdf = result_combined_sdf.join(circuits_sdf, "circuit_id", "inner")

# COMMAND ----------

result_combined_sdf = result_combined_sdf.join(drivers_sdf, "driver_id", "inner")

# COMMAND ----------

result_combined_sdf = result_combined_sdf.join(constructors_sdf, "constructor_id", "inner")

# COMMAND ----------

display(result_combined_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 3 - Drop unwanted columns

# COMMAND ----------

result_dropped_sdf = result_combined_sdf.drop("constructor_id", "driver_id", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Add the modify timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

result_final_sdf = result_dropped_sdf.withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 - Store results

# COMMAND ----------

result_list = reorder_columns_with_partition_param_at_the_end(result_final_sdf, "race_id")

# COMMAND ----------

result_final_sdf = result_final_sdf.select(result_list)

# COMMAND ----------

increment_load_data(result_final_sdf, "f1_presentation", "race_results", "race_id")
