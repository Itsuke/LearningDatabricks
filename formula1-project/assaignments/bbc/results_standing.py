# Databricks notebook source
# MAGIC %md 
# MAGIC #### Produce driver standings as on https://www.bbc.com/sport/formula1/results

# COMMAND ----------

# MAGIC %run "../../includes/configuration"

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
 .select("race_id", "driver_id", "constructor_id", "grid", "fastest_lap_time", "time", "laps", "points", "position")

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

display(combined_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Step 3 - Drop unwanted columns

# COMMAND ----------

result_dropped_sdf = result_combined_sdf.drop("constructor_id", "driver_id", "circuit_id", "race_id")

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

result_final_sdf.write.parquet(f"{presentation_catalog_path}/race_results", mode="overwrite")

# COMMAND ----------

display(result_final_sdf.filter("race_year = 2020") \
    .where(races_sdf.race_name == "Abu Dhabi Grand Prix") \
    .orderBy(result_final_sdf.points.desc()))

# COMMAND ----------


