# Databricks notebook source
# MAGIC %md 
# MAGIC #### Produce driver standings as on https://www.bbc.com/sport/formula1/drivers-world-championship/standings

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the data

# COMMAND ----------

race_results_sdf = spark.read.parquet(f"{presentation_catalog_path}/race_results") \
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

race_year_list = df_column_to_list(race_results_sdf, "race_year")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

race_results_sdf = spark.read.parquet(f"{presentation_catalog_path}/race_results") \
    .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

race_grouped_sdf = race_results_sdf \
    .groupBy("race_year", "driver_name", "driver_nationality", "constructors_name") \
    .agg(sum("points").alias("total_points"), \
         count(when(col("position") == 1, True)).alias("win_count"))


# COMMAND ----------

display(race_grouped_sdf.filter("race_year = 2020"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("win_count"))

# COMMAND ----------

race_final_sdf = race_grouped_sdf.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

race_final_sdf = race_final_sdf.select(reorder_columns_with_partition_param_at_the_end(race_final_sdf, "race_year"))

# COMMAND ----------

increment_load_data(race_final_sdf, "f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC   FROM f1_presentation.driver_standings
# MAGIC  WHERE race_year = 2021;

# COMMAND ----------


