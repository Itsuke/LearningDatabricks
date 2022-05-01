# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Aggregation Practice

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built-in aggregation funtions

# COMMAND ----------

race_results_sdf = spark.read.parquet(f"{presentation_catalog_path}/race_results")\
 .where("race_year = 2020")

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

race_results_sdf.select(count("*")).show()

# COMMAND ----------

race_results_sdf.select(count("race_name")).show()

# COMMAND ----------

race_results_sdf.select(countDistinct("race_name")).show()

# COMMAND ----------

race_results_sdf.select(sum("points")).show()

# COMMAND ----------

race_results_sdf.filter(race_results_sdf.driver_name == "Max Verstappen").select(sum("points")).show()

# COMMAND ----------

race_results_sdf.filter(race_results_sdf.driver_name == "Max Verstappen").select(sum("points"), countDistinct("race_name")) \
.withColumnRenamed("sum(points)", "Total points of Max") \
.withColumnRenamed("count(DISTINCT race_name)", "Number of races") \
.show()

# COMMAND ----------

race_results_sdf \
    .groupBy("driver_name") \
    .sum("points") \
    .orderBy("sum(points)", ascending=False) \
    .show()

# COMMAND ----------

race_results_sdf \
    .groupBy("driver_name") \
    .agg(sum("points").alias("sum_points"), countDistinct("race_name").alias("number_of_races")) \
    .orderBy("sum_points", ascending=False) \
    .show()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Window functions 

# COMMAND ----------

demo_sdf = spark.read.parquet(f"{presentation_catalog_path}/race_results")\
    .filter("race_year in (2019, 2020)")

# COMMAND ----------

demo_grouped_sdf = demo_sdf \
    .groupBy("race_year", "driver_name") \
    .agg(sum("points").alias("sum_points"), countDistinct("race_name").alias("number_of_races")) \
    .orderBy("sum_points", ascending=False)

# COMMAND ----------

demo_grouped_sdf.show()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("sum_points"))

# COMMAND ----------

demo_grouped_sdf = demo_grouped_sdf.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(demo_grouped_sdf)

# COMMAND ----------


