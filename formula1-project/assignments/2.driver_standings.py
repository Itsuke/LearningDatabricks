# Databricks notebook source
# MAGIC %md 
# MAGIC #### Produce driver standings as on https://www.bbc.com/sport/formula1/drivers-world-championship/standings

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 - Read the data

# COMMAND ----------

race_results_sdf = spark.read.parquet(f"{presentation_catalog_path}/race_results")

# COMMAND ----------

display(race_results_sdf)

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col

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

display(race_final_sdf.filter("race_year in (2019, 2020)"))

# COMMAND ----------

if save_as_table:
    race_final_sdf.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
else:
    race_final_sdf.write.parquet(f"{presentation_catalog_path}/driver_standings", mode="overwrite")

# COMMAND ----------


