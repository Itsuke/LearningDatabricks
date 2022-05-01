# Databricks notebook source
# MAGIC %md 
# MAGIC ## Access dataframe using SQL
# MAGIC **Objectives**
# MAGIC 1. Create temporary views on dataframe
# MAGIC 2. Access the data from SQL cell
# MAGIC 3. Access the data from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_sdf = spark.read.parquet(f"{presentation_catalog_path}/race_results")


# COMMAND ----------

race_results_sdf.createOrReplaceTempView("tv_race_result")

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM tv_race_result 
# MAGIC WHERE race_year = 2020 
# MAGIC LIMIT 10;

# COMMAND ----------

race_year = 2019

# COMMAND ----------

race_results_2019_sdf = spark.sql(f"SELECT * FROM tv_race_result WHERE race_year = {race_year}")

# COMMAND ----------

display(race_results_2019_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Access dataframe using SQL
# MAGIC **Global temporary views**
# MAGIC 1. Create global temporary views on dataframe
# MAGIC 2. Access the data from SQL cell
# MAGIC 3. Access the data from Python cell
# MAGIC 4. Access the view from another Notebook

# COMMAND ----------

race_results_sdf.createOrReplaceGlobalTempView("gtv_race_result")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES; 

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp; 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM global_temp.gtv_race_result 
# MAGIC WHERE race_year = 2020 
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## Differences between the SQL and Spark.Sql
# MAGIC 1. Spark.Sql returns the dataframe which may be usefull for next operations on df.
# MAGIC 2. Spark.Sql may use Python variables as arguments in query
# MAGIC 
# MAGIC ## Characteristics of temp view
# MAGIC 1. temp view is only valid inside a spark session
# MAGIC 2. temp view cannot be accessed from another notebook
# MAGIC 3. temp view cannot be accessed after the re-atach of cluster
# MAGIC 4. createOrReplace may be used to create a temp view whether it's existing or not 
# MAGIC 
# MAGIC ## Characteristics of global temp view
# MAGIC 1. the view has to be accessed through global_temp
# MAGIC 2. global temp view can be accessed from another notebook
# MAGIC 3. global temp view can be accessed after the re-atach of cluster
# MAGIC 4. the view will be accessible as long as the cluster is up and running
# MAGIC 5. createOrReplace may be used to create a global temp view whether it's existing or not 

# COMMAND ----------


