# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest constructors.json file from Ergast Developer DB 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the json file with Spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_sdf = spark.read \
    .schema(constructors_schema) \
    .json("/mnt/formula1datalakestudy/raw/constructors.json")

# COMMAND ----------

display(constructors_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop the unwanted columns 

# COMMAND ----------

constructors_dropped_sdf = constructors_sdf.drop(constructors_sdf["url"])

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Rename the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

constructors_final_sdf = constructors_dropped_sdf \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write ouput to the parquet file

# COMMAND ----------

constructors_final_sdf.write.parquet("/mnt/formula1datalakestudy/processed/constructors", mode="overwrite")

# COMMAND ----------


