# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest constructors.json file from Ergast Developer DB 

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
# MAGIC ##### Step 1 - Read the json file with Spark dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_sdf = spark.read \
    .schema(constructors_schema) \
    .json(f"{raw_incr_load_catalog_path}/{v_file_date}/constructors.json")

# COMMAND ----------

display(constructors_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Drop the unwanted columns 

# COMMAND ----------

constructors_dropped_sdf = constructors_sdf.drop(constructors_sdf["url"])

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 3 - Rename and add new columns
# MAGIC 1. Rename constructorId constructorRef
# MAGIC 2. Add data_source from input param
# MAGIC 3. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_final_sdf = constructors_dropped_sdf \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

constructors_final_sdf = add_ingestion_date(constructors_final_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write ouput to the processed container in parquet fromat

# COMMAND ----------

if save_as_table:
    constructors_final_sdf.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")
else:
    constructors_final_sdf.write.parquet(f"{processed_catalog_path}/constructors", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("succes")
