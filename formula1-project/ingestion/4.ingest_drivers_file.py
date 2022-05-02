# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest drivers.json file from Ergast Developer DB 

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

from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField

# COMMAND ----------

name_schema = StructType([
    StructField("forename", StringType()),
    StructField("surname", StringType()),
])

# COMMAND ----------

drivers_schema = StructType([
    StructField("driverId", IntegerType(), False),
    StructField("driverRef", StringType()),
    StructField("number", IntegerType()),
    StructField("code", StringType()),
    StructField("name", name_schema),
    StructField("dob", DateType()),
    StructField("nationality", StringType()),
    StructField("url", StringType())
])

# COMMAND ----------

drivers_sdf = spark.read \
    .schema(drivers_schema) \
    .json(f"{raw_incr_load_catalog_path}/{v_file_date}/drivers.json")

# COMMAND ----------

display(drivers_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Rename and add new columns
# MAGIC 1. Rename driverId, driverRef
# MAGIC 2. Concatenate name.forname and name.surname
# MAGIC 2. Add data_source from input param
# MAGIC 3. Add ingestion_date with current timestamp

# COMMAND ----------

from pyspark.sql.functions import concat, lit

# COMMAND ----------

drivers_modified_sdf = drivers_sdf \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(drivers_sdf.name.forename, lit(" "), drivers_sdf.name.surname)) \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_modified_sdf = add_ingestion_date(drivers_modified_sdf)

# COMMAND ----------

display(drivers_modified_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns 

# COMMAND ----------

drivers_final_sdf = drivers_modified_sdf.drop(drivers_modified_sdf["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4 - Write ouput to the processed container in parquet fromat

# COMMAND ----------

if save_as_table:
    drivers_final_sdf.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")
else:
    drivers_final_sdf.write.parquet(f"{processed_catalog_path}/drivers", mode="overwrite")

# COMMAND ----------

dbutils.notebook.exit("succes")
