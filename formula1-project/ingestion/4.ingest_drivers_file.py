# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest drivers.json file from Ergast Developer DB 

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
    .schema(driver_schema) \
    .json("/mnt/formula1datalakestudy/raw/drivers.json")

# COMMAND ----------

display(drivers_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 2 - Modify the columns and add ingestion date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

# COMMAND ----------

drivers_modified_sdf = drivers_sdf \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("name", concat(drivers_sdf.name.forename, lit(" "), drivers_sdf.name.surname)) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(drivers_modified_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns 

# COMMAND ----------

drivers_final_sdf = drivers_modified_sdf.drop(drivers_modified_sdf["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write ouput to the parquet file

# COMMAND ----------

drivers_final_sdf.write.parquet("/mnt/formula1datalakestudy/processed/drivers", mode="overwrite")
