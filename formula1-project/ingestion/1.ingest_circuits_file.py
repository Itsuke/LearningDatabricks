# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest circuits.csv file from Ergast Developer DB 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the csv file with Spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType, StringType, StructType, StructField

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), nullable=False),
                                     StructField("circuitRef", StringType()),
                                     StructField("name", StringType()),
                                     StructField("location", StringType()),
                                     StructField("country", StringType()),
                                     StructField("lat", DoubleType()),
                                     StructField("lng", DoubleType()),
                                     StructField("alt", IntegerType()),
                                     StructField("url", StringType())
])

# COMMAND ----------

circuits_with_schema_sdf = spark.read.schema(circuits_schema).csv("dbfs:/mnt/formula1datalakestudy/raw/circuits.csv", header=True)

# COMMAND ----------

display(circuits_with_schema_sdf)

# COMMAND ----------

circuits_with_schema_sdf.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Get the dataframe without the "url" column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_sdf = circuits_with_schema_sdf.select(col("circuitId"), 
                                                        col("circuitRef"), 
                                                        col("name"), 
                                                        col("location"), 
                                                        col("country"), 
                                                        col("lat"), 
                                                        col("lng"), 
                                                        col("alt"))

# COMMAND ----------

display(circuits_selected_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Rename the columns withColumnRenamed

# COMMAND ----------

circuits_renamed_sdf = circuits_selected_sdf \
    .withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

display(circuits_renamed_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Add ingestion date to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_sdf = circuits_renamed_sdf.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(circuits_final_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 5 - write dataframe to datalake as parquet

# COMMAND ----------

circuits_final_sdf.write.parquet("dbfs:/mnt/formula1datalakestudy/processed/circuits", mode="overwrite")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls dbfs:/mnt/formula1datalakestudy/processed/circuits

# COMMAND ----------

sdf = spark.read.parquet("dbfs:/mnt/formula1datalakestudy/processed/circuits")

# COMMAND ----------

display(sdf)

# COMMAND ----------

sdf.printSchema()

# COMMAND ----------


