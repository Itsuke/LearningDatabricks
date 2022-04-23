# Databricks notebook source
# MAGIC %md 
# MAGIC ### Ingest races.csv file from Ergast Developer DB 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 1 - Read the csv file with Spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import IntegerType, DateType, StringType, StructType, StructField

# COMMAND ----------

my_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType()),
    StructField("round", IntegerType()),
    StructField("circuitId", IntegerType()),
    StructField("name", StringType()),
    StructField("date", DateType()),
    StructField("time", StringType()),
    StructField("url", StringType()),
])

# COMMAND ----------

races_sdf = spark.read.schema(my_schema).csv("/mnt/formula1datalakestudy/raw/races.csv", header=True)

# COMMAND ----------

display(races_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Select and rename wanted columns

# COMMAND ----------

from pyspark.sql.functions import col, lit, to_timestamp, concat, current_timestamp

# COMMAND ----------

races_select_columns_sdf = races_sdf.select(
    col("raceId").alias("race_id"),
    col("year").alias("race_year"),
    col("round"),
    col("circuitId").alias("circuit_id"),
    col("name"),
    col("date"),
    col("time")
)

# COMMAND ----------

display(races_select_columns_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Concatenate data and time 

# COMMAND ----------

races_concat_columns_sdf = races_select_columns_sdf.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), format="yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(races_concat_columns_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 4 - Drop the date and time columns 

# COMMAND ----------

races_drop_sdf = races_concat_columns_sdf.drop("date", "time")

# COMMAND ----------

display(races_drop_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 6 - Add the ingestion timestamp

# COMMAND ----------

races_final_sdf = races_drop_sdf.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(races_final_sdf)

# COMMAND ----------

races_final_sdf.show()

# COMMAND ----------

races_final_sdf.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Step 7 - Save the ingested dataframe in parquet partioned by race_year collumn

# COMMAND ----------

races_sdf = races_final_sdf.write.parquet("/mnt/formula1datalakestudy/processed/races", mode="overwrite", partitionBy="race_year")

# COMMAND ----------

sdf = spark.read.parquet("dbfs:/mnt/formula1datalakestudy/processed/races")

# COMMAND ----------

display(sdf)
