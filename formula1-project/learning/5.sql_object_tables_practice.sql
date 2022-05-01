-- Databricks notebook source
-- MAGIC %md 
-- MAGIC **Objectives**
-- MAGIC 1. See Spark SQL documentation
-- MAGIC 2. Create database 
-- MAGIC 3. Data tab in the Databricks UI 
-- MAGIC 4. SHOW command
-- MAGIC 5. DESCRIBE command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE()

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Learning Objectives**
-- MAGIC 1. Create managed table using Python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_sdf = spark.read.parquet(f"{presentation_catalog_path}/race_results")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC race_results_sdf.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

DESC race_results_python

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS demo.race_results_sql
AS
SELECT *
  FROM demo.race_results_python
  WHERE race_year = 2020;

-- COMMAND ----------

DESC EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC **Learning Objectives**
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping a external table

-- COMMAND ----------

-- MAGIC %py
-- MAGIC race_results_sdf.write.format("parquet").option("path", f"{presentation_catalog_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESC EXTENDED demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
grid INT,
fastest_lap_time STRING,
time STRING,
laps INT,
points INT,
position INT,
race_year INT,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number int,
driver_nationality string,
constructors_name string,
created_date timestamp
)
USING parquet 
LOCATION "dbfs:/mnt/formula1datalakestudy/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

SELECT COUNT(1) FROM demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql;

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------


