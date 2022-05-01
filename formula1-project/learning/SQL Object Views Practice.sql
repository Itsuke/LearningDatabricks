-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Views on tables
-- MAGIC **Learning Objectives**
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global View
-- MAGIC 3. Create Pernament View

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tv_race_results
AS
SELECT *
  FROM demo.race_results_python 
  WHERE race_year = 2020

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gtv_race_results
AS
SELECT *
  FROM demo.race_results_python 
  WHERE race_year = 2020

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
  FROM demo.race_results_python 
  WHERE race_year = 2020

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN global_temp;

-- COMMAND ----------

SELECT * FROM global_temp.gtv_race_results;

-- COMMAND ----------


