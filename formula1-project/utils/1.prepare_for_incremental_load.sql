-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ##### Drop the f1_processed and f1_presentation tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_processed CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "dbfs:/mnt/formula1datalakestudy/processed"

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "dbfs:/mnt/formula1datalakestudy/presentation"

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Notes:
-- MAGIC CASCADE is being used to drop tables first before database
