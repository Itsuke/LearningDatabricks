-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1datalakestudy/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;
