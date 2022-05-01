-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tv_driver_standings_2018
AS
SELECT race_year, driver_name, constructors_name, total_points, rank
  FROM driver_standings
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM tv_driver_standings_2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW tv_driver_standings_2020
AS
SELECT race_year, driver_name, constructors_name, total_points, rank
  FROM driver_standings
WHERE race_year = 2020

-- COMMAND ----------

SELECT * FROM tv_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Inner Join

-- COMMAND ----------

SELECT *
  FROM tv_driver_standings_2018 d_2018
  JOIN tv_driver_standings_2020 d_2020
    ON (d_2018.driver_name == d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Left Join

-- COMMAND ----------

SELECT *
  FROM tv_driver_standings_2018 d_2018
  LEFT JOIN tv_driver_standings_2020 d_2020
    ON (d_2018.driver_name == d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Right Join

-- COMMAND ----------

SELECT *
  FROM tv_driver_standings_2018 d_2018
  RIGHT JOIN tv_driver_standings_2020 d_2020
    ON (d_2018.driver_name == d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Full Join

-- COMMAND ----------

SELECT *
  FROM tv_driver_standings_2018 d_2018
  FULL JOIN tv_driver_standings_2020 d_2020
    ON (d_2018.driver_name == d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Semi Join

-- COMMAND ----------

SELECT *
  FROM tv_driver_standings_2018 d_2018
  SEMI JOIN tv_driver_standings_2020 d_2020
    ON (d_2018.driver_name == d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Anti Join

-- COMMAND ----------

SELECT *
  FROM tv_driver_standings_2018 d_2018
  ANTI JOIN tv_driver_standings_2020 d_2020
    ON (d_2018.driver_name == d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Cross Join

-- COMMAND ----------

SELECT *
  FROM tv_driver_standings_2018 d_2018
  CROSS JOIN tv_driver_standings_2020 d_2020

-- COMMAND ----------


