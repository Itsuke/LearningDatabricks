-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Simple Functions**

-- COMMAND ----------

USE f1_processed;

-- COMMAND ----------

SELECT *, CONCAT(driver_ref, '-', code) AS new_driver_ref
  FROM drivers

-- COMMAND ----------

SELECT *, SPLIT(name, ' ')[0] forename, SPLIT(name, ' ')[1] surname
  FROM drivers

-- COMMAND ----------

SELECT *, current_timestamp
  FROM drivers

-- COMMAND ----------

SELECT name, date_format(dob, "dd-MM-yyyy") AS formated_dob
  FROM drivers

-- COMMAND ----------

SELECT name, date_add(dob, 1) AS dob_plus_1
  FROM drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Aggregate Functions**

-- COMMAND ----------

SELECT COUNT(*)
  FROM drivers;

-- COMMAND ----------

SELECT max(dob)
  FROM drivers;

-- COMMAND ----------

SELECT name 
  FROM drivers
WHERE dob = "2000-05-11";

-- COMMAND ----------

SELECT COUNT(*)
  FROM drivers
WHERE nationality = "Polish";

-- COMMAND ----------

SELECT nationality, COUNT(*)
  FROM drivers
GROUP BY nationality
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality, COUNT(*)
  FROM drivers
GROUP BY nationality
HAVING COUNT(*) > 100
ORDER BY nationality;

-- COMMAND ----------

SELECT min(col) FROM VALUES (10), (-1), (20) AS tab(col);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Window Functions**

-- COMMAND ----------

SELECT nationality, name, dob, RANK() OVER(PARTITION BY nationality ORDER BY dob DESC) AS age_rank
  FROM drivers
ORDER BY nationality, age_rank;

-- COMMAND ----------


