# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook introductions 
# MAGIC * UI intro
# MAGIC * Magic commands

# COMMAND ----------

msg = "Hello World"

# COMMAND ----------

print(msg)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   "HELLO"

# COMMAND ----------

# MAGIC %scala
# MAGIC var txt = "Hello"

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls 

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls dbfs:/databricks-datasets/COVID/USAFacts/

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/databricks-datasets/COVID/USAFacts/covid_confirmed_usafacts.csv

# COMMAND ----------

# MAGIC %sh 
# MAGIC ps

# COMMAND ----------

# MAGIC %sh
# MAGIC ps -aux 

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -a

# COMMAND ----------


