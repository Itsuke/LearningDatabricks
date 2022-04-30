# Databricks notebook source
dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("input_param", "this is default string", "Send the imput parameter")

# COMMAND ----------

input_param = dbutils.widgets.get("input_param")

# COMMAND ----------

print(input_param)

# COMMAND ----------

print("Hi, this is Patric!")

# COMMAND ----------

dbutils.notebook.exit("success") #it can return integers as well, they will be casted to string 
