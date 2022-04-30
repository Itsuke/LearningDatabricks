# Databricks notebook source
# MAGIC %md Databricks utilities

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls("/")

# COMMAND ----------

for catalog in dbutils.fs.ls("/"):
    print(catalog)
    print(type(catalog))

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

path = "./child notebook"
timeout = 10
dbutils.notebook.run(path, timeout, {"input_param": "Called from Databricks utilities"})

# COMMAND ----------

# Instead of using the dbutils it's recommended to use %pip commands.
# %pip instal pandas
dbutils.library.help()

# COMMAND ----------

# MAGIC %pip install pymcdm

# COMMAND ----------

print("nothing")

# COMMAND ----------


