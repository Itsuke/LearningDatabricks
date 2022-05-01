# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_sdf = spark.read.parquet(f"{processed_catalog_path}/circuits") \
    .filter("circuit_id < 70") \
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

races_sdf = spark.read.parquet(f"{processed_catalog_path}/races") \
    .filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")

# COMMAND ----------

display(circuits_sdf)

# COMMAND ----------

display(races_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **Inner join**

# COMMAND ----------

combined_sdf = circuits_sdf.join(races_sdf, circuits_sdf.circuit_id == races_sdf.circuit_id, "inner")

# COMMAND ----------

display(combined_sdf)

# COMMAND ----------

display(combined_sdf.select(combined_sdf.race_name))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **Left join**

# COMMAND ----------

combined_left_sdf = circuits_sdf.join(races_sdf, circuits_sdf.circuit_id == races_sdf.circuit_id, "left")

# COMMAND ----------

display(combined_left_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **Right join**

# COMMAND ----------

combined_right_sdf = circuits_sdf.join(races_sdf, circuits_sdf.circuit_id == races_sdf.circuit_id, "right")

# COMMAND ----------

display(combined_right_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **Outer join**

# COMMAND ----------

combined_outer_sdf = circuits_sdf.join(races_sdf, circuits_sdf.circuit_id == races_sdf.circuit_id, "outer")

# COMMAND ----------

display(combined_outer_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **Semi join**
# MAGIC Note: It's like a inner join, but we cannot select columns from the right dataframe

# COMMAND ----------

combined_semi_sdf = circuits_sdf.join(races_sdf, circuits_sdf.circuit_id == races_sdf.circuit_id, "semi") \
    .select(circuits_sdf.circuit_id, circuits_sdf.circuit_name, circuits_sdf.location)

# COMMAND ----------

display(combined_semi_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **Anti join**
# MAGIC Note: Returns the opposite of Semi join

# COMMAND ----------

combined_anti_sdf = circuits_sdf.join(races_sdf, circuits_sdf.circuit_id == races_sdf.circuit_id, "anti") \
    .select(circuits_sdf.circuit_id, circuits_sdf.circuit_name, circuits_sdf.location)

# COMMAND ----------

display(combined_anti_sdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### **Cross join**

# COMMAND ----------

combined_cross_sdf = circuits_sdf.crossJoin(races_sdf)

# COMMAND ----------

display(combined_cross_sdf)

# COMMAND ----------

combined_cross_sdf.count() # it's a number of circuits_sdf.count() * races_sdf.count()

# COMMAND ----------


