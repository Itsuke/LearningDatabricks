# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_sdf = spark.read.parquet(f"{processed_catalog_path}/races")

# COMMAND ----------

display(races_sdf)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year > 2010").collect()
display(races_filtered_df)

# COMMAND ----------

print(races_filtered_df)

# COMMAND ----------

import pprint

# COMMAND ----------

pprint.pprint(races_filtered_df)

# COMMAND ----------

races_filtered_df = races_df.filter(races_df["race_year"] == 2010)

# COMMAND ----------

print(type(races_filtered_df))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------

races_filtered_df = races_df.where((races_df["race_year"] == 2010) & (races_df["round"] <= 5)) 

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------


