# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

files = ["1.ingest_circuits_file",
         "2.ingest_races_file",
         "3.ingest_constructors_file",
         "4.ingest_drivers_file",
         "5.ingest_results_file",
         "6.ingest_pit_stops_file",
         "7.ingest_lap_times_folder",
         "8.ingest_qualifying_folder"]

# COMMAND ----------

for file in files:
    print(file + " - " + dbutils.notebook.run(file, 0, {"p_data_source":"Ergast API"}))

# COMMAND ----------

# MAGIC %md To run notebooks concurrently see the https://docs.microsoft.com/en-us/azure/databricks/notebooks/notebook-workflows#run-multiple-notebooks-concurrently
