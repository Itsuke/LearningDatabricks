# Databricks notebook source
# MAGIC %md 
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_practice
# MAGIC LOCATION '/mnt/formula1datalakestudy/practice'

# COMMAND ----------

results_sdf = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1datalakestudy/raw/incr_load/2021-03-28/results.json")

# COMMAND ----------

results_sdf.write.format("delta").mode("overwrite").saveAsTable("results_managed")

# COMMAND ----------

results_sdf.write.format("delta").mode("overwrite").save("/mnt/formula1datalakestudy/practice/results_external")


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_practice.results_external 
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/formula1datalakestudy/practice/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.results_external

# COMMAND ----------

results_external_df = spark.read.format("delta").load("/mnt/formula1datalakestudy/practice/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

results_sdf.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_practice.results_partitioned")


# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Detla Table
# MAGIC 2. Delete from Delta Table

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_practice.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_practice.results_managed 
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_practice.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC EXTENDED f1_practice.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

# Somehow it says that my results_managed isn't a Delta table. After some study, it's because the managed tables are stored under warehouse path
# deltaTable = DeltaTable.forPath(spark, '/mnt/formula1datalakestudy/practice/results_managed')

# Declare the predicate by using a SQL-formatted string.
# deltaTable.update(
#   condition = "position <= 10",
#   set = { "points": "21 - position" }
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_practice.results_managed 
# MAGIC WHERE position > 10

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_practice.results_managed 

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Upsert using merge

# COMMAND ----------

drivers_day1_sdf = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1datalakestudy/raw/incr_load/2021-03-28/drivers.json") \
    .filter("driverId <= 10") \
    .select("driverId", "dob", "name.forename", "name.surname")
    

# COMMAND ----------

display(drivers_day1_sdf)

# COMMAND ----------

drivers_day1_sdf.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

drivers_day2_sdf = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1datalakestudy/raw/incr_load/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 6 AND 15") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day2_sdf)

# COMMAND ----------

 drivers_day2_sdf.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

drivers_day3_sdf = spark.read \
    .option("inferSchema", True) \
    .json("/mnt/formula1datalakestudy/raw/incr_load/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
    .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE TABLE IF NOT EXISTS f1_practice.driver_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING, 
# MAGIC   surname STRING,
# MAGIC   createdDate DATE, 
# MAGIC   updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.driver_merge

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO f1_practice.driver_merge AS tgt
# MAGIC USING drivers_day1 AS src
# MAGIC ON tgt.driverId = src.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     tgt.dob = src.dob,
# MAGIC     tgt.forename = src.forename,
# MAGIC     tgt.surname = src.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_practice.driver_merge

# COMMAND ----------

# MAGIC %sql 
# MAGIC MERGE INTO f1_practice.driver_merge AS tgt
# MAGIC USING drivers_day2 AS src
# MAGIC ON tgt.driverId = src.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC   UPDATE SET 
# MAGIC     tgt.dob = src.dob,
# MAGIC     tgt.forename = src.forename,
# MAGIC     tgt.surname = src.surname,
# MAGIC     tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createdDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp
# MAGIC   )

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_practice.driver_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/user/hive/warehouse/mnt/formula1datalakestudy/demo/driver_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_sdf.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM f1_practice.driver_merge

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. History & Versioning 
# MAGIC 2. Time Travel 
# MAGIC 3. Vaccum 

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESC HISTORY f1_practice.driver_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.driver_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.driver_merge TIMESTAMP AS OF "2022-05-03T14:59:00.000+0000";

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", "2022-05-03T14:59:00.000+0000").load("/user/hive/warehouse/mnt/formula1datalakestudy/demo/driver_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM removes the data older than 7 days by default
# MAGIC VACUUM f1_practice.driver_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.driver_merge TIMESTAMP AS OF "2022-05-03T14:59:00.000+0000";

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_practice.driver_merge RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.driver_merge;

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Restoring removed data

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_practice.driver_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.driver_merge;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESC HISTORY f1_practice.driver_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_practice.driver_merge AS tgt
# MAGIC USING f1_practice.driver_merge VERSION AS OF 3 AS src
# MAGIC   ON (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESC HISTORY f1_practice.driver_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_practice.driver_merge;

# COMMAND ----------


