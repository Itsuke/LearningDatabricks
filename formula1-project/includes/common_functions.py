# Databricks notebook source
from pyspark.sql.functions import current_timestamp
def add_ingestion_date(input_sdf):
    output_sdf = input_sdf.withColumn("ingestion_date", current_timestamp())
    return output_sdf

# COMMAND ----------

def reorder_columns_with_partition_param_at_the_end(data_sdf, partition_param_name):
    if partition_param_name in data_sdf.schema.names:
        names_list = data_sdf.schema.names
        names_list.remove(partition_param_name)
        names_list.append(partition_param_name)

        return names_list
    else:
        raise ValueError("The given partition param name is not included in given dataframe")

# COMMAND ----------

def increment_load_data(sdf, catalog_name, table_name, partition_param_name):
# Dynamic overwrite mode allows to replace only the partitions it recieved. The static would overwrite the whole parquet
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    if (spark._jsparkSession.catalog().tableExists(f"{catalog_name}.{table_name}")): # update table if it exists
        sdf.write.mode("overwrite").insertInto(f"{catalog_name}.{table_name}")
    else:
        if save_as_table:
            sdf.write.mode("append").format("parquet").partitionBy(f"{partition_param_name}").saveAsTable(f"{catalog_name}.{table_name}")
        else:
            sdf.write.parquet(f"{processed_catalog_path}/{table_name}", mode="overwrite", partitionBy=f"{partition_param_name}")

# COMMAND ----------

def df_column_to_list(input_df, column_name):
    df_row_list = input_df.select(column_name) \
                          .distinct() \
                          .collect()
    column_value_list = [row[column_name] for row in df_row_list]
    return column_value_list

# COMMAND ----------


