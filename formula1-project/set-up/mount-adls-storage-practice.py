# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

for metadata in dbutils.secrets.list("formula1-scope"):
    print(metadata)

# COMMAND ----------

storage_account_name = "formula1datalakestudy"
client_id            = dbutils.secrets.get("formula1-scope", "databricks-app-client-id")
tenant_id            = dbutils.secrets.get("formula1-scope", "databricks-app-client-secret")
client_secret        = dbutils.secrets.get(scope="formula1-scope", key="databricks-app-tenant-id")
print(client_id) # interesting - the print says that value was redacted. 

# COMMAND ----------

# workaround for key secret 
for char in dbutils.secrets.get("formula1-scope", "databricks-app-client-id"):
    print(char, end=" ")
    
print()
for char in dbutils.secrets.get("formula1-scope", "databricks-app-client-id"):
    print(char, end="")

# COMMAND ----------

configs ={
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{client_id}",
    "fs.azure.account.oauth2.client.secret": f"{client_secret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

container_name = "raw"

dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs
)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1datalakestudy/raw")

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1datalakestudy/processed")

# COMMAND ----------


