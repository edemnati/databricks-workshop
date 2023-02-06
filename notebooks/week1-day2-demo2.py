# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Demo - Connect to Azure Data Lake Storage Gen2
# MAGIC 
# MAGIC __Steps__
# MAGIC 
# MAGIC   _Azure Portal_
# MAGIC 
# MAGIC     1. Create Key Vault
# MAGIC     1. Create Azure storage (data lake gen 2)
# MAGIC     1. Create a container
# MAGIC     1. Add a file to container
# MAGIC     1. Copy Azure storage key 1
# MAGIC     1. Create Key Vault secret to store Azure storage key 1
# MAGIC   
# MAGIC   _Datarbicks UI_
# MAGIC 
# MAGIC     1. Create databricks scret scope
# MAGIC     1. Connect to Azure Storage using Python
# MAGIC     1. Create mount

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Access to Azure blob storage Gen2
# MAGIC 
# MAGIC ### Mount point

# COMMAND ----------

# DBTITLE 1,Mount point using credential passthrough
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}


# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://ez-filesystem@ezmylake.dfs.core.windows.net/",
  mount_point = "/mnt/my_mount_pass",
  extra_configs = configs)


# COMMAND ----------

spark.read.format("parquet").load("/mnt/my_mount_pass/NYCTripSmall.parquet").display()

# COMMAND ----------

dbutils.fs.unmount("/mnt/my_lake")

# COMMAND ----------

f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"

# COMMAND ----------

# DBTITLE 1,Mount point using Account token
container_name = "<your-container-name>" 
storage_account_name = "<your-storage-account-name>"
mount_name = "<your-mount-name>" 
conf_key ="fs.azure.account.key.<your-storage-account-name>.blob.core.windows.net"
scope_name = "<your-scope-name>" 
key_name = "<your-key-name>" 
file_name = "<your-file-name>"

container_name = "ez-filesystem"
storage_account_name = "ezmylake"
mount_name ="my_lake"
conf_key = f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net"
scope_name = "my-db-secret" 
key_name = "ezmylake-key"
file_name = "NYCTripSmall.parquet"



dbutils.fs.mount(
                    source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
                    mount_point = f"/mnt/{mount_name}",
                    extra_configs = {conf_key:dbutils.secrets.get(scope = scope_name, key = key_name)}
                 )

#df = spark.read.text("/mnt/<mount-name>/<file-name>")
dbutils.fs.ls(f"/mnt/{mount_name}/")
#df.show()

# COMMAND ----------

spark.read.format("parquet").load("/mnt/my_lake/NYCTripSmall.parquet").display()

# COMMAND ----------

# DBTITLE 1,SQL Query using mount point
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS test_db.NYCTripSmall2;
# MAGIC 
# MAGIC COPY INTO test_db.NYCTripSmall
# MAGIC FROM '/mnt/my_lake/NYCTripSmall.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

#display(spark.sql("SELECT * FROM parquet.`abfss://ez-filesystem@ezmylake.dfs.core.windows.net/NYCTripSmall.parquet`"))
display(spark.sql("SELECT * FROM parquet.`/mnt/my_lake/NYCTripSmall.parquet`"))

# COMMAND ----------

"""
dbutils.fs.ls("abfss://container@storageAccount.dfs.core.windows.net/external-location/path/to/data")

spark.read.format("parquet").load("abfss://container@storageAccount.dfs.core.windows.net/external-location/path/to/data")

spark.sql("SELECT * FROM parquet.`abfss://container@storageAccount.dfs.core.windows.net/external-location/path/to/data`")
"""

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC __Access azure storage Gen2 using service principal:__
# MAGIC 
# MAGIC 1. Create Service Principal: https://learn.microsoft.com/en-us/azure/databricks/security/aad-storage-service-principal
# MAGIC 1. Assign role "Storage Blob Data Contributor" on azure storage to service principal

# COMMAND ----------

dbutils.fs.unmount("/mnt/test_mnt_sp")

# COMMAND ----------

# DBTITLE 1,Mount point using Service principal
service_credential = dbutils.secrets.get(scope="my-db-secret" ,key="ez-test-db-sp-secret-key")

storage_account = "ezmylake"
application_id="5922fe8f-4a22-48f7-92cf-c72752041bec"
tenant_id="16b3c013-d300-468d-ac64-7eda0820b6d3"

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": f"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": application_id,
       "fs.azure.account.oauth2.client.secret": service_credential,
       "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://ez-filesystem@ezmylake.dfs.core.windows.net/",
mount_point = "/mnt/test_mnt_sp",
extra_configs = configs)

spark.read.format("parquet").load("/mnt/test_mnt_sp/NYCTripSmall.parquet").display()

# COMMAND ----------

service_credential = dbutils.secrets.get(scope="my-db-secret" ,key="ez-test-db-sp-secret-key")

storage_account = "ezmylake"
application_id="5922fe8f-4a22-48f7-92cf-c72752041bec"
tenant_id="16b3c013-d300-468d-ac64-7eda0820b6d3"

spark.conf.set(F"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(F"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(F"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(F"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", service_credential)
spark.conf.set(F"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

spark.read.format("parquet").load("abfss://ez-filesystem@ezmylake.dfs.core.windows.net/NYCTripSmall.parquet").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC __Create external table using Databricks SQL__
# MAGIC 
# MAGIC _Run this query from Databricks SQL after configuring service principal to azure storage_
# MAGIC 
# MAGIC ```
# MAGIC CREATE TABLE IF NOT EXISTS test_db.NYCTripSmall ;
# MAGIC 
# MAGIC COPY INTO test_db.NYCTripSmall
# MAGIC FROM 'abfss://ez-filesystem@ezmylake.dfs.core.windows.net/NYCTripSmall.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM test_db.NYCTripSmall3
# MAGIC limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS test_db.new_extenal_table2
# MAGIC LOCATION 'abfss://ez-filesystem@ezmylake.dfs.core.windows.net/new_extenal_table2.parquet';
# MAGIC 
# MAGIC COPY INTO test_db.new_extenal_table2
# MAGIC FROM 'abfss://ez-filesystem@ezmylake.dfs.core.windows.net/NYCTripSmall.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS test_db.new_extenal_table2_copy ;
# MAGIC 
# MAGIC COPY INTO test_db.new_extenal_table2_copy
# MAGIC FROM 'abfss://ez-filesystem@ezmylake.dfs.core.windows.net/new_extenal_table2.parquet'
# MAGIC FILEFORMAT = PARQUET
# MAGIC COPY_OPTIONS ('mergeSchema' = 'true');

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC __Access azure storage Gen2 using SAS Token__

# COMMAND ----------

#Using SAS token
storage_account = "ezmylake"
sas_token=dbutils.secrets.get(scope="my-db-secret" ,key="ez-db-sas-token")
spark.conf.set("fs.azure.account.auth.type.storage_account.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storage_account.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storage_account.dfs.core.windows.net",sas_token)

# COMMAND ----------

spark.read.format("parquet").load("abfss://ez-filesystem@ezmylake.dfs.core.windows.net/NYCTripSmall.parquet").display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Key Vault configuration

# COMMAND ----------

# Key vault config
"""
Go to https://adb-6124167456231216.16.azuredatabricks.net/#secrets/createScope

#SETUP:
    DNS_name: https://<YOUR_KV>.vault.azure.net/
    Resource ID: /subscriptions/<YOUR-SUBSCRIPTION-ID>/resourcegroups/<YOUR-RG>/providers/Microsoft.KeyVault/vaults/db-test-vault

#List secret scopes
databricks secrets list-scopes

#Delete a secret scope
databricks secrets delete-scope --scope <scope-name>
"""


# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

#dbutils.secrets.listScopes()
dbutils.secrets.list('my-db-secret')

# COMMAND ----------

x=dbutils.secrets.get(scope = 'my-db-secret', key = 'ezmylake-key')

#for i in x:
#    print(i)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## dbutils

# COMMAND ----------


dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

#Data
dbutils.data.summarize("<dataframe_name>")

#files
dbutils.fs.ls("<path>")

#Run notebook
dbutils.notebook.run("<notebook_path>")


#--------------------
#Secrets
dbutils.secrets.listScopes()
dbutils.secrets.get("<scope_name>","<key_name>")
dbutils.secrets.list("<scope_name>")

#--------------------
#Widgets
#Set
dbutils.widgetscombobox() #PARAMETERS: name: String, defaultValue: String, choices: Seq, label: String)
dbutils.widgetsdropdown() #PARAMETERS: name: String, defaultValue: String, choices: Seq, label: String)
dbutils.widgetstext() #PARAMETERS: name: String, defaultValue: String, label: String)
dbutils.widgetsmultiselect() #PARAMETERS: name: String, defaultValue: String, choices: Seq, label: String)
#Get
dbutils.widgets.get("<widgets_name>")
#Delete
dbutils.widgets.remove("<widgets_name>")
dbutils.widgets.removeAll()


# COMMAND ----------

dbutils.widgets.text(name="storage_account",defaultValue="value",label="storage_account")

