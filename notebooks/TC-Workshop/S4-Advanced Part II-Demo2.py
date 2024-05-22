# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Databricks integration with Azure Synapse Datawarehouse
# MAGIC
# MAGIC __Setup environment__
# MAGIC
# MAGIC       1. Start  Azure Synapse workspace Create sql pool
# MAGIC       1. Create a login and a user 
# MAGIC       1. create master key
# MAGIC
# MAGIC __Demo steps__
# MAGIC
# MAGIC       1. Get some data from an Azure Synapse table
# MAGIC       1. Apply some transformations to the data
# MAGIC       1. Write the data back to another table in Azure Synapse
# MAGIC       
# MAGIC       

# COMMAND ----------

#Read synapse data from Databricks: https://learn.microsoft.com/en-us/azure/databricks/external-data/synapse-analytics
# The Azure Synapse connector does not delete the temporary files that it creates in the Azure storage container. Databricks recommends that you periodically delete temporary files under the user-supplied tempDir location.
#https://learn.microsoft.com/en-us/azure/databricks/external-data/sql-server

container_name = "ez-filesystem"
storage_account_name = "ezmylake"
mount_name ="my_lake"
scope_name = "my-db-secret" 
key_name = "ezmylake-key"
file_name = "NYCTripSmall.parquet"

# Set up the storage account access key in the notebook session conf. 
spark.conf.set( f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", dbutils.secrets.get(scope = scope_name, key = key_name))

# jdbc connection string
jdbc_cs_sql_pwd="jdbc:sqlserver://ez-az-synapse-test.sql.azuresynapse.net:1433;database=mysqlpool;user=test_pool_login_db;password=MyPass1234;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.sql.azuresynapse.net;loginTimeout=30;"



# Get some data from an Azure Synapse table. 
synapse_dedicated_pool_cs = "ez-az-synapse-test.sql.azuresynapse.net"
df = (spark.read 
      .format("com.databricks.spark.sqldw") 
      .option("url", jdbc_cs_sql_pwd) 
      .option("tempDir", f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/temp_synapse_dbx_tc_4") 
      .option("forwardSparkAzureStorageCredentials", "true") 
      .option("dbTable", "dbo.NYCTaxiTripSmall") 
    ).load() 

df.count()


# COMMAND ----------

df.display()

# COMMAND ----------

# Apply some transformations to the data
import pyspark.sql.functions as f
df_agg = (df.groupBy("PaymentType")
            .agg(f.sum("FareAmount").alias("sum_FareAmount"),
                 f.sum("PassengerCount").alias("sum_PassengerCount"),
                 
                )
         )

display(df_agg)

# COMMAND ----------


# Write the data back to another table in Azure Synapse. 
(df_agg.write 
      .format("com.databricks.spark.sqldw") 
      .option("url", jdbc_cs_sql_pwd) 
      .option("forwardSparkAzureStorageCredentials", "true")            
      .option("dbTable", "dbo.NYCTaxiTripSmall_agg_new_tc_3") 
      .mode("overwrite")
      .option("tempDir", f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/temp_synapse_dbx") 
     .save()
)


# COMMAND ----------

df_agg.createOrReplaceTempView("df_agg")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from df_agg where PaymentType = 'DIS'

# COMMAND ----------


