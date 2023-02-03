# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Azure Eventhub integration with Databricks
# MAGIC 
# MAGIC __Preparation__
# MAGIC 
# MAGIC     1. Create Event Hubs Namespace
# MAGIC     1. create Event Hubs Instance
# MAGIC         1. Add SAS Policy to generate a connectionString
# MAGIC         1. Create a consumer group
# MAGIC     1. Create a secret in Azure Key Vault with Event Hubs Instance connectionString
# MAGIC     1. Databricks cluster: Install maven library: com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22
# MAGIC 
# MAGIC __Receive messages to event hub__
# MAGIC 
# MAGIC     1. Create a read stream to Event Hub Instance
# MAGIC     1. Store messages to storage location
# MAGIC     1. Start receiver
# MAGIC 
# MAGIC __Send messages to event hub__
# MAGIC 
# MAGIC     1. Create an event generator
# MAGIC     1. Write messages to Event Hub Instance
# MAGIC     1. Start sender

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Receive messages to event hub

# COMMAND ----------

# DBTITLE 1,Preparation
# MAGIC %md

# COMMAND ----------

# DBTITLE 1,Receive messages to event hub
#Ref: https://github.com/Azure/azure-event-hubs-spark/blob/master/docs/PySpark/structured-streaming-pyspark.md
#REf2: https://techcommunity.microsoft.com/t5/analytics-on-azure-blog/ingest-azure-event-hub-telemetry-data-with-apache-pyspark/ba-p/3440394#:~:text=1%20An%20Azure%20Event%20Hub%20service%20must%20be,Maven%20repository%20in%20the%20provisioned%20Databricks%20cluster.%20



import pyspark.sql.functions as f
import json
# Initialize event hub config dictionary with connectionString
connectionString = 'Endpoint=sb://ez-event-hub-mip.servicebus.windows.net/;SharedAccessKeyName=eztesteventhubsas;SharedAccessKey=9R2RVrw+1BGNTjXqonRE88hx95riYY/ZkBM4HaTZeW0=;EntityPath=ez-test-eventhub'
consumer_group = 'test-eventhub-capture-cg' #'$Default'
eventhub_instance_name = 'ez-test-eventhub'

ehConf = {}
ehConf['eventhubs.connectionString'] = connectionString
# Add consumer group to the ehConf dictionary
ehConf['eventhubs.consumerGroup'] = consumer_group

# Create the positions
# Start from beginning of stream
startOffset = "-1"
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
#ehConf['eventhubs.startingPosition'] = json.dumps(startingEventPosition)

# Encrypt ehConf connectionString property
ehConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

# Read stream Data from Event Hub
df_readStream = (spark
                 .readStream
                 .format("eventhubs")
                 .options(**ehConf)
                ).load()

# COMMAND ----------

# DBTITLE 1,Display streaming events
# Display streaming events
(df_readStream
 .select(f.decode(f.col("body").cast("string"), 'UTF-8').alias("Payload"),"*") 
).display()

# COMMAND ----------

# MAGIC %pip install dlt

# COMMAND ----------

# DBTITLE 1,Read stream messages and save to storage location
# Read stream messages and save to storage location

# Initialize event hub config dictionary with connectionString
connectionString = 'Endpoint=sb://ez-event-hub-mip.servicebus.windows.net/;SharedAccessKeyName=eztesteventhubsas;SharedAccessKey=9R2RVrw+1BGNTjXqonRE88hx95riYY/ZkBM4HaTZeW0=;EntityPath=ez-test-eventhub'
consumer_group = 'test-eventhub-capture-cg' #'$Default'
eventhub_instance_name = 'ez-test-eventhub'

ehConf_write = {}
ehConf_write['eventhubs.connectionString'] = connectionString
# Add consumer group to the ehConf dictionary
ehConf_write['eventhubs.consumerGroup'] = consumer_group

# Create the positions
# Start from beginning of stream
startOffset = "4040"
startingEventPosition = {
  "offset": startOffset,  
  "seqNo": -1,            #not in use
  "enqueuedTime": None,   #not in use
  "isInclusive": True
}
#ehConf_write['eventhubs.startingPosition'] = json.dumps(startingEventPosition)

# Encrypt ehConf connectionString property
ehConf_write['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)

(df_readStream
          .select(f.decode(f.col("body").cast("string"), 'UTF-8').alias("Payload"),"*")
          .writeStream
          .format("json")
          .outputMode("append")
          .options(**ehConf_write)
          .option("checkpointLocation", "dbfs:/FileStore/test_stream/checkpointapievents5")
          .start("dbfs:/FileStore/test_stream/writedata5")
         )

# COMMAND ----------

df_read=spark.read.json("dbfs:/FileStore/test_stream/writedata5/")
display(df_read)


# COMMAND ----------

# DBTITLE 1,Create table 
# MAGIC %sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS test_db.test_events_bronze
# MAGIC USING json LOCATION 'dbfs:/FileStore/test_stream/writedata5';
# MAGIC 
# MAGIC --select * from test_db.test_events_bronze;
# MAGIC 
# MAGIC select Payload:data,* from test_db.test_events_bronze;

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC # Send messages to event hub

# COMMAND ----------

# DBTITLE 0,Send messages to event hub
# Set up the Event Hub config dictionary with default settings
writeConnectionString = 'Endpoint=sb://ez-event-hub-mip.servicebus.windows.net/;SharedAccessKeyName=eztesteventhubsas;SharedAccessKey=9R2RVrw+1BGNTjXqonRE88hx95riYY/ZkBM4HaTZeW0=;EntityPath=ez-test-eventhub'
consumer_group = 'test-eventhub-capture-cg' #'$Default'


ehWriteConf = {}
ehWriteConf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(writeConnectionString)
ehConf['eventhubs.consumerGroup'] = consumer_group

# Write body data from a DataFrame to EventHubs. Events are distributed across partitions using round-robin model.

import time
import json
import pandas as pd
import pyspark.sql.types as T 

myschema = T.StructType([T.StructField('body', T.StringType(), False)])
i_max=10
for i in range(i_max):
    test_event = [{"body":json.dumps({"data":f"test write to event data {i}"})}]
    df_test = spark.createDataFrame(test_event,schema=myschema)
    ds = (df_test
          .select(f.col("body"))
          .write 
          .format("eventhubs") 
          .options(**ehWriteConf) 
          ).save()
    time.sleep(3)


