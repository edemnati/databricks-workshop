# Databricks notebook source
# MAGIC %md
# MAGIC # Data Exploration
# MAGIC 
# MAGIC __Steps:__
# MAGIC Steps:
# MAGIC 1. Get data:
# MAGIC 	• Data source: Toronto Open data Portal - Festivals & Events:  https://open.toronto.ca/dataset/festivals-events/
# MAGIC 2. Explore data structure
# MAGIC 	1. Count number of rows
# MAGIC 	2. Display dataframe schema
# MAGIC 3. Data transformation
# MAGIC 	1. Flatten dataframe structure
# MAGIC 	2. Keep rows: eventName, category.name, shortDescription, startDate, endDate, locationName, freeEvent, frequency, cost, dates.allDay, dates.startDateTime
# MAGIC 	3. Count number of rows
# MAGIC 4. Data profiling
# MAGIC 5. Data analysis
# MAGIC 	1. Count percentage of free events 
# MAGIC 	2. Count percentage of free events per category
# MAGIC 	3. Count events per category
# MAGIC 	4. Count events per location
# MAGIC 6. Enhance data
# MAGIC 	1. Add day of week 
# MAGIC 	2. Add day of year
# MAGIC 	3. For events that have multiple dates, we want to assign an incremental number to each date 
# MAGIC 	for example, if an event has 5 dates, the earliest date will be assigned number 1 and last date will be assigned number 5
# MAGIC Save transformed data

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Explore Dataset

# COMMAND ----------

import requests

#Read data
URL="https://secure.toronto.ca/cc_sr_v1/data/edc_eventcal_APR?limit=500"

result = requests.get(URL).json()

len(result)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Read data as spark dataFrame

# COMMAND ----------

# Read data with spark
from pyspark import SparkFiles

#Show files within directory:
def get_all_files_paths():
     for i in dbutils.fs.ls(f"file://{SparkFiles.getRootDirectory()}"):
        print(i.path)


URL="https://secure.toronto.ca/cc_sr_v1/data/edc_eventcal_APR?limit=500"
spark.sparkContext.addFile(URL)
print(get_all_files_paths())


# COMMAND ----------

# DBTITLE 1,Move file to dbfs
# MAGIC %sh
# MAGIC mv /local_disk0/spark-23426fb2-714b-40c8-ba99-f611b131b583/userFiles-ab02e600-186f-4db9-94ab-5684575feb1a/edc_eventcal_APR /dbfs/FileStore/datasets/

# COMMAND ----------

"""
write code to read the json file that you save to dbfs folder
1. Count number of rows
2. Display dataframe schema
"""


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check data for Raptors events

# COMMAND ----------

"""
Write code to keep only evvents that are related to Toronto Raptors
display result
"""


# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Flatten Dataset

# COMMAND ----------

import pyspark.sql.functions as f 
import pyspark.sql.types as T

"""
Data transformation
1. Flatten dataframe structure
2. Keep columns:
    eventName,category.name,shortDescription,startDate,endDate,locationName,
    freeEvent,frequency,cost,dates.allDay,dates.endDateTime,dates.startDateTime

3. Count number of rows

"""
       

# COMMAND ----------

"""
Write code to keep only events that are related to Toronto Raptors
display result
"""


# COMMAND ----------

"""
Create a temporary view using DataFrame function createOrReplaceTempView()
"""


# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to count number of events that are free or not
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to count number of events per category
# MAGIC Add a column that show the percentage of free events per category
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to show events that are schedules only once
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC What is the percentage of free events?
# MAGIC Write SQL to:
# MAGIC   - Count number of rows
# MAGIC   - Calculate the percentage of free events
# MAGIC */

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to count number of events per location
# MAGIC */

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enhance Data

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

"""
Use the flatten dataframe that you created previously to :
    1. Drop duplicates
    2. Add day of week
    3. Add day of year
    4. Add a new column event_date_id as: for each event, assign an incremental number to each date for example, if an event has 5 dates, the earliest date will be assigned number 1 and last date will be assigned number 5 Save transformed data 
"""



# COMMAND ----------

"""
Write code to keep only events that are related to Toronto Raptors
display result
"""

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Create a new database test_db if it does not exist
# MAGIC */
# MAGIC CREATE DATABASE IF NOT EXISTS test_db;

# COMMAND ----------

# DBTITLE 1,Save to Table
"""
Save DataFrame to a new table in database test_db
"""
df_flatten2.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("test_db.toronto_events_transformed")

# COMMAND ----------

# DBTITLE 1,Save to Delta file
"""
Save DataFrame to dbfs location as delta format
"""
df_flatten2.write.mode("overwrite").format("delta").save("dbfs:/FileStore/datasets/toronto_events_transformed2")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe extended test_db.toronto_events_transformed

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC describe extended test_db.toronto_events_transformed2

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Merge data

# COMMAND ----------

#Create new DataFrame to test merge statement
df_new_data = (df_flatten2
                   .where("eventName like '%Raptor%'")
                   .withColumn('startDate',f.date_add("startDate",365))
                   .withColumn('endDate',f.date_add("endDate",365))
                   .withColumn('startDateTime',
                               f.concat(f.date_add(f.substring(f.col('startDateTime'),0,10),365),
                                        f.substring(f.col('startDateTime'),10,24)                                        
                                       )
                              )
                   .withColumn('event_description',f.concat(f.col("event_description"),f.lit(" New name")))
              )


df_new_data.createOrReplaceTempView("df_new_data")
display(df_new_data)


# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write sql command to merge the new DataFrame to the saved table
# MAGIC */
