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
# MAGIC mv /local_disk0/spark-c6335ece-7f23-4d2e-8f49-8c2db8f682e6/userFiles-cb5c2d50-cc3a-455e-aa67-1b26ae9dbecf/edc_eventcal_APR /dbfs/FileStore/datasets/

# COMMAND ----------

"""
write code to read the json file that you save to dbfs folder
1. Count number of rows
2. Display dataframe schema
"""
df = spark.read.json("/FileStore/datasets/edc_eventcal_APR")

display(df.count())



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Check data for Raptors events

# COMMAND ----------

"""
Write code to keep only events that are related to Toronto Raptors
display result
"""
display(df.where("calEvent.eventName like '%Raptors%'"))

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
    
    explode: category, dates, locations
    
3. Count number of rows

"""
df_flatten = (
                df.select("calEvent.*")
                .select(f.explode("category").alias("event_category"),"*")
                .select(f.explode("dates").alias("event_dates"),"*")
                .select(f.explode("locations").alias("event_location"),"*")
                .select("eventName",
                         f.col("event_category.name").alias("event_category"),
                         f.col("event_dates.description").alias("event_description"),
                         "shortDescription",
                         "startDate","endDate",
                         "event_location.locationName",
                         "freeEvent","frequency",
                         "cost",
                         "expectedAvg",
                         "event_dates.allDay","event_dates.startDateTime"
                        )

             ) 

df_flatten.count()
display(df_flatten)

# COMMAND ----------

"""
Write code to keep only events that are related to Toronto Raptors
display result
"""

display(df_flatten.where("calEvent.eventName like '%Raptors%'"))


# COMMAND ----------

"""
Create a temporary view using DataFrame function createOrReplaceTempView()
"""
df_flatten.createOrReplaceTempView("toronto_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to count number of events that are free or not
# MAGIC */
# MAGIC select count(*) as ct_rows,
# MAGIC        count(distinct eventName) as ct_distinct_events,
# MAGIC        freeEvent
# MAGIC       from toronto_events
# MAGIC       where eventName like '%Raptors%'
# MAGIC       group by freeEvent
# MAGIC     order by freeEvent desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to count number of events per category
# MAGIC Add a column that show the percentage of free events per category (case statement)
# MAGIC */
# MAGIC select count(*) as ct_rows,
# MAGIC        count(distinct eventName) as ct_distinct_events,
# MAGIC        event_category,
# MAGIC        sum(case when freeEvent='Yes' then 1
# MAGIC             else 0
# MAGIC        end)/count(*)*100 as pct_free_events
# MAGIC       from toronto_events
# MAGIC       group by event_category
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to show events that are schedules only once
# MAGIC */
# MAGIC select *
# MAGIC from toronto_events
# MAGIC where frequency='once'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as ct_rows,
# MAGIC              event_category,freeEvent
# MAGIC       from toronto_events
# MAGIC       group by event_category,freeEvent

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_category,
# MAGIC        IsFree,
# MAGIC        NotFree,
# MAGIC        coalesce(IsFree,0)/(coalesce(NotFree,0)+coalesce(IsFree,0))*100  as pct_isFree
# MAGIC from(
# MAGIC       select count(*) as ct_rows,
# MAGIC              event_category,freeEvent
# MAGIC       from toronto_events
# MAGIC       group by event_category,freeEvent
# MAGIC       ) as a
# MAGIC pivot(sum(ct_rows) as ct_rows
# MAGIC       for freeEvent 
# MAGIC       IN ('No' as NotFree, 'Yes' as IsFree)
# MAGIC       )
# MAGIC order by pct_isFree desc

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC What is the percentage of free events?
# MAGIC Write SQL to:
# MAGIC   - Count number of rows
# MAGIC   - Calculate the percentage of free events
# MAGIC */
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to count number of events per location
# MAGIC */
# MAGIC
# MAGIC
# MAGIC select count(*) as ct_rows,
# MAGIC              count(distinct eventName, startDateTime) as ct_distinct
# MAGIC       from toronto_events
# MAGIC       
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enhance Data

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

"""
df_flatten.dropDuplicates
f.dayofweek("startDateTime")
f.dayofyear("startDateTime")
df_flatten.withColumn()

Use the flatten dataframe that you created previously to :
    1. Drop duplicates
    2. Add day of week 
    3. Add day of year
    4. Add a new column event_date_id as: for each event, assign an incremental number to each date for example, if an event has 5 dates, the earliest date will be assigned number 1 and last date will be assigned number 5 Save transformed data 
"""

df_flatten_transformed = (df_flatten
                          .dropDuplicates()
                          .withColumn("event_start_dayofweek",f.dayofweek("startDateTime"))
                          .withColumn("event_start_dayofyear",f.dayofyear("startDateTime"))
                                              .withColumn("event_date_id",
                                                          f.row_number().over(Window.partitionBy(["eventName"])
                                                                        .orderBy(f.col("startDateTime").asc())
                                                                             )
                                                         )
                         )
display(df_flatten_transformed.filter("eventName like '%Raptors%'"))

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
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS test_db;

# COMMAND ----------

# DBTITLE 1,Save to Table
"""
Save DataFrame to a new table in database test_db
"""

df_flatten_transformed.write.mode("overwrite").saveAsTable("test_db.toronto_events_transformed2")


# COMMAND ----------

# DBTITLE 1,Save to Delta file
"""
Save DataFrame to dbfs location as delta format
"""


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
