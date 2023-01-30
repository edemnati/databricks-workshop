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

from pyspark import SparkFiles

# Read data with spark
URL="https://secure.toronto.ca/cc_sr_v1/data/edc_eventcal_APR?limit=500"
spark.sparkContext.addFile(URL)

df = (spark.read
      #.format("json")
      .option("multiline", "true")
      #.load(URL)
      .json("file://"+SparkFiles.get("edc_eventcal_APR"))
     )
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Check data for Raptors events

# COMMAND ----------

display(df.where("calEvent.eventName like '%Raptor%'"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Flatten Dataset

# COMMAND ----------

import pyspark.sql.functions as f 

"""
Keep columns:
    eventName,category.name,shortDescription,startDate,endDate,locationName,
    freeEvent,frequency,cost,dates.allDay,dates.endDateTime,dates.startDateTime
"""

df_flatten = (df.select("calEvent.*")
 .select(f.explode("category").alias("event_category"),"*")
 .select(f.explode("dates").alias("event_dates"),"*")
 #.select(f.explode("dates.startDateTime").alias("event_startDateTime"),"*")
 .select(f.explode("locations").alias("event_location"),"*")
 .select("eventName",
         f.col("event_category.name").alias("event_category"),
         f.col("event_dates.description").alias("event_description"),
         "shortDescription",
         "startDate","endDate",
         "event_location.locationName",
         "freeEvent","frequency",
         "cost",
         "event_dates.allDay","event_dates.startDateTime"
        )
)
df_flatten.count()

# COMMAND ----------

display(df_flatten)

# COMMAND ----------

display(df_flatten.where("eventName like '%Raptor%'"))

# COMMAND ----------

df_flatten.createOrReplaceTempView("toronto_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as ct_rows,
# MAGIC        freeEvent
# MAGIC       from toronto_events
# MAGIC       group by freeEvent
# MAGIC     order by freeEvent desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as ct_rows,
# MAGIC        event_category,
# MAGIC        sum(case when freeEvent='Yes' then 1 else 0 end)/count(*)*100 as pct_isFree
# MAGIC       from toronto_events
# MAGIC       group by event_category
# MAGIC     order by pct_isFree desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_category,
# MAGIC        IsFree,
# MAGIC        NotFree,
# MAGIC        coalesce(IsFree,0)/(coalesce(NotFree,0)+coalesce(IsFree,0))  as pct_isFree
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
# MAGIC select count(*) as ct_rows,
# MAGIC        event_category,frequency
# MAGIC from toronto_events
# MAGIC group by event_category,frequency

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from toronto_events
# MAGIC where frequency='once'
# MAGIC order by event_category

# COMMAND ----------

# MAGIC %sql
# MAGIC --Solution 1
# MAGIC select count(distinct eventName) as ct_distinct,
# MAGIC        count(*) as ct_rows,
# MAGIC        sum(case when freeEvent='Yes' then 1 else 0 end)/count(*)*100 as pct_free
# MAGIC from toronto_events
# MAGIC order by ct_distinct desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --Solution 2
# MAGIC select count(distinct eventName) as ct_distinct,
# MAGIC        count(*) as ct_rows,
# MAGIC        sum(case when freeEvent='Yes' then 1 else 0 end)/count(*)*100 as pct_free
# MAGIC from (select distinct eventName,freeEvent from toronto_events) as a
# MAGIC order by ct_distinct desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select event_category,count(distinct eventName) as ct_distinct,count(*) as ct
# MAGIC from toronto_events
# MAGIC group by event_category
# MAGIC order by ct_distinct desc

# COMMAND ----------

# MAGIC %sql
# MAGIC select locationName,count(distinct eventName) as ct_distinct,count(*) as ct
# MAGIC from toronto_events
# MAGIC group by locationName
# MAGIC order by ct_distinct desc

# COMMAND ----------

# MAGIC %md
# MAGIC ## Enhance Data

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

df_flatten2 = (df_flatten
               .drop_duplicates()
               .withColumn("event_start_dayofweek",f.dayofweek('startDateTime')).alias('day')
               .withColumn("event_start_dayofyear",f.dayofyear('startDateTime')).alias('day')
               .withColumn("event_date_id",f.row_number().over(Window.partitionBy(["event_description","event_category"]).orderBy(f.col("startDateTime").desc())))
)

display(df_flatten2)

# COMMAND ----------

display(df_flatten2.where("eventName like '%Raptor%'"))

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS test_db;

# COMMAND ----------

# DBTITLE 1,Save to Table
df_flatten2.write.mode("overwrite").saveAsTable("test_db.toronto_events_transformed")

# COMMAND ----------

# DBTITLE 1,Save to Delta file
df_flatten2.write.mode("overwrite").format("delta").save("dbfs:/FileStore/datasets/toronto_events_transformed")
