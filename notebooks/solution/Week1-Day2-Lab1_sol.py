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

df = (spark.read
      .option("multiline", "true")
      .json("dbfs:/FileStore/datasets/edc_eventcal_APR")      
     )
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Check data for Raptors events

# COMMAND ----------

"""
Write code to keep only evvents that are related to Toronto Raptors
display result
"""

display(df.where("calEvent.eventName like '%Raptor%'"))

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
             
df_flatten = (df.select("calEvent.*")
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
Write code to keep only evvents that are related to Toronto Raptors
display result
"""

display(df_flatten.where("eventName like '%Raptor%'"))

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
# MAGIC        freeEvent
# MAGIC       from toronto_events
# MAGIC       group by freeEvent
# MAGIC     order by freeEvent desc

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to count number of events per category
# MAGIC Add a column that show the percentage of free events per category
# MAGIC */
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
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write SQL to show events that are schedules only once
# MAGIC */
# MAGIC select * 
# MAGIC from toronto_events
# MAGIC where frequency='once'
# MAGIC order by event_category

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC What is the percentage of free events?
# MAGIC Write SQL to:
# MAGIC   - Count number of rows
# MAGIC   - Calculate the percentage of free events
# MAGIC */
# MAGIC
# MAGIC --Solution 1
# MAGIC select count(distinct eventName) as ct_distinct,
# MAGIC        count(*) as ct_rows,
# MAGIC        sum(case when freeEvent='Yes' then 1 else 0 end)/count(*)*100 as pct_free
# MAGIC from toronto_events
# MAGIC order by ct_distinct desc
# MAGIC
# MAGIC

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
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select locationName,count(distinct eventName) as ct_distinct,count(*) as ct
# MAGIC from toronto_events
# MAGIC group by locationName
# MAGIC order by ct_distinct desc
# MAGIC
# MAGIC

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

df_flatten2 = (df_flatten
               .drop_duplicates()
               .withColumn("event_start_dayofweek",f.dayofweek('startDateTime')).alias('day')
               .withColumn("event_start_dayofyear",f.dayofyear('startDateTime')).alias('day')
               .withColumn("event_date_id",f.row_number().over(Window.partitionBy(["event_description","event_category"]).orderBy(f.col("startDateTime").desc())))
)

display(df_flatten2)

# COMMAND ----------

"""
Write code to keep only events that are related to Toronto Raptors
display result
"""
display(df_flatten2.where("eventName like '%Raptor%'"))

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
# MAGIC describe extended  test_db.toronto_events_transformed2

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
# MAGIC select count(*)
# MAGIC from test_db.toronto_events_transformed2

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Write sql command to merge the new DataFrame to the saved table
# MAGIC */
# MAGIC MERGE INTO test_db.toronto_events_transformed2 as t1
# MAGIC USING df_new_data as t2
# MAGIC ON t1.eventName = t2.eventName and t1.startDateTime = t2.startDateTime
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     event_description = t2.event_description
# MAGIC     
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC    eventName,
# MAGIC    event_category,
# MAGIC    event_description,
# MAGIC    shortDescription,
# MAGIC    startDate,
# MAGIC    endDate,
# MAGIC    locationName,
# MAGIC    freeEvent,
# MAGIC    frequency,
# MAGIC    cost,
# MAGIC    expectedAvg,
# MAGIC    allDay,
# MAGIC    startDateTime,
# MAGIC    event_start_dayofweek,
# MAGIC    event_start_dayofyear,
# MAGIC    event_date_id
# MAGIC   )
# MAGIC   VALUES (
# MAGIC    t2.eventName,
# MAGIC    t2.event_category,
# MAGIC    t2.event_description,
# MAGIC    t2.shortDescription,
# MAGIC    t2.startDate,
# MAGIC    t2.endDate,
# MAGIC    t2.locationName,
# MAGIC    t2.freeEvent,
# MAGIC    t2.frequency,
# MAGIC    t2.cost,
# MAGIC    t2.expectedAvg,
# MAGIC    t2.allDay,
# MAGIC    t2.startDateTime,
# MAGIC    t2.event_start_dayofweek,
# MAGIC    t2.event_start_dayofyear,
# MAGIC    t2.event_date_id
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table history (versioning)
# MAGIC DESCRIBE HISTORY test_db.toronto_events_transformed2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count rows for each version
# MAGIC SELECT 3 as version, count(*) as ct FROM test_db.toronto_events_transformed2 VERSION AS OF 3
# MAGIC UNION ALL
# MAGIC SELECT 4 as version, count(*) as ct FROM test_db.toronto_events_transformed2 VERSION AS OF 4

# COMMAND ----------

# MAGIC %sql
# MAGIC --select version at of timestamp
# MAGIC SELECT 0 as version, count(*) as ct FROM test_db.toronto_events_transformed2 TIMESTAMP AS OF '2023-02-01'

# COMMAND ----------

#select version at of timestamp
df1 = spark.read.format('delta').option('timestampAsOf', '2023-02-01').table("test_db.toronto_events_transformed2")

df1.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore table to a specific timestamp
# MAGIC --RESTORE TABLE test_db.toronto_events_transformed2 TO TIMESTAMP AS OF '2022-08-02 00:00:00';
# MAGIC
# MAGIC -- Restore the employee table to a specific version number retrieved from DESCRIBE HISTORY employee
# MAGIC RESTORE TABLE test_db.toronto_events_transformed2 TO VERSION AS OF 0;
# MAGIC DESCRIBE HISTORY test_db.toronto_events_transformed2
# MAGIC
# MAGIC -- Restore the employee table to the state it was in an hour ago
# MAGIC --RESTORE TABLE test_db.toronto_events_transformed2 TO TIMESTAMP AS OF current_timestamp() - INTERVAL '1' HOUR;

# COMMAND ----------

# MAGIC %sql
# MAGIC --Cache subset of table
# MAGIC CACHE SELECT * FROM test_db.toronto_events_transformed2 where eventName like '%Raptor%'

# COMMAND ----------

# MAGIC %sql
# MAGIC --Clone table
# MAGIC CREATE OR REPLACE TABLE test_db.toronto_events_clone CLONE test_db.toronto_events_transformed2;
# MAGIC
# MAGIC select count(*) from test_db.toronto_events_clone

# COMMAND ----------

# MAGIC %sql
# MAGIC --Convert table to delta format
# MAGIC CONVERT TO DELTA database_name.table_name; -- only for Parquet tables
# MAGIC
# MAGIC CONVERT TO DELTA parquet.`abfss://container-name@storage-account-name.dfs.core.windows.net/path/to/table`
# MAGIC   PARTITIONED BY (date DATE); -- if the table is partitioned
