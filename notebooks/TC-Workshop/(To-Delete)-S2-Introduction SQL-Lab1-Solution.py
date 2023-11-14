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
# MAGIC 6. Enhance data
# MAGIC 	1. Add day of week 
# MAGIC 	2. Add day of year
# MAGIC 	3. For events that have multiple dates, we want to assign an incremental number to each date 
# MAGIC 	for example, if an event has 5 dates, the earliest date will be assigned number 1 and last date will be assigned number 5
# MAGIC Save transformed data
# MAGIC
# MAGIC __pyspark SQL functions__
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
# MAGIC
# MAGIC __Delta Lake Cheat sheet__
# MAGIC https://pages.databricks.com/rs/094-YMS-629/images/Delta%20Lake%20Cheat%20Sheet.pdf

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Explore Dataset

# COMMAND ----------

import requests

#Read data
URL="https://secure.toronto.ca/cc_sr_v1/data/edc_eventcal_APR?limit=500"

result = requests.get(URL).json()

print(len(result))

# Save data
import json
with open("/dbfs/FileStore/ezzatdemnati/test.json","w") as f:
    json.dump(result,f) 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read data as spark dataFrame

# COMMAND ----------

"""
write code to read the json file that you save to dbfs folder
1. Count number of rows
2. Display dataframe schema
"""
df = spark.read.json("dbfs:/FileStore/ezzatdemnati/test.json")

print(f"Count rows:{df.count()}")
#1. Count number of rows

#2. Display dataframe schema
df.printSchema()

# COMMAND ----------

# Display DataFrame content
display(df)

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

"""
Write code to count the number of rows per eventName.
Order results by count in descending order
display result
"""

df.select("calEvent.eventName").groupBy("eventName").count().orderBy("count",ascending=False).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Flatten Dataset

# COMMAND ----------

import pyspark.sql.functions as f 
import pyspark.sql.types as T


"""
Data transformation
1. Flatten dataframe calEvent structure
2. Keep columns:
    eventName,category.name,shortDescription,startDate,endDate,locationName,
    freeEvent,frequency,cost,dates.allDay,dates.endDateTime,dates.startDateTime
    
    explode: category, dates, locations
    
3. Count number of rows

"""
print(f"Count or rows before transformation:{df.count()}")
df_flatten = (
                df
                .select("calEvent.*")
                .select(f.explode("dates").alias("event_dates"),"*")
                .select(f.explode("locations").alias("event_location"),"*")
                .select("eventName",
                         f.col("category.name").alias("event_category"),
                         f.col("event_dates.description").alias("event_description"),
                         "shortDescription",
                         "startDate","endDate",
                         "event_location.locationName",
                         "freeEvent","frequency",
                         "cost",
                         "expectedAvg",
                         "event_dates.allDay",
                         "event_dates.startDateTime",
                         "event_dates.endDateTime"
                        )
             ) 

print(f"Count of rows:{df_flatten.count()}")
display(df_flatten)

# COMMAND ----------

"""
Write code to keep only events that are related to Toronto Raptors
display result
"""

display(df_flatten.where("eventName like '%Raptors%'"))


# COMMAND ----------

"""
count number of events that are free or not
"""
display(df_flatten.groupBy("freeEvent").count())
display(df_flatten.select("eventName","freeEvent").distinct().groupBy("freeEvent").count())


# COMMAND ----------

"""
Add a column that show the percentage of free events
"""
import pyspark.sql.functions as f
from pyspark.sql.window import Window

windowSpec  = Window.partitionBy()
(df_flatten
    .select("eventName","freeEvent")
    .distinct()
    .select("eventName","freeEvent",f.count("eventName").over(windowSpec).alias("total"))
    .groupBy("freeEvent","total")
    .count()
    .withColumn("pct", f.round(f.col("count")/f.col("total")*100,2) )
    .orderBy("pct",ascending=False)
).display()



# COMMAND ----------

"""
Add a column that show the percentage of free events per category
1- Create a Free_event flag (0,1)
2- Count free events per category
"""
import pyspark.sql.functions as f
from pyspark.sql.window import Window

windowSpec  = Window.partitionBy()
df_flatten_filtered = (df_flatten
                       .select(f.explode("event_category").alias("event_category2"),"*")
                       .withColumn("is_freeEvent", f.when(df_flatten.freeEvent == "Yes",1).otherwise(0))
                        .select("eventName","is_freeEvent","event_category","event_category2","freeEvent")
                        .distinct()
                        )
(df_flatten_filtered
    .groupBy("event_category2")
    .sum("is_freeEvent")
    .withColumnRenamed("sum(is_freeEvent)","sum_is_freeEvent")
    #.withColumn("pct", f.round(f.col("sum(is_freeEvent)")/f.col("total")*100,2) )
    .orderBy("sum_is_freeEvent",ascending=False)
).display()



# COMMAND ----------

"""
Show events that are schedules only once
"""
df_flatten.where("frequency='once'").count()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Enhance Data

# COMMAND ----------

# MAGIC %md
# MAGIC 1. __df.distinct()__: This will return a new DataFrame that contains only the unique rows from the original DataFrame, based on all columns. This means that if two rows have the same id and name, but different age, they will both be kept in the result.
# MAGIC 1. __df.dropDuplicates(['id', 'name'])__: This will return a new DataFrame that contains only the unique rows from the original DataFrame, based on the specified columns. This means that if two rows have the same id and name, regardless of their age, only one of them will be kept in the result.
# MAGIC

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

# DBTITLE 1,Save to Table
"""
Save DataFrame to a new table in database test_db
"""

df_flatten_transformed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("test_db.toronto_events_transformed3")


# COMMAND ----------

# DBTITLE 1,Save to Delta file
"""
Save DataFrame as delta format
"""
(df_flatten_transformed
    .write
    #.partitionBy(<colA>,<colB>)
    .format("delta")
    .mode('overwrite')
    .option("overwriteSchema", "true")
    .save("/mnt/my_lake/td_workshop/test_data")
)


# COMMAND ----------

# MAGIC %sh
# MAGIC
# MAGIC ls /mnt/my_lake/td_workshop/test_data

# COMMAND ----------

"""
list folder content
"""
display(dbutils.fs.ls("/mnt/my_lake/td_workshop/test_data"))

# COMMAND ----------

"""
Test reading saved delta file
"""
df_test = spark.read.format("delta").load("/mnt/my_lake/td_workshop/test_data")
df_test.display()

# COMMAND ----------

#List all mounts
display(dbutils.fs.mounts())

