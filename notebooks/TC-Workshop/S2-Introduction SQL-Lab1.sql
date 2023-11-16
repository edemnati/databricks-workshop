-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Data Exploration
-- MAGIC
-- MAGIC __Steps:__
-- MAGIC Steps:
-- MAGIC 1. Get data:
-- MAGIC 	• Data source: Toronto Open data Portal - Festivals & Events:  https://open.toronto.ca/dataset/festivals-events/
-- MAGIC 2. Explore data structure
-- MAGIC 	1. Count number of rows
-- MAGIC 	2. Display dataframe schema
-- MAGIC 3. Data transformation
-- MAGIC 	1. Flatten dataframe structure
-- MAGIC 	2. Keep rows: eventName, category.name, shortDescription, startDate, endDate, locationName, freeEvent, frequency, cost, dates.allDay, dates.startDateTime
-- MAGIC 	3. Count number of rows
-- MAGIC 4. Data profiling
-- MAGIC 5. Data analysis
-- MAGIC 6. Enhance data
-- MAGIC 	1. Add day of week 
-- MAGIC 	2. Add day of year
-- MAGIC 	3. For events that have multiple dates, we want to assign an incremental number to each date 
-- MAGIC 	for example, if an event has 5 dates, the earliest date will be assigned number 1 and last date will be assigned number 5
-- MAGIC Save transformed data
-- MAGIC
-- MAGIC __pyspark SQL functions__
-- MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html
-- MAGIC
-- MAGIC __Delta Lake Cheat sheet__
-- MAGIC https://pages.databricks.com/rs/094-YMS-629/images/Delta%20Lake%20Cheat%20Sheet.pdf

-- COMMAND ----------

-- DBTITLE 1,Read data
-- MAGIC %python
-- MAGIC import requests
-- MAGIC import json
-- MAGIC #Read data
-- MAGIC URL="https://secure.toronto.ca/cc_sr_v1/data/edc_eventcal_APR?limit=500"
-- MAGIC
-- MAGIC result = requests.get(URL).json()
-- MAGIC
-- MAGIC df = spark.read.json(sc.parallelize([json.dumps(result)]))
-- MAGIC
-- MAGIC df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Run this command to find a mount that you can useto save the file to 
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # List files that are under the selected mount point
-- MAGIC mount_point = "/mnt/my_lake/"
-- MAGIC display(dbutils.fs.ls(mount_point))

-- COMMAND ----------

-- DBTITLE 1,Save to file
-- MAGIC %python
-- MAGIC """
-- MAGIC Save DataFrame as delta format
-- MAGIC TC mount: /mnt/tc-tea-air-01
-- MAGIC """
-- MAGIC (df
-- MAGIC     .write
-- MAGIC     .format("json")
-- MAGIC     .mode('overwrite')
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .save(f"{mount_point}/td_workshop/toronto_events_raw.json")
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 1,Create a table
CREATE DATABASE IF NOT EXISTS tes_db;
CREATE TABLE IF NOT EXISTS tes_db.toronto_events_raw_delta;

 ALTER TABLE test_db.toronto_events_raw_delta SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
  );

COPY INTO tes_db.toronto_events_raw_delta
FROM f'{mount_point}/td_workshop/toronto_events_raw.json' 
FILEFORMAT = JSON
FORMAT_OPTIONS('header'='true','inferSchema'='True')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

--Write a SQL query to: Select events that contain more than one row (observation)



-- COMMAND ----------

--Write a SQL query to: count the number of events per category 




-- COMMAND ----------

/*
Data transformation
1. Flatten dataframe calEvent structure
2. Keep columns:
    eventName,category.name,locations.locationName,shortDescription,startDate,endDate,
    freeEvent,frequency,cost,dates
    
    explode array: dates
    
3. Count number of rows
4. Save as a new table called: toronto_events_dataset
*/




-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a Toronto Event and Festivals summary report
-- MAGIC 1. From the SQL editor, create a query that create a SQL view named vw_toronto_events_categories as:
-- MAGIC     - Explode event_category
-- MAGIC     - Cast startDatetime as date (format use this format: 'y-M-d')
-- MAGIC 2. Save the new query
-- MAGIC 3. From the Catalog, click on the view and download the PowerBI Data source Connection file
-- MAGIC
-- MAGIC 4. Build a report the sho the following information:
-- MAGIC    - Total of events (Card)
-- MAGIC    - Total of free events (Card)
-- MAGIC    - Count of events per category (Bar chart)
-- MAGIC    - Count of events per day of week (Bar chart)
-- MAGIC    - Count of events per start date (Line chart)
-- MAGIC    - Event details (Table)
-- MAGIC
-- MAGIC    
