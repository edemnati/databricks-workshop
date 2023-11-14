-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --SHOW SCHEMAS IN samples
-- MAGIC SHOW TABLES IN samples.tpch

-- COMMAND ----------

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
-- MAGIC """
-- MAGIC Save DataFrame as delta format
-- MAGIC """
-- MAGIC (df
-- MAGIC     .write
-- MAGIC     .format("json")
-- MAGIC     .mode('overwrite')
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .save("/mnt/my_lake/td_workshop/toronto_events_raw.json")
-- MAGIC )

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS test_db.toronto_events_raw_delta;

 ALTER TABLE test_db.toronto_events_raw_delta SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
  );

COPY INTO test_db.toronto_events_raw_delta
FROM '/mnt/my_lake/td_workshop/toronto_events_raw.json' 
FILEFORMAT = JSON
FORMAT_OPTIONS('header'='true','inferSchema'='True')
COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

--Write a SQL query to: Select events that contain more than one row (observation)

SELECT calEvent.eventName,count(*) as ct 
FROM test_db.toronto_events_raw_delta
GROUP BY calEvent.eventName
HAVING ct >1
LIMIT 10

-- COMMAND ----------

--Write a SQL query to: count the numner of events per category

SELECT category.name as category_name,count(*) as ct 
FROM (SELECT calEvent.eventName,explode(calEvent.category) as category FROM test_db.toronto_events_raw_delta)
GROUP BY category_name
ORDER BY ct  DESC
LIMIT 10

-- COMMAND ----------

/*
Data transformation
1. Flatten dataframe calEvent structure
2. Keep columns:
    eventName,category.name,locations.locationName,shortDescription,startDate,endDate,locationName,
    freeEvent,frequency,cost,dates.allDay,dates.endDateTime,dates.startDateTime
    
    explode array: dates
    
3. Count number of rows
4. Save as a new table called: toronto_events_dataset
5. Count number of rows from test_db.toronto_events_dataset
*/

CREATE TABLE IF NOT EXISTS test_db.toronto_events_dataset AS
SELECT 
  calEvent.eventName,
  calEvent.category.name as event_category,
  calEvent.locations.locationName as event_location,
  calEvent.shortDescription,
  calEvent.startDate,
  calEvent.endDate,
  calEvent.freeEvent,
  calEvent.frequency,
  calEvent.cost,
  calEvent.expectedAvg,
  explode(calEvent.dates) as event_dates
FROM test_db.toronto_events_raw_delta;

SELECT count(*) FROM test_db.toronto_events_dataset;



-- COMMAND ----------


