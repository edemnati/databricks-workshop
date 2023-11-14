-- Databricks notebook source


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

-- DBTITLE 1,Save to file
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

-- DBTITLE 1,Create a table
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


-- COMMAND ----------

--Write a SQL query to: count the numner of events per category



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
*/




-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create a Toronto Event and Festivals summary report
-- MAGIC 1. From the SQL editor, create a query that create a SQL view named vw_toronto_events_categories as:
-- MAGIC     - Explode event_category
-- MAGIC     - Cast startDatetime as date (formatuse this format: 'y-M-d')
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
