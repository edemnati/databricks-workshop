-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Read Data
-- MAGIC
-- MAGIC __Run the following python code to prepare the data for the lab__

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import pyspark.sql.functions as f
-- MAGIC from pyspark.sql.window import Window
-- MAGIC import requests
-- MAGIC import json
-- MAGIC #Read data
-- MAGIC URL="https://secure.toronto.ca/cc_sr_v1/data/edc_eventcal_APR?limit=500"
-- MAGIC
-- MAGIC result = requests.get(URL).json()
-- MAGIC
-- MAGIC # Create a test table
-- MAGIC df_table = spark.read.json(sc.parallelize([json.dumps(result)]))
-- MAGIC
-- MAGIC # Create a test table
-- MAGIC #df_table=spark.read.format("delta").load("dbfs:/FileStore/datasets/toronto_events_transformed2")
-- MAGIC mount_point = "/mnt/my_lake"
-- MAGIC (df_table
-- MAGIC     .write
-- MAGIC     .format("json")
-- MAGIC     .mode('overwrite')
-- MAGIC     .option("overwriteSchema", "true")
-- MAGIC     .save(f"{mount_point}/tc_workshop/toronto_events_raw.json")
-- MAGIC )
-- MAGIC
-- MAGIC
-- MAGIC #Flatten dataset
-- MAGIC df_flatten = (df_table
-- MAGIC                 .select("calEvent.*")
-- MAGIC                 .select(f.explode("dates").alias("event_dates"),"*")
-- MAGIC                 .select(f.explode("locations").alias("event_location"),"*")
-- MAGIC                 .select("eventName",
-- MAGIC                          f.col("category.name").alias("event_category"),
-- MAGIC                          f.col("event_dates.description").alias("event_description"),
-- MAGIC                          "shortDescription",
-- MAGIC                          "startDate","endDate",
-- MAGIC                          "event_location.locationName",
-- MAGIC                          "freeEvent","frequency",
-- MAGIC                          "cost",
-- MAGIC                          "expectedAvg",
-- MAGIC                          "event_dates.allDay",
-- MAGIC                          "event_dates.startDateTime",
-- MAGIC                          "event_dates.endDateTime"
-- MAGIC                         )
-- MAGIC              ) 
-- MAGIC
-- MAGIC #Transform dataset
-- MAGIC df_flatten_transformed = (df_flatten
-- MAGIC                           .dropDuplicates()
-- MAGIC                           .withColumn("event_start_dayofweek",f.dayofweek("startDateTime"))
-- MAGIC                           .withColumn("event_start_dayofyear",f.dayofyear("startDateTime"))
-- MAGIC                                               .withColumn("event_date_id",
-- MAGIC                                                           f.row_number().over(Window.partitionBy(["eventName"])
-- MAGIC                                                                         .orderBy(f.col("startDateTime").asc())
-- MAGIC                                                                              )
-- MAGIC                                                          )
-- MAGIC                          )             
-- MAGIC
-- MAGIC #df_flatten_transformed.createOrReplaceTempView("toronto_events_test")
-- MAGIC df_flatten_transformed.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("tbl_toronto_events_test")
-- MAGIC
-- MAGIC
-- MAGIC #Create new DataFrame to test merge statement
-- MAGIC df_new_data = (df_flatten_transformed
-- MAGIC                    .where("eventName like '%Raptor%'")
-- MAGIC                    .withColumn('startDate',f.date_add("startDate",365))
-- MAGIC                    .withColumn('endDate',f.date_add("endDate",365))
-- MAGIC                    .withColumn('startDateTime',
-- MAGIC                                f.concat(f.date_add(f.substring(f.col('startDateTime'),0,10),365),
-- MAGIC                                         f.substring(f.col('startDateTime'),10,24)                                        
-- MAGIC                                        )
-- MAGIC                               )
-- MAGIC                    .withColumn('event_description',f.concat(f.col("event_description"),f.lit(" New name")))
-- MAGIC               )
-- MAGIC
-- MAGIC
-- MAGIC df_new_data.createOrReplaceTempView("df_new_data")
-- MAGIC display(df_new_data)

-- COMMAND ----------

select count(*)
from tbl_toronto_events_test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Merge data

-- COMMAND ----------

/*
Write sql command to merge the new DataFrame to the saved table
*/
MERGE INTO tbl_toronto_events_test as t1
USING df_new_data as t2
ON t1.eventName = t2.eventName and t1.startDateTime = t2.startDateTime
WHEN MATCHED THEN
  UPDATE SET
    event_description = t2.event_description
    
WHEN NOT MATCHED
  THEN INSERT (
   eventName,
   event_category,
   event_description,
   shortDescription,
   startDate,
   endDate,
   locationName,
   freeEvent,
   frequency,
   cost,
   expectedAvg,
   allDay,
   startDateTime,
   event_start_dayofweek,
   event_start_dayofyear,
   event_date_id
  )
  VALUES (
   t2.eventName,
   t2.event_category,
   t2.event_description,
   t2.shortDescription,
   t2.startDate,
   t2.endDate,
   t2.locationName,
   t2.freeEvent,
   t2.frequency,
   t2.cost,
   t2.expectedAvg,
   t2.allDay,
   t2.startDateTime,
   t2.event_start_dayofweek,
   t2.event_start_dayofyear,
   t2.event_date_id
  )

-- COMMAND ----------

-- Show table history (versioning)
DESCRIBE HISTORY tbl_toronto_events_test

-- COMMAND ----------

-- DBTITLE 1,Versions
-- Count rows for each version
SELECT 0 as version, count(*) as ct FROM tbl_toronto_events_test VERSION AS OF 0
UNION ALL
SELECT 1 as version, count(*) as ct FROM tbl_toronto_events_test VERSION AS OF 1

-- COMMAND ----------

-- DBTITLE 1,Time Travel
--select version at of timestamp
SELECT * FROM tbl_toronto_events_test TIMESTAMP AS OF '2023-11-20T21:01:40'
limit 100

-- COMMAND ----------

-- DBTITLE 1,Time Travel
-- MAGIC %python
-- MAGIC #select version at of timestamp
-- MAGIC df1 = spark.read.format('delta').option('timestampAsOf', '2023-11-20T21:01:40').table("tbl_toronto_events_test")
-- MAGIC
-- MAGIC df1.count()
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Restore
-- Restore table to a specific timestamp
--RESTORE TABLE test_db.toronto_events_transformed2 TO TIMESTAMP AS OF '2022-08-02 00:00:00';

-- Restore the employee table to a specific version number retrieved from DESCRIBE HISTORY employee
RESTORE TABLE tbl_toronto_events_test TO VERSION AS OF 0;
DESCRIBE HISTORY tbl_toronto_events_test

-- Restore the employee table to the state it was in an hour ago
--RESTORE TABLE test_db.toronto_events_transformed2 TO TIMESTAMP AS OF current_timestamp() - INTERVAL '1' HOUR;

-- COMMAND ----------

-- DBTITLE 1,Cache
--Cache subset of table
CACHE SELECT * FROM tbl_toronto_events_test where eventName like '%Raptor%'

-- COMMAND ----------

-- DBTITLE 1,Clone
--Clone table
CREATE OR REPLACE TABLE tbl_toronto_events_clone CLONE tbl_toronto_events_test;

select count(*) from tbl_toronto_events_clone

-- COMMAND ----------

-- DBTITLE 1,Convert to Delta
--Convert table to delta format
CONVERT TO DELTA database_name.table_name; -- only for Parquet tables

CONVERT TO DELTA parquet.`abfss://container-name@storage-account-name.dfs.core.windows.net/path/to/table`
  PARTITIONED BY (date DATE); -- if the table is partitioned
