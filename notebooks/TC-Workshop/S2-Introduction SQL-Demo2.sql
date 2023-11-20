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

-- MAGIC %sql
-- MAGIC select count(*)
-- MAGIC from tbl_toronto_events_test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Merge data

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC /*
-- MAGIC Write sql command to merge the new DataFrame to the saved table
-- MAGIC */
-- MAGIC MERGE INTO tbl_toronto_events_test as t1
-- MAGIC USING df_new_data as t2
-- MAGIC ON t1.eventName = t2.eventName and t1.startDateTime = t2.startDateTime
-- MAGIC WHEN MATCHED THEN
-- MAGIC   UPDATE SET
-- MAGIC     event_description = t2.event_description
-- MAGIC     
-- MAGIC WHEN NOT MATCHED
-- MAGIC   THEN INSERT (
-- MAGIC    eventName,
-- MAGIC    event_category,
-- MAGIC    event_description,
-- MAGIC    shortDescription,
-- MAGIC    startDate,
-- MAGIC    endDate,
-- MAGIC    locationName,
-- MAGIC    freeEvent,
-- MAGIC    frequency,
-- MAGIC    cost,
-- MAGIC    expectedAvg,
-- MAGIC    allDay,
-- MAGIC    startDateTime,
-- MAGIC    event_start_dayofweek,
-- MAGIC    event_start_dayofyear,
-- MAGIC    event_date_id
-- MAGIC   )
-- MAGIC   VALUES (
-- MAGIC    t2.eventName,
-- MAGIC    t2.event_category,
-- MAGIC    t2.event_description,
-- MAGIC    t2.shortDescription,
-- MAGIC    t2.startDate,
-- MAGIC    t2.endDate,
-- MAGIC    t2.locationName,
-- MAGIC    t2.freeEvent,
-- MAGIC    t2.frequency,
-- MAGIC    t2.cost,
-- MAGIC    t2.expectedAvg,
-- MAGIC    t2.allDay,
-- MAGIC    t2.startDateTime,
-- MAGIC    t2.event_start_dayofweek,
-- MAGIC    t2.event_start_dayofyear,
-- MAGIC    t2.event_date_id
-- MAGIC   )

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Show table history (versioning)
-- MAGIC DESCRIBE HISTORY tbl_toronto_events_test

-- COMMAND ----------

-- DBTITLE 1,Versions
-- MAGIC %sql
-- MAGIC -- Count rows for each version
-- MAGIC SELECT 0 as version, count(*) as ct FROM tbl_toronto_events_test VERSION AS OF 0
-- MAGIC UNION ALL
-- MAGIC SELECT 1 as version, count(*) as ct FROM tbl_toronto_events_test VERSION AS OF 1

-- COMMAND ----------

-- DBTITLE 1,Time Travel
-- MAGIC %sql
-- MAGIC --select version at of timestamp
-- MAGIC SELECT * FROM tbl_toronto_events_test TIMESTAMP AS OF '2023-11-20T21:01:40'
-- MAGIC limit 100

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
-- MAGIC %sql
-- MAGIC -- Restore table to a specific timestamp
-- MAGIC --RESTORE TABLE test_db.toronto_events_transformed2 TO TIMESTAMP AS OF '2022-08-02 00:00:00';
-- MAGIC
-- MAGIC -- Restore the employee table to a specific version number retrieved from DESCRIBE HISTORY employee
-- MAGIC RESTORE TABLE tbl_toronto_events_test TO VERSION AS OF 0;
-- MAGIC DESCRIBE HISTORY tbl_toronto_events_test
-- MAGIC
-- MAGIC -- Restore the employee table to the state it was in an hour ago
-- MAGIC --RESTORE TABLE test_db.toronto_events_transformed2 TO TIMESTAMP AS OF current_timestamp() - INTERVAL '1' HOUR;

-- COMMAND ----------

-- DBTITLE 1,Cache
-- MAGIC %sql
-- MAGIC --Cache subset of table
-- MAGIC CACHE SELECT * FROM tbl_toronto_events_test where eventName like '%Raptor%'

-- COMMAND ----------

-- DBTITLE 1,Clone
-- MAGIC %sql
-- MAGIC --Clone table
-- MAGIC CREATE OR REPLACE TABLE tbl_toronto_events_clone CLONE tbl_toronto_events_test;
-- MAGIC
-- MAGIC select count(*) from tbl_toronto_events_clone

-- COMMAND ----------

-- DBTITLE 1,Convert to Delta
-- MAGIC %sql
-- MAGIC --Convert table to delta format
-- MAGIC CONVERT TO DELTA database_name.table_name; -- only for Parquet tables
-- MAGIC
-- MAGIC CONVERT TO DELTA parquet.`abfss://container-name@storage-account-name.dfs.core.windows.net/path/to/table`
-- MAGIC   PARTITIONED BY (date DATE); -- if the table is partitioned
