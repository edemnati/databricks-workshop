-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Read Data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import pyspark.sql.functions as f
-- MAGIC
-- MAGIC # Create a test table
-- MAGIC df_table=spark.read.format("delta").load("dbfs:/FileStore/datasets/toronto_events_transformed2")
-- MAGIC
-- MAGIC df_table.write.option("mergeSchema", "true").mode("overwrite").saveAsTable("test_db.toronto_events_test")
-- MAGIC
-- MAGIC #Create new DataFrame to test merge statement
-- MAGIC df_new_data = (spark.read.format("delta").load("dbfs:/FileStore/datasets/toronto_events_transformed2")
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
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC select count(*)
-- MAGIC from test_db.toronto_events_test

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Merge data

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC /*
-- MAGIC Write sql command to merge the new DataFrame to the saved table
-- MAGIC */
-- MAGIC MERGE INTO test_db.toronto_events_transformed2 as t1
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
-- MAGIC DESCRIBE HISTORY test_db.toronto_events_transformed2

-- COMMAND ----------

-- DBTITLE 1,Versions
-- MAGIC %sql
-- MAGIC -- Count rows for each version
-- MAGIC SELECT 0 as version, count(*) as ct FROM test_db.toronto_events_transformed2 VERSION AS OF 0
-- MAGIC UNION ALL
-- MAGIC SELECT 6 as version, count(*) as ct FROM test_db.toronto_events_transformed2 VERSION AS OF 6

-- COMMAND ----------

-- DBTITLE 1,Time Travel
-- MAGIC %sql
-- MAGIC --select version at of timestamp
-- MAGIC SELECT count(*) as ct FROM test_db.toronto_events_transformed2 TIMESTAMP AS OF '2023-11-14T17:29:38.000+00:00'

-- COMMAND ----------

-- DBTITLE 1,Time Travel
-- MAGIC %python
-- MAGIC #select version at of timestamp
-- MAGIC df1 = spark.read.format('delta').option('timestampAsOf', '2023-02-01').table("test_db.toronto_events_transformed2")
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
-- MAGIC RESTORE TABLE test_db.toronto_events_transformed2 TO VERSION AS OF 0;
-- MAGIC DESCRIBE HISTORY test_db.toronto_events_transformed2
-- MAGIC
-- MAGIC -- Restore the employee table to the state it was in an hour ago
-- MAGIC --RESTORE TABLE test_db.toronto_events_transformed2 TO TIMESTAMP AS OF current_timestamp() - INTERVAL '1' HOUR;

-- COMMAND ----------

-- DBTITLE 1,Cache
-- MAGIC %sql
-- MAGIC --Cache subset of table
-- MAGIC CACHE SELECT * FROM test_db.toronto_events_transformed2 where eventName like '%Raptor%'

-- COMMAND ----------

-- DBTITLE 1,Clone
-- MAGIC %sql
-- MAGIC --Clone table
-- MAGIC CREATE OR REPLACE TABLE test_db.toronto_events_clone CLONE test_db.toronto_events_transformed2;
-- MAGIC
-- MAGIC select count(*) from test_db.toronto_events_clone

-- COMMAND ----------

-- DBTITLE 1,Convert to Delta
-- MAGIC %sql
-- MAGIC --Convert table to delta format
-- MAGIC CONVERT TO DELTA database_name.table_name; -- only for Parquet tables
-- MAGIC
-- MAGIC CONVERT TO DELTA parquet.`abfss://container-name@storage-account-name.dfs.core.windows.net/path/to/table`
-- MAGIC   PARTITIONED BY (date DATE); -- if the table is partitioned
