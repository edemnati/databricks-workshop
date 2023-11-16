-- Databricks notebook source


-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Introduction to Databricks
-- MAGIC
-- MAGIC Databricck Moden Ar
-- MAGIC ![](https://learn.microsoft.com/en-us/azure/databricks/scenarios/media/what-is-azure-databricks/azure-databricks-overview.png)
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC __What is Databricks__
-- MAGIC _"The Azure Databricks Lakehouse Platform provides a unified set of tools for building, deploying, sharing, and maintaining enterprise-grade data solutions at scale. Azure Databricks integrates with cloud storage and security in your cloud account, and manages and deploys cloud infrastructure on your behalf."_
-- MAGIC
-- MAGIC __What are common use cases for Azure Databricks?__
-- MAGIC   - Build an enterprise data lakehouse
-- MAGIC 	  - The Databricks Lakehouse combines the ACID transactions and data governance of data warehouses with the flexibility and cost-efficiency of data lakes to enable business intelligence (BI) and machine learning (ML) on all data. 
-- MAGIC 	  - ACID stands for atomicity, consistency, isolation, and durability.
-- MAGIC 		  -  Atomicity means that all transactions either succeed or fail completely.
-- MAGIC 		  - Consistency guarantees relate to how a given state of the data is observed by simultaneous operations.
-- MAGIC 		  - Isolation refers to how simultaneous operations potentially conflict with one another.
-- MAGIC 		  - Durability means that committed changes are permanent.
-- MAGIC 		
-- MAGIC   - ETL and data engineering
-- MAGIC   - Machine learning, AI, and data science
-- MAGIC   - Data warehousing, analytics, and BI
-- MAGIC   - Data governance and secure data sharing
-- MAGIC   - DevOps, CI/CD, and task orchestration
-- MAGIC   - Real-time and streaming analytics

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Demo summary
-- MAGIC
-- MAGIC _pyspark API reference: https://spark.apache.org/docs/latest/api/python/reference/index.html_
-- MAGIC
-- MAGIC __Read data__
-- MAGIC
-- MAGIC 1. Explore sample datasets
-- MAGIC 1. Read Tables using SQL
-- MAGIC 1. Discover and preview data
-- MAGIC 1. Create table from CSV File (External tables and Managed tables)
-- MAGIC 1. Create table from select statement output
-- MAGIC 1. Explore data using SQL
-- MAGIC     1. Select variables
-- MAGIC     1. Filter rows based on conditions
-- MAGIC     1. Count observations per group
-- MAGIC     1. Group by variable and apply aggregation functions
-- MAGIC 1. Working with nested data
-- MAGIC     1. Create managed table from json file
-- MAGIC     1. Flatten table structure
-- MAGIC 1. Join tables
-- MAGIC 1. Union tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Explore sample datasets
-- MAGIC Ref: https://learn.microsoft.com/en-us/azure/databricks/dbfs/databricks-datasets

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --SHOW SCHEMAS IN samples
-- MAGIC SHOW TABLES IN samples.tpch

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Read  Tables using SQL
-- MAGIC
-- MAGIC Azure Databricks uses Delta Lake for all tables by default. Once data are loaded into tables, they can easily be queried using SQL
-- MAGIC - SQL Langugage reference: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/
-- MAGIC - SQL Built-in Functions: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin-alpha

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC --Select and were statements
-- MAGIC SELECT *
-- MAGIC FROM samples.tpch.customer 
-- MAGIC WHERE c_mktsegment in ('BUILDING','FURNITURE')
-- MAGIC LIMIT 100

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC --Join tables and aggregate results.
-- MAGIC SELECT  
-- MAGIC   c_mktsegment,
-- MAGIC   o_orderpriority,
-- MAGIC   o_orderstatus, 
-- MAGIC   sum(o_totalprice) as sum_order_total 
-- MAGIC FROM samples.tpch.orders as o 
-- MAGIC LEFT JOIN samples.tpch.customer as c on c.c_custkey=o.o_custkey
-- MAGIC WHERE c_mktsegment in ('BUILDING','FURNITURE')
-- MAGIC GROUP BY c_mktsegment,o_orderpriority,o_orderstatus
-- MAGIC ORDER BY sum_order_total DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Discover and preview data

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Explore Mount points that are available
-- MAGIC display(dbutils.fs.mounts())

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/mnt/my_lake/"))

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC /* Preview your data */
-- MAGIC SELECT * FROM read_files("dbfs:/mnt/my_lake/NYCTripSmall.parquet") LIMIT 10
-- MAGIC
-- MAGIC -- OR --
-- MAGIC --SELECT * FROM csv.`/mnt/my_lake/data/daily-bike-share.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table from CSV File (External tables and Managed tables)

-- COMMAND ----------

-- DBTITLE 1,Create test database
-- MAGIC %sql
-- MAGIC --CREATE [SCHEMA|DATABASE] IF NOT EXISTS database_name
-- MAGIC
-- MAGIC CREATE DATABASE IF NOT EXISTS test_db

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Create External Table
-- MAGIC
-- MAGIC --Path ADLS nomenclature: abfss://<container>@<storage-account>.dfs.core.windows.net/<folder>
-- MAGIC --DROP TABLE test_db.tbl_daily_bike_share_EXT;
-- MAGIC CREATE TABLE IF NOT EXISTS tbl_daily_bike_share_EXT
-- MAGIC USING CSV
-- MAGIC OPTIONS (path "/mnt/my_lake/data/daily-bike-share.csv",
-- MAGIC 'header' = 'true',
-- MAGIC   'inferSchema' = 'true',
-- MAGIC   'mergeSchema' = 'true')
-- MAGIC ;
-- MAGIC SELECT year,count(*) FROM test_db.tbl_daily_bike_share_EXT
-- MAGIC group by year
-- MAGIC LIMIT 100
-- MAGIC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC -- Create Managed Table
-- MAGIC CREATE TABLE IF NOT EXISTS test_db.tbl_daily_bike_share_csv_managed;
-- MAGIC
-- MAGIC COPY INTO test_db.tbl_daily_bike_share_csv_managed
-- MAGIC FROM '/mnt/my_lake2/data/daily-bike-share.csv' 
-- MAGIC FILEFORMAT = CSV
-- MAGIC FORMAT_OPTIONS('header'='true','inferSchema'='True')
-- MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
-- MAGIC ;
-- MAGIC SELECT year,count(*) FROM test_db.tbl_daily_bike_share_csv_managed
-- MAGIC group by year
-- MAGIC LIMIT 100

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --[ EXTENDED | FORMATTED ]
-- MAGIC DESCRIBE TABLE FORMATTED test_db.tbl_daily_bike_share_csv_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table from select statement output

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS test_db.tbl_daily_bike_share_EXT_2011;
-- MAGIC CREATE TABLE test_db.tbl_daily_bike_share_EXT_2011 
-- MAGIC AS SELECT * FROM test_db.tbl_daily_bike_share_EXT where year=2011;
-- MAGIC
-- MAGIC SELECT year,count(*) FROM test_db.tbl_daily_bike_share_EXT_2011
-- MAGIC group by year
-- MAGIC LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore data using SQL

-- COMMAND ----------

-- DBTITLE 1,Select variables
-- MAGIC %sql
-- MAGIC
-- MAGIC --Select distinct values 
-- MAGIC
-- MAGIC SELECT DISTINCT season, holiday
-- MAGIC FROM test_db.tbl_daily_bike_share_csv_managed
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Filter rows based on conditions
-- MAGIC %sql
-- MAGIC
-- MAGIC SELECT DISTINCT season, holiday
-- MAGIC FROM test_db.tbl_daily_bike_share_csv_managed
-- MAGIC WHERE windspeed<0.19

-- COMMAND ----------

-- DBTITLE 1,Count observations per group
-- MAGIC %sql
-- MAGIC --Group and count values
-- MAGIC
-- MAGIC SELECT  season, holiday, count(*) as ct
-- MAGIC FROM test_db.tbl_daily_bike_share_csv_managed
-- MAGIC GROUP BY season, holiday

-- COMMAND ----------

-- DBTITLE 1,Group by variable and apply aggregation functions
-- MAGIC %sql
-- MAGIC
-- MAGIC --Group and agggregate values
-- MAGIC SELECT  mnth, 
-- MAGIC         count(*) as ct,
-- MAGIC         avg(windspeed) as avg_windspeed,
-- MAGIC         median(windspeed) as med_windspeed,
-- MAGIC         sum(rentals) as sum_rentals
-- MAGIC FROM test_db.tbl_daily_bike_share_csv_managed
-- MAGIC GROUP BY mnth
-- MAGIC ORDER BY avg_windspeed DESC
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Working with nested data

-- COMMAND ----------

-- DBTITLE 1,Create managed table from json file
-- MAGIC %sql
-- MAGIC -- Create Managed Table
-- MAGIC DRop table test_db.test_json_array_nested_managed;
-- MAGIC CREATE TABLE IF NOT EXISTS test_db.test_json_array_nested_managed;
-- MAGIC COPY INTO test_db.test_json_array_nested_managed
-- MAGIC FROM '/mnt/my_lake2/data/test_json_array_nested.json' 
-- MAGIC FILEFORMAT = JSON
-- MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
-- MAGIC ;
-- MAGIC
-- MAGIC SELECT * FROM test_db.test_json_array_nested_managed
-- MAGIC LIMIT 100
-- MAGIC

-- COMMAND ----------

SELECT a,b,c,d_exp.*
FROM (
            SELECT a,b,c,explode(d) as d_exp
                  FROM test_db.test_json_array_nested_managed
)


-- COMMAND ----------

-- DBTITLE 1,Flatten table structure
-- MAGIC %sql
-- MAGIC
-- MAGIC --Array
-- MAGIC --Structure 
-- MAGIC
-- MAGIC CREATE TABLE IF NOT EXISTS test_db.test_json_flat_managed
-- MAGIC AS 
-- MAGIC SELECT a,b,c,col.*,upper(col.Country) as UPP_Country
-- MAGIC FROM (
-- MAGIC       SELECT a,b,c,explode(d) as col 
-- MAGIC       FROM test_db.test_json_array_nested_managed
-- MAGIC );
-- MAGIC
-- MAGIC SELECT * FROM test_db.test_json_flat_managed
-- MAGIC LIMIT 100
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Join tables
-- MAGIC %sql
-- MAGIC
-- MAGIC -- Create Managed Table to join with
-- MAGIC CREATE TABLE IF NOT EXISTS test_db.test_json_managed;
-- MAGIC COPY INTO test_db.test_json_managed
-- MAGIC FROM '/mnt/my_lake2/data/test_json.json' 
-- MAGIC FILEFORMAT = JSON
-- MAGIC COPY_OPTIONS ('mergeSchema' = 'true');
-- MAGIC ;
-- MAGIC
-- MAGIC --Select from joined tables
-- MAGIC SELECT * 
-- MAGIC FROM test_db.test_json_flat_managed as tbl1
-- MAGIC INNER JOIN test_db.test_json_managed tbl2 ON tbl1.a=tbl2.a
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Union tables
-- MAGIC %sql
-- MAGIC SELECT * 
-- MAGIC FROM 
-- MAGIC (SELECT a,b,c FROM test_db.test_json_flat_managed)
-- MAGIC UNION
-- MAGIC (SELECT a,b,c FROM test_db.test_json_managed)
