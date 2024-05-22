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

--SHOW SCHEMAS IN samples
SHOW TABLES IN samples.tpch

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Read  Tables using SQL
-- MAGIC
-- MAGIC Azure Databricks uses Delta Lake for all tables by default. Once data are loaded into tables, they can easily be queried using SQL
-- MAGIC - SQL Langugage reference: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/
-- MAGIC - SQL Built-in Functions: https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-functions-builtin-alpha

-- COMMAND ----------


--Select and were statements
SELECT *
FROM samples.tpch.customer 
WHERE c_mktsegment in ('BUILDING','FURNITURE')
LIMIT 100

-- COMMAND ----------


--Join tables and aggregate results.
SELECT  
  c_mktsegment,
  o_orderpriority,
  o_orderstatus, 
  sum(o_totalprice) as sum_order_total 
FROM samples.tpch.orders as o 
LEFT JOIN samples.tpch.customer as c on c.c_custkey=o.o_custkey
WHERE c_mktsegment in ('BUILDING','FURNITURE')
GROUP BY c_mktsegment,o_orderpriority,o_orderstatus
ORDER BY sum_order_total DESC

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

-- MAGIC %python
-- MAGIC # Azure storage access info
-- MAGIC blob_account_name = "azureopendatastorage"
-- MAGIC blob_container_name = "nyctlc"
-- MAGIC blob_relative_path = "yellow"
-- MAGIC blob_sas_token = "r"
-- MAGIC
-- MAGIC # Allow SPARK to read from Blob remotely
-- MAGIC wasbs_path = 'wasbs://%s@%s.blob.core.windows.net/%s' % (blob_container_name, blob_account_name, blob_relative_path)
-- MAGIC spark.conf.set(
-- MAGIC   'fs.azure.sas.%s.%s.blob.core.windows.net' % (blob_container_name, blob_account_name),
-- MAGIC   blob_sas_token)
-- MAGIC print('Remote blob path: ' + wasbs_path)
-- MAGIC
-- MAGIC #Check storage content
-- MAGIC #display(dbutils.fs.ls("wasbs://nyctlc@azureopendatastorage.blob.core.windows.net"))
-- MAGIC
-- MAGIC # SPARK read parquet, note that it won't load any data yet by now
-- MAGIC df = spark.read.parquet(wasbs_path)
-- MAGIC print('Register the DataFrame as a SQL temporary view: source')
-- MAGIC df.createOrReplaceTempView('source')
-- MAGIC
-- MAGIC # Display top 10 rows
-- MAGIC print('Displaying top 10 rows: ')
-- MAGIC display(spark.sql('SELECT * FROM source LIMIT 10'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC pdf = pd.read_csv("https://raw.githubusercontent.com/edemnati/databricks-workshop/main/datasets/daily-bike-share.csv")
-- MAGIC
-- MAGIC df = spark.createDataFrame(pdf)
-- MAGIC display(df)

-- COMMAND ----------

/* Preview your data */
SELECT * FROM read_files("dbfs:/mnt/my_lake/NYCTripSmall.parquet") LIMIT 10

-- OR --
--SELECT * FROM csv.`/mnt/my_lake/data/daily-bike-share.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table from CSV File (External tables and Managed tables)

-- COMMAND ----------

-- DBTITLE 1,Create test database
--CREATE [SCHEMA|DATABASE] IF NOT EXISTS database_name

CREATE DATABASE IF NOT EXISTS test_db

-- COMMAND ----------

-- Create External Table

--Path ADLS nomenclature: abfss://<container>@<storage-account>.dfs.core.windows.net/<folder>
--DROP TABLE test_db.tbl_daily_bike_share_EXT;
CREATE TABLE IF NOT EXISTS tbl_daily_bike_share_EXT
USING CSV
OPTIONS (path "/mnt/my_lake/data/daily-bike-share.csv",
'header' = 'true',
  'inferSchema' = 'true',
  'mergeSchema' = 'true')
;
SELECT year,count(*) FROM test_db.tbl_daily_bike_share_EXT
group by year
LIMIT 100

-- COMMAND ----------

-- Create Managed Table
CREATE TABLE IF NOT EXISTS test_db.tbl_daily_bike_share_csv_managed;

COPY INTO test_db.tbl_daily_bike_share_csv_managed
FROM '/mnt/my_lake2/data/daily-bike-share.csv' 
FILEFORMAT = CSV
FORMAT_OPTIONS('header'='true','inferSchema'='True')
COPY_OPTIONS ('mergeSchema' = 'true');
;
SELECT year,count(*) FROM test_db.tbl_daily_bike_share_csv_managed
group by year
LIMIT 100

-- COMMAND ----------

--[ EXTENDED | FORMATTED ]
DESCRIBE TABLE FORMATTED test_db.tbl_daily_bike_share_csv_managed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create table from select statement output

-- COMMAND ----------

DROP TABLE IF EXISTS test_db.tbl_daily_bike_share_EXT_2011;
CREATE TABLE test_db.tbl_daily_bike_share_EXT_2011 
AS SELECT * FROM test_db.tbl_daily_bike_share_EXT where year=2011;

SELECT year,count(*) FROM test_db.tbl_daily_bike_share_EXT_2011
group by year
LIMIT 100

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Explore data using SQL

-- COMMAND ----------

-- DBTITLE 1,Select variables

--Select distinct values 

SELECT DISTINCT season, holiday
FROM test_db.tbl_daily_bike_share_csv_managed

-- COMMAND ----------

-- DBTITLE 1,Filter rows based on conditions

SELECT DISTINCT season, holiday
FROM test_db.tbl_daily_bike_share_csv_managed
WHERE windspeed<0.19

-- COMMAND ----------

-- DBTITLE 1,Count observations per group
--Group and count values

SELECT  season, holiday, count(*) as ct
FROM test_db.tbl_daily_bike_share_csv_managed
GROUP BY season, holiday

-- COMMAND ----------

-- DBTITLE 1,Group by variable and apply aggregation functions

--Group and agggregate values
SELECT  mnth, 
        count(*) as ct,
        avg(windspeed) as avg_windspeed,
        median(windspeed) as med_windspeed,
        sum(rentals) as sum_rentals
FROM test_db.tbl_daily_bike_share_csv_managed
GROUP BY mnth
ORDER BY avg_windspeed DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Working with nested data

-- COMMAND ----------

-- DBTITLE 1,Create managed table from json file
-- Create Managed Table
DRop table test_db.test_json_array_nested_managed;
CREATE TABLE IF NOT EXISTS test_db.test_json_array_nested_managed;
COPY INTO test_db.test_json_array_nested_managed
FROM '/mnt/my_lake2/data/test_json_array_nested.json' 
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');
;

SELECT * FROM test_db.test_json_array_nested_managed
LIMIT 100

-- COMMAND ----------

SELECT a,b,c,d_exp.*
FROM (
            SELECT a,b,c,explode(d) as d_exp
                  FROM test_db.test_json_array_nested_managed
)


-- COMMAND ----------

-- DBTITLE 1,Flatten table structure

--Array
--Structure 

CREATE TABLE IF NOT EXISTS test_db.test_json_flat_managed
AS 
SELECT a,b,c,col.*,upper(col.Country) as UPP_Country
FROM (
      SELECT a,b,c,explode(d) as col 
      FROM test_db.test_json_array_nested_managed
);

SELECT * FROM test_db.test_json_flat_managed
LIMIT 100

-- COMMAND ----------

-- DBTITLE 1,Join tables

-- Create Managed Table to join with
CREATE TABLE IF NOT EXISTS test_db.test_json_managed;
COPY INTO test_db.test_json_managed
FROM '/mnt/my_lake2/data/test_json.json' 
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');
;

--Select from joined tables
SELECT * 
FROM test_db.test_json_flat_managed as tbl1
INNER JOIN test_db.test_json_managed tbl2 ON tbl1.a=tbl2.a

-- COMMAND ----------

-- DBTITLE 1,Union tables
SELECT * 
FROM 
(SELECT a,b,c FROM test_db.test_json_flat_managed)
UNION
(SELECT a,b,c FROM test_db.test_json_managed)
