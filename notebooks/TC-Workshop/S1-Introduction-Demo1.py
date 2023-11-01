# Databricks notebook source
# MAGIC %md
# MAGIC # Introduction to Databricks
# MAGIC
# MAGIC Databricck Moden Ar
# MAGIC ![](https://learn.microsoft.com/en-us/azure/databricks/scenarios/media/what-is-azure-databricks/azure-databricks-overview.png)
# MAGIC
# MAGIC
# MAGIC
# MAGIC __What is Databricks__
# MAGIC _"The Azure Databricks Lakehouse Platform provides a unified set of tools for building, deploying, sharing, and maintaining enterprise-grade data solutions at scale. Azure Databricks integrates with cloud storage and security in your cloud account, and manages and deploys cloud infrastructure on your behalf."_
# MAGIC
# MAGIC __What are common use cases for Azure Databricks?__
# MAGIC   - Build an enterprise data lakehouse
# MAGIC 	  - The Databricks Lakehouse combines the ACID transactions and data governance of data warehouses with the flexibility and cost-efficiency of data lakes to enable business intelligence (BI) and machine learning (ML) on all data. 
# MAGIC 	  - ACID stands for atomicity, consistency, isolation, and durability.
# MAGIC 		  -  Atomicity means that all transactions either succeed or fail completely.
# MAGIC 		  - Consistency guarantees relate to how a given state of the data is observed by simultaneous operations.
# MAGIC 		  - Isolation refers to how simultaneous operations potentially conflict with one another.
# MAGIC 		  - Durability means that committed changes are permanent.
# MAGIC 		
# MAGIC   - ETL and data engineering
# MAGIC   - Machine learning, AI, and data science
# MAGIC   - Data warehousing, analytics, and BI
# MAGIC   - Data governance and secure data sharing
# MAGIC   - DevOps, CI/CD, and task orchestration
# MAGIC   - Real-time and streaming analytics

# COMMAND ----------

# MAGIC %md
# MAGIC # Demo summary
# MAGIC
# MAGIC _pyspark API reference: https://spark.apache.org/docs/latest/api/python/reference/index.html_
# MAGIC
# MAGIC __Read data__
# MAGIC
# MAGIC 1. Pandas and spark dataframes
# MAGIC     - Read data with pandas
# MAGIC     - Pandas dataframe to spark dataframe
# MAGIC     - Spark dataframe to pandas dataframe 
# MAGIC     - Leverage Spark pandas API
# MAGIC 1. Explore sample datasets
# MAGIC 1. Read Table to DataFrame
# MAGIC 1. Read CSV Files
# MAGIC     - Handle bad records
# MAGIC     - Explicit schema definition
# MAGIC 1. Read json files  
# MAGIC 1. Read parquet files
# MAGIC
# MAGIC __Explore Data__
# MAGIC
# MAGIC 1. Explore Dataframe
# MAGIC     - Schema
# MAGIC     - Describe variables
# MAGIC     - Data profiling
# MAGIC 1. Query Dataframes
# MAGIC     - Select Variables
# MAGIC     - Filter rows based on conditions
# MAGIC     - Count observations per group
# MAGIC     - Group by variable and apply aggregation functions
# MAGIC     - Get variable list of distinct values
# MAGIC     - Explode nested data
# MAGIC     - Select observation using expression
# MAGIC     - Select observation using sql query
# MAGIC     
# MAGIC __Save data__
# MAGIC
# MAGIC 1. Save to table
# MAGIC 1. Save to file
# MAGIC
# MAGIC __Explore data using SQL queries__
# MAGIC
# MAGIC 1. Create test database
# MAGIC 1. Read file to table
# MAGIC 1. Query table

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Read Data
# MAGIC #### Pandas and spark dataframes

# COMMAND ----------

# DBTITLE 1,Read data with pandas
import pandas as pd

pdf = pd.read_csv("/dbfs/FileStore/datasets/daily_bike_share.csv")
pdf.head()


# COMMAND ----------

pdf.windspeed.hist()

# COMMAND ----------

# DBTITLE 1,Pandas dataframe to spark dataframe
df1 = spark.createDataFrame(pdf)
display(df1)
print(f"count rows:{df1.count()}")

# COMMAND ----------

# DBTITLE 1,Spark dataframe to pandas dataframe 
pdf2 = df1.toPandas()
pdf2.head()

# COMMAND ----------

# DBTITLE 1,Leverage Spark pandas API
import pyspark.pandas as ps

ps_df = ps.read_csv("dbfs:/FileStore/datasets/daily_bike_share.csv")
ps_df.head()


# COMMAND ----------

ps_df.windspeed.hist()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Explore sample datasets
# MAGIC Ref: https://learn.microsoft.com/en-us/azure/databricks/dbfs/databricks-datasets

# COMMAND ----------

display(dbutils.fs.ls('/databricks-datasets'))

# COMMAND ----------

# MAGIC %sql
# MAGIC --SHOW SCHEMAS IN samples
# MAGIC SHOW TABLES IN samples.tpch

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Read  Table to DataFrame
# MAGIC
# MAGIC Azure Databricks uses Delta Lake for all tables by default. You can easily load tables to DataFrames, such as in the following example:

# COMMAND ----------

#spark.read.table("<catalog_name>.<schema_name>.<table_name>")
df = spark.read.table("samples.tpch.orders")

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read CSV Files

# COMMAND ----------

#Read data
"""
mode:
   - PERMISSIVE (default): nulls are inserted for fields that could not be parsed correctly
   - DROPMALFORMED: drops lines that contain fields that could not be parsed
   - FAILFAST: aborts the reading if any malformed data is found

In the PERMISSIVE mode it is possible to inspect the rows that could not be parsed correctly using one of the following methods:
   - You can provide a custom path to the option badRecordsPath to record corrupt records to a file.
   - You can add the column _corrupt_record to the schema provided to the DataFrameReader to review corrupt records in the resultant DataFrame.

"""
test_df = (spark.read
              .format("csv")
              .options(header='true', inferSchema='true')
              #.option("mode", "PERMISSIVE")
              .option("badRecordsPath", "/FileStore/tables/badRecordsPath")
              .load("dbfs:/FileStore/datasets/daily_bike_share_corrupted.csv")
              #.load("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv")
              )
display(test_df)

# COMMAND ----------

# DBTITLE 1,Read Bad records

df_bad = (spark.read
          .format("json")
          .option("recursiveFileLookup", "true")
          .load("dbfs:/FileStore/tables/badRecordsPath")
         )
df_bad.display()

# COMMAND ----------

# DBTITLE 1,Explicit schema definition
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType, StructField

myschema = StructType(
     [
       StructField('day', IntegerType(), True), 
       StructField('mnth', IntegerType(), True), 
       StructField('year', IntegerType(), True), 
       StructField('season', IntegerType(), True), 
       StructField('holiday', IntegerType(), True), 
       StructField('weekday', IntegerType(), True), 
       StructField('workingday', IntegerType(), True), 
       StructField('weathersit', IntegerType(), True), 
       StructField('temp', DoubleType(), True), 
       StructField('atemp', DoubleType(), True), 
       StructField('hum', DoubleType(), True), 
       StructField('windspeed', DoubleType(), True), 
       StructField('rentals', IntegerType(), True)
     ]
)

test_df2 = (spark.read
              .format("csv")
              .schema(myschema)
              .options(header='true', inferSchema='true')
              .option("mode", "DROPMALFORMED")
              .load("dbfs:/FileStore/datasets/daily_bike_share_corrupted.csv")              
              )
display(test_df2)

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Read json files

# COMMAND ----------


import pyspark.sql.functions as f
df_json = (spark.read.format("json")
           #.load("dbfs:/FileStore/datasets/test_json.json")
           #.option("multiline", "true") #set to true for multiline json files, set to false otherwise
           #.load("dbfs:/FileStore/datasets/test_json_array.json")
           
           .load("dbfs:/FileStore/datasets/test_json_array_nested.json")
           #.select("*",f.explode("d"))
           #.drop("d")
           #.select("*","col.*")
           #drop("col")           
          )
df_json.display()
#df_json.select(("calEvent.*")).display()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ### Read parquet files

# COMMAND ----------

df_parquet = (spark.read
               .parquet("dbfs:/FileStore/datasets/NYCTripSmall.parquet")
              )
df_parquet.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore data
# MAGIC ### Explore Dataframe

# COMMAND ----------

test_df.printSchema()
print(F"count: {test_df.count()}")
test_df.show(100, truncate=False) #show more lines, do not truncate
test_df.display()


# COMMAND ----------

# DBTITLE 1,Describe variables
test_df.summary().display()
#test_df.summary("count", "min", "25%", "75%", "max").display()

# COMMAND ----------

# DBTITLE 1,Data profiling
display(test_df)
#dbutils.data.summarize(test_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query Dataframes

# COMMAND ----------

# DBTITLE 1,Select variables
#Select columns
(test_df.select( "season","holiday")
 .distinct() #remove duplicates
 .display())


# COMMAND ----------

# DBTITLE 1,Filter rows based on conditions
test_df.filter(test_df.windspeed<0.19).count()
#or
test_df.filter("windspeed<0.19").count()

# COMMAND ----------

# DBTITLE 1,Count observations per group
# Count rows by group
(test_df.groupBy("holiday")
 .count() #count
 .show())


# COMMAND ----------

# DBTITLE 1,Group by variable and apply aggregation functions
import pyspark.sql.functions as f

#Group and agggregate
display(test_df.groupBy("mnth")
               .agg(f.mean("windspeed").alias("avg_windspeed"),
                    f.median("windspeed").alias("med_windspeed"),
                    f.sum("rentals").alias("sum_rentals")
                   )
               .sort(f.desc("avg_windspeed"))
        )





# COMMAND ----------

# DBTITLE 1,Get variable list of distinct values
import pyspark.sql.functions as f
# collect all distinct values
test_df.select(f.array_distinct(f.collect_list("mnth"))).display()


# COMMAND ----------

# DBTITLE 1,Explode nested data
#Work with nested structures
from pyspark.sql import Row
import pyspark.sql.functions as f


df_nested = spark.read.json("dbfs:/FileStore/datasets/test_json_array_nested.json")
df_nested.show()
df_nested.printSchema()
df_nested_flattened = (df_nested
 .withColumn("exploded_d",f.explode("d"))
 .select("a","b","c","exploded_d.*")
 
)
display(df_nested_flattened)

# COMMAND ----------

# DBTITLE 1,Select observation using expression
display(df_nested_flattened.selectExpr("a", "upper(Country) as UPP_Country"))
#display(df_nested_flattened.select("a", f.expr("upper(Country) as UPP_Country")))

# COMMAND ----------

# DBTITLE 1,Select observation using sql query
(df_nested_flattened.write
 #.mode("overwrite")
 .saveAsTable("test_table") 
)

query_df = spark.sql("SELECT * FROM test_table")

display(query_df)

# COMMAND ----------

df2 = spark.read.json("dbfs:/FileStore/datasets/test_json.json")
display(df2)

# COMMAND ----------

# DBTITLE 1,Join datasets

joined_df = df_nested_flattened.join(df2, how="inner", on="a")
display(joined_df)




# COMMAND ----------

unioned_df = (df_nested_flattened.select("a","b","c")
              .union(df2.select("a","b","c"))
              #.distinct()
             )
display(unioned_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save data

# COMMAND ----------

# DBTITLE 1,Save to table
#mode: append | overwrite

test_df.write.mode("overwrite").saveAsTable("test_db.test_df")

# COMMAND ----------

# DBTITLE 1,Save to file
test_df.write.format("json").save("/tmp/test_df.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore data using SQL queries

# COMMAND ----------

# DBTITLE 1,Create test database
# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS test_db

# COMMAND ----------

# DBTITLE 1,Read file to table
# MAGIC %sql
# MAGIC -- mode "FAILFAST" will abort file parsing with a RuntimeException if any malformed lines are encountered
# MAGIC --CREATE TEMPORARY VIEW diamonds
# MAGIC CREATE TABLE IF NOT EXISTS test_db.diamonds
# MAGIC USING CSV
# MAGIC OPTIONS (path "/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv", header "true", mode "FAILFAST")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC describe extended test_db.diamonds

# COMMAND ----------

# DBTITLE 1,Query table
# MAGIC %sql 
# MAGIC SELECT * FROM test_db.diamonds
