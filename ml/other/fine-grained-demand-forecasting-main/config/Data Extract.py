# Databricks notebook source
# MAGIC %md The purpose of this notebook is to download and set up the data we will use for the solution accelerator. Before running this notebook, make sure you have entered your own credentials for Kaggle.

# COMMAND ----------

# MAGIC %md Move the downloaded data to the folder used throughout the accelerator:

# COMMAND ----------

# MAGIC %sh pip install delta-sharing

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/")

# COMMAND ----------

# MAGIC %scala
# MAGIC dbutils.fs.put("/share/config.share","""
# MAGIC {"shareCredentialsVersion":1,"bearerToken":"wMzvMbAi-7pGOmmbhCaqn-RPq80LBVaZTb-Kj3crOAAzrrb80RzBHFYY3g8DCrfG","endpoint":"https://eastus-c3.azuredatabricks.net/api/2.0/delta-sharing/metastores/6d7623d5-2d9d-4103-abdf-f8b6a88541b4","expirationTime":"2024-06-10T21:38:41.062Z"}
# MAGIC """,overwrite=true)

# COMMAND ----------

import delta_sharing

client = delta_sharing.SharingClient(f"/dbfs/share/config.share")

client.list_all_tables()

# COMMAND ----------

#f"<profile-path>#<share-name>.<schema-name>.<table-name>"
#share_path = "/share/config.share#demand-forecasting-train-dataset.default.demande_forecast_train"
#delta_sharing.load_as_spark(share_path, version=1)

(spark.read.format("deltaSharing")
.option("versionAsOf", 1)
.load("/share/config.share#demand-forecasting-train-dataset.default.demande_forecast_train")
.limit(10)
).show()



# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS demande_forecast_train_shared;
# MAGIC
# MAGIC CREATE TABLE demande_forecast_train_shared USING deltaSharing LOCATION "dbfs:/share/config.share#demand-forecasting-train-dataset.default.demande_forecast_train";
# MAGIC
# MAGIC SELECT * FROM demande_forecast_train_shared LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Set up user-scoped database location to avoid conflicts
import re
from pathlib import Path
# Creating user-specific paths and database names
useremail = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
username_sql_compatible = re.sub('\W', '_', useremail.split('@')[0])
tmp_data_path = f"/tmp/fine_grain_forecast/data/{useremail}/"
database_name = f"fine_grain_forecast_{username_sql_compatible}"

# Create user-scoped environment
spark.sql(f"DROP DATABASE IF EXISTS {database_name} CASCADE")
spark.sql(f"CREATE DATABASE {database_name} LOCATION '{tmp_data_path}'")
spark.sql(f"USE {database_name}")
Path(tmp_data_path).mkdir(parents=True, exist_ok=True)
