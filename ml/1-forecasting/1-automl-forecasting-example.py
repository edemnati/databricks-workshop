# Databricks notebook source
# MAGIC %md
# MAGIC # AutoML forecasting example
# MAGIC ## Requirements
# MAGIC Databricks Runtime for Machine Learning 10.0 or above.  
# MAGIC To save model predictions, Databricks Runtime for Machine Learning 10.5 or above.
# MAGIC
# MAGIC Ref: https://docs.databricks.com/en/_extras/notebooks/source/machine-learning/automl-forecasting-example.html

# COMMAND ----------

# MAGIC %md
# MAGIC ## COVID-19 dataset
# MAGIC The dataset contains records for the number of cases of the COVID-19 virus by date in the US, with additional geographical information. The goal is to forecast how many cases of the virus will occur over the next 30 days in the US.

# COMMAND ----------

import os

os.getcwd()

# COMMAND ----------



import pyspark.pandas as ps
df = ps.read_csv("file:///Workspace/Repos/ezzatdemnati@microsoft.com/databricks-workshop/datasets/covid19_us-counties.csv")
df.to_csv("dbfs:/FileStore/test.csv")
df["date"] = ps.to_datetime(df['date'], errors='coerce')
df["cases"] = df["cases"].astype(int)
print(f"len:{len(df)}")
display(df)

# COMMAND ----------

"""import pyspark.pandas as ps
df = ps.read_csv("/databricks-datasets/COVID/covid-19-data")
df["date"] = ps.to_datetime(df['date'], errors='coerce')
df["cases"] = df["cases"].astype(int)
len(df)
"""

# COMMAND ----------


print(f"len county:{len(df[['county']].drop_duplicates())   }")
print(f"len state:{len(df[['state']].drop_duplicates())}")
df[["county","state"]].drop_duplicates().head()

# COMMAND ----------

import matplotlib.pyplot as plt
import pandas as pd

df_true = df.groupby("date").agg(y=("cases", "avg")).reset_index().to_pandas().sort_values("date")

fig = plt.figure(facecolor='w', figsize=(10, 6))
plt.plot(df_true.date, df_true.y)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoML training
# MAGIC The following command starts an AutoML run. You must provide the column that the model should predict in the `target_col` argument and the time column. 
# MAGIC When the run completes, you can follow the link to the best trial notebook to examine the training code.  
# MAGIC
# MAGIC This example also specifies:
# MAGIC - `horizon=30` to specify that AutoML should forecast 30 days into the future. 
# MAGIC - `frequency="d"` to specify that a forecast should be provided for each day. 
# MAGIC - `primary_metric="mdape"` to specify the metric to optimize for during training.

# COMMAND ----------

# MAGIC %md
# MAGIC __Time series aggregation (Default)__
# MAGIC
# MAGIC For forecasting problems, when there are multiple values for a timestamp in a time series, AutoML uses the average of the values.
# MAGIC
# MAGIC ref: https://learn.microsoft.com/en-us/azure/databricks/machine-learning/automl/how-automl-works

# COMMAND ----------

import pyspark.pandas as ps
from sklearn.ensemble import RandomForestRegressor
import mlflow 


# Write a table to Unity Catalog
df.to_table("main.default.covid19_ts_dataset", mode="overwrite")

# Load a Unity Catalog table, train a model, and log the input table
dataset = mlflow.data.load_delta(table_name="main.default.covid19_ts_dataset", version="0")

pd_df = dataset.df.toPandas()

# COMMAND ----------

import databricks.automl
import logging

# Disable informational messages from fbprophet
logging.getLogger("py4j").setLevel(logging.WARNING)

# Note: If you are running Databricks Runtime for Machine Learning 10.4 or below, use this line instead:
# summary = databricks.automl.forecast(df, target_col="cases", time_col="date", horizon=30, frequency="d",  primary_metric="mdape")

#with mlflow.start_run(run_name='covid_forecast_run'):
summary = databricks.automl.forecast(pd_df, target_col="cases", time_col="date", horizon=30, frequency="d",  primary_metric="mdape", output_database="default")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC * Explore the notebooks and experiments linked above.
# MAGIC * If the metrics for the best trial notebook look good, you can continue with the next cell.
# MAGIC * If you want to improve on the model generated by the best trial:
# MAGIC   * Go to the notebook with the best trial and clone it.
# MAGIC   * Edit the notebook as necessary to improve the model.
# MAGIC   * When you are satisfied with the model, note the URI where the artifact for the trained model is logged. Assign this URI to the `model_uri` variable in the next cell.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show the predicted results from the best model
# MAGIC **Note:** This section requires Databricks Runtime for Machine Learning 10.5 or above.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load predictions from the best model
# MAGIC In Databricks Runtime for Machine Learning 10.5 or above, if `output_database` is provided, AutoML saves the predictions from the best model.  

# COMMAND ----------

# Load the saved predictions.
forecast_pd = spark.table(summary.output_table_name)
display(forecast_pd.orderBy("date"))

# COMMAND ----------

# MAGIC %md ## Use the model for forecasting
# MAGIC You can use the commands in this section with Databricks Runtime for Machine Learning 10.0 or above.

# COMMAND ----------

# MAGIC %md ### Load the model with MLflow
# MAGIC MLflow allows you to easily import models back into Python by using the AutoML `trial_id` .

# COMMAND ----------

import mlflow.pyfunc
from mlflow.tracking import MlflowClient

run_id = MlflowClient()
trial_id = summary.best_trial.mlflow_run_id

model_uri = "runs:/{run_id}/model".format(run_id=trial_id)
pyfunc_model = mlflow.pyfunc.load_model(model_uri)

# COMMAND ----------

# MAGIC %md ### Use the model to make forecasts
# MAGIC
# MAGIC Call the `predict_timeseries` model method to generate forecasts.    
# MAGIC In Databricks Runtime for Machine Learning 10.5 or above, you can set `include_history=False` to get the predicted data only.

# COMMAND ----------

forecasts = pyfunc_model._model_impl.python_model.predict_timeseries()
display(forecasts.sort_values("ds"))

# Option for Databricks Runtime for Machine Learning 10.5 or above
# forecasts = pyfunc_model._model_impl.python_model.predict_timeseries(include_history=False)

# COMMAND ----------

# MAGIC %md ### Plot the forecasted points
# MAGIC In the plot below, the thick black line shows the time series dataset, and the blue line is the forecast created by the model.
# MAGIC

# COMMAND ----------

df_true = df.groupby("date").agg(y=("cases", "avg")).reset_index().to_pandas()

# COMMAND ----------

import matplotlib.pyplot as plt

fig = plt.figure(facecolor='w', figsize=(10, 6))
ax = fig.add_subplot(111)
forecasts = pyfunc_model._model_impl.python_model.predict_timeseries(include_history=True)
fcst_t = forecasts['ds'].dt.to_pydatetime()
ax.plot(df_true['date'].dt.to_pydatetime(), df_true['y'], 'k.', label='Observed data points')
ax.plot(fcst_t, forecasts['yhat'], ls='-', c='#0072B2', label='Forecasts')
ax.fill_between(fcst_t, forecasts['yhat_lower'], forecasts['yhat_upper'],
                color='#0072B2', alpha=0.2, label='Uncertainty interval')
ax.legend()
plt.show()

# COMMAND ----------

import mlflow

mlflow.set_registry_uri("databricks-uc") # Not required if using Databricks Runtime 13.3 LTS or above
CATALOG_NAME = "main"
SCHEMA_NAME = "default"

# COMMAND ----------

# MAGIC %md #### Register the model to Unity Catalog
# MAGIC
# MAGIC By registering this model to Unity Catalog, you can easily reference the model from anywhere within Databricks.
# MAGIC
# MAGIC The following section shows how to do this programmatically, but you can also register a model using the UI.

# COMMAND ----------

mlflow.last_active_run()

# COMMAND ----------

# download mode artifacts
#mlflow.artifacts.download_artifacts(run_id=trial_id)

# COMMAND ----------

import time

# Register the model to Unity Catalog. 
model_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.covid_forecast"
print(f"model_uri:{model_uri}")
print(f"model_name:{model_name}")
model_version = mlflow.register_model(model_uri, model_name)

# Registering the model takes a few seconds, so add a small delay
time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC Next, assign this model the "Champion" tag, and load it into this notebook from Unity Catalog.

# COMMAND ----------

from mlflow.tracking import MlflowClient

client = MlflowClient()
client.set_registered_model_alias(model_name, "Champion", model_version.version)

# COMMAND ----------

# MAGIC %md In Unity Catalog, the model version now has the tag "Champion". 
# MAGIC
# MAGIC You can now refer to the model using the path "models:/{model_name}@Champion".

# COMMAND ----------

from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error

# Load model
model = mlflow.pyfunc.load_model(f"models:/{model_name}@Champion")


