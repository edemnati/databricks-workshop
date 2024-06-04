# Databricks notebook source
# MAGIC %md 
# MAGIC Dataset: https://github.com/nytimes/covid-19-data
# MAGIC U.S. County-Level Data (Raw CSV): https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv

# COMMAND ----------

# MAGIC %md The objective of this notebook is to illustrate how we might generate a large number of fine-grained forecasts at the state level in an efficient manner leveraging the distributed computational power of Databricks.  This is a Spark 3.x update to a previously published notebook which had been developed for Spark 2.x.  **UPDATE** marks in this notebook indicate changes in the code intended to reflect new functionality in either Spark 3.x or the Databricks platform.

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC For this exercise, we will make use of an increasingly popular library for demand forecasting, [prophet](https://facebook.github.io/prophet/), which we will load into the notebook session using the %pip magic command. 

# COMMAND ----------

# DBTITLE 1,Install Required Libraries
# MAGIC %pip install prophet

# COMMAND ----------

# MAGIC %md ## Step 1: Examine the Data
# MAGIC ### COVID-19 dataset
# MAGIC The dataset contains records for the number of cases of the COVID-19 virus by date in the US, with additional geographical information. The goal is to forecast how many cases of the virus will occur over the next 30 days in the US.

# COMMAND ----------

# MAGIC %md
# MAGIC __Create a unity catalog volume to store files__

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog main;
# MAGIC use schema default;
# MAGIC create volume if not exists myfiles;
# MAGIC show volumes

# COMMAND ----------

# Download file
import urllib

data_file_https_url = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
urllib.request.urlretrieve(data_file_https_url, "/tmp/us-counties.csv")

# Move data with dbutils
dbutils.fs.mv("file:/tmp/us-counties.csv", "/Volumes/main/default/myfiles/us-counties.csv")


# COMMAND ----------

from pyspark.sql.types import *
from pyspark import SparkFiles

# structure of the training data set
train_schema = StructType([
  StructField('date', DateType()),
  StructField('county', StringType()),
  StructField('state', StringType()),
  StructField('fips', IntegerType()),
  StructField('cases', IntegerType()),
  StructField('death', IntegerType())
  ])


df = spark.read.load("/Volumes/main/default/myfiles/us-counties.csv", 
                     format="csv", 
                     header="true", 
                     sep=',', 
                     #inferSchema= True
                     schema=train_schema
                     )
print(df.printSchema())
display(df)


# COMMAND ----------

# make the dataframe queryable as a temporary view
CATALOG_NAME = "main"
SCHEMA_NAME = "default"

# Save as temp view
#df.createOrReplaceTempView(f"covid_train_dataset")

# Save as table in Catalog
df.write.mode("overwrite").saveAsTable(f"{CATALOG_NAME}.{SCHEMA_NAME}.covid_train_dataset")


# COMMAND ----------

# MAGIC %md When performing demand forecasting, we are often interested in general trends and seasonality.  Let's start our exploration by examining the annual trend in unit cases:

# COMMAND ----------

# DBTITLE 1,View Yearly Trends
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   year(date) as year, 
# MAGIC   sum(cases) as sum_cases
# MAGIC FROM covid_train_dataset
# MAGIC GROUP BY year(date)
# MAGIC ORDER BY year;

# COMMAND ----------

# DBTITLE 1,View Monthly Trends
# MAGIC %sql
# MAGIC
# MAGIC SELECT 
# MAGIC   TRUNC(date, 'MM') as month,
# MAGIC   SUM(cases) as sum_cases
# MAGIC FROM covid_train_dataset
# MAGIC GROUP BY TRUNC(date, 'MM')
# MAGIC ORDER BY month;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --DROP TABLE main.default.covid19_cases_by_state;
# MAGIC CREATE TABLE IF NOT EXISTS main.default.covid19_cases_by_state  AS
# MAGIC SELECT 
# MAGIC  DISTINCT
# MAGIC  date,
# MAGIC  state,
# MAGIC  sum(cases) as cases
# MAGIC FROM covid_train_dataset
# MAGIC group by state,date
# MAGIC ORDER BY cases DESC;
# MAGIC
# MAGIC SELECT * FROM main.default.covid19_cases_by_state LIMIT 10;

# COMMAND ----------

# MAGIC %md Now that we are oriented to the basic patterns within our data, let's explore how we might build a forecast.

# COMMAND ----------

# MAGIC %md ## Step 2: Build a Single Forecast
# MAGIC
# MAGIC Before attempting to generate forecasts for individual states, it might be helpful to build a single forecast for no other reason than to orient ourselves to the use of prophet.
# MAGIC
# MAGIC Our first step is to assemble the historical dataset on which we will train the model:

# COMMAND ----------

# DBTITLE 1,Retrieve Data for a Single Item-Store Combination
# query to aggregate data to date (ds) level
sql_statement = '''
  SELECT
    CAST(date as date) as ds,
    cases as y
  FROM main.default.covid19_cases_by_state
  WHERE state='California'
  ORDER BY ds

  '''

# assemble dataset in Pandas dataframe
history_pd = spark.sql(sql_statement).toPandas()

# drop any missing records
history_pd = history_pd.dropna()

# COMMAND ----------

# MAGIC %md Now, we will import the prophet library, but because it can be a bit verbose when in use, we will need to fine-tune the logging settings in our environment:

# COMMAND ----------

# DBTITLE 1,Import Prophet Library
from prophet import Prophet
import logging

# disable informational messages from prophet
logging.getLogger('py4j').setLevel(logging.ERROR)

# COMMAND ----------

# MAGIC %md Based on our review of the data, it looks like we should set our overall growth pattern to linear and enable the evaluation of weekly and yearly seasonal patterns. We might also wish to set our seasonality mode to multiplicative as the seasonal pattern seems to grow with overall growth in cases:

# COMMAND ----------

# DBTITLE 1,Train Prophet Model
# set model parameters
model = Prophet(
  interval_width=0.95,
  growth='linear',
  daily_seasonality=False,
  weekly_seasonality=True,
  yearly_seasonality=True,
  seasonality_mode='multiplicative'
  )

# fit the model to historical data
model.fit(history_pd)

# COMMAND ----------

# MAGIC %md Now that we have a trained model, let's use it to build a 90-day forecast:

# COMMAND ----------

# DBTITLE 1,Build Forecast
# define a dataset including both historical dates & 90-days beyond the last available date
future_pd = model.make_future_dataframe(
  periods=30, 
  freq='d', 
  include_history=True
  )

# predict over the dataset
forecast_pd = model.predict(future_pd)

display(forecast_pd)

# COMMAND ----------

# MAGIC %md How did our model perform? Here we can see the general and seasonal trends in our model presented as graphs:

# COMMAND ----------

# DBTITLE 1,Examine Forecast Components
trends_fig = model.plot_components(forecast_pd)
#display(trends_fig)

# COMMAND ----------

# MAGIC %md And here, we can see how our actual and predicted data line up as well as a forecast for the future, though we will limit our graph to the last year of historical data just to keep it readable:

# COMMAND ----------

# DBTITLE 1,View Historicals vs. Predictions
predict_fig = model.plot( forecast_pd, xlabel='date', ylabel='cases')

# adjust figure to display dates from last year + the 30 day forecast
xlim = predict_fig.axes[0].get_xlim()
new_xlim = ( xlim[1]-(180), xlim[1]-30)
predict_fig.axes[0]#.set_xlim(new_xlim)


# COMMAND ----------

# MAGIC %md **NOTE** This visualization is a bit busy. Bartosz Mikulski provides [an excellent breakdown](https://www.mikulskibartosz.name/prophet-plot-explained/) of it that is well worth checking out.  In a nutshell, the black dots represent our actuals with the darker blue line representing our predictions and the lighter blue band representing our (95%) uncertainty interval.

# COMMAND ----------

# MAGIC %md Visual inspection is useful, but a better way to evaluate the forecast is to calculate Mean Absolute Error, Mean Squared Error and Root Mean Squared Error values for the predicted relative to the actual values in our set:
# MAGIC
# MAGIC **UPDATE** A change in pandas functionality requires us to use *pd.to_datetime* to coerce the date string into the right data type.

# COMMAND ----------

# DBTITLE 1,Calculate Evaluation metrics
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error
from math import sqrt
from datetime import date

# get historical actuals & predictions for comparison
actuals_pd = history_pd[ history_pd['ds'] < date(2022, 5, 14) ]['y']
predicted_pd = forecast_pd[ forecast_pd['ds'] < pd.to_datetime('2022-05-14') ]['yhat']

# calculate evaluation metrics
mae = mean_absolute_error(actuals_pd, predicted_pd)
mse = mean_squared_error(actuals_pd, predicted_pd)
rmse = sqrt(mse)

# print metrics to the screen
print( '\n'.join(['MAE: {0}', 'MSE: {1}', 'RMSE: {2}']).format(mae, mse, rmse) )

# COMMAND ----------

# MAGIC %md prophet provides [additional means](https://facebook.github.io/prophet/docs/diagnostics.html) for evaluating how your forecasts hold up over time. You're strongly encouraged to consider using these and those additional techniques when building your forecast models but we'll skip this here to focus on the scaling challenge.

# COMMAND ----------

# MAGIC %md ## Step 3: Scale Forecast Generation
# MAGIC
# MAGIC With the mechanics under our belt, let's now tackle our original goal of building numerous, fine-grain models & forecasts for individual states.  We will start by assembling cases data at the state-date level of granularity:
# MAGIC
# MAGIC **NOTE**: The data in this data set should already be aggregated at this level of granularity but we are explicitly aggregating to ensure we have the expected data structure.

# COMMAND ----------

sc.defaultParallelism

# COMMAND ----------

# DBTITLE 1,Retrieve Data for All Store-Item Combinations
sql_statement = '''
  SELECT
    state,
    CAST(date as date) as ds,
    SUM(cases) as y
  FROM main.default.covid19_cases_by_state
  GROUP BY state, ds
  ORDER BY state, ds
  '''

state_history = (
  spark
    .sql( sql_statement )
    .repartition(sc.defaultParallelism, ['state'])
  ).cache()

# COMMAND ----------

# MAGIC %md With our data aggregated at the state-date level, we need to consider how we will pass our data to prophet. If our goal is to build a model for each state, we will need to pass in a state subset from the dataset we just assembled, train a model on that subset, and receive a state forecast back. We'd expect that forecast to be returned as a dataset with a structure like this where we retain the state identifier for which the forecast was assembled and we limit the output to just the relevant subset of fields generated by the Prophet model:

# COMMAND ----------

# DBTITLE 1,Define Schema for Forecast Output
from pyspark.sql.types import *

result_schema =StructType([
  StructField('ds',DateType()),
  StructField('state',StringType()),
  StructField('y',FloatType()),
  StructField('yhat',FloatType()),
  StructField('yhat_upper',FloatType()),
  StructField('yhat_lower',FloatType())
  ])

# COMMAND ----------

# MAGIC %md To train the model and generate a forecast we will leverage a Pandas function.  We will define this function to receive a subset of data organized around a state.  It will return a forecast in the format identified in the previous cell:
# MAGIC
# MAGIC **UPDATE** With Spark 3.0, pandas functions replace the functionality found in pandas UDFs.  The deprecated pandas UDF syntax is still supported but will be phased out over time.  For more information on the new, streamlined pandas functions API, please refer to [this document](https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html).

# COMMAND ----------

# DBTITLE 1,Define Function to Train Model & Generate Forecast
def forecast_state( history_pd: pd.DataFrame ) -> pd.DataFrame:
  
  # TRAIN MODEL AS BEFORE
  # --------------------------------------
  # remove missing values (more likely at day-state level)
  history_pd = history_pd.dropna()
  
  # configure the model
  model = Prophet(
    interval_width=0.95,
    growth='linear',
    daily_seasonality=False,
    weekly_seasonality=True,
    yearly_seasonality=True,
    seasonality_mode='multiplicative'
    )
  
  # train the model
  model.fit( history_pd )
  # --------------------------------------
  
  # BUILD FORECAST AS BEFORE
  # --------------------------------------
  # make predictions
  future_pd = model.make_future_dataframe(
    periods=90, 
    freq='d', 
    include_history=True
    )
  forecast_pd = model.predict( future_pd )  
  # --------------------------------------
  
  # ASSEMBLE EXPECTED RESULT SET
  # --------------------------------------
  # get relevant fields from forecast
  f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
  
  # get relevant fields from history
  h_pd = history_pd[['ds','state','y']].set_index('ds')
  
  # join history and forecast
  results_pd = f_pd.join( h_pd, how='left' )
  results_pd.reset_index(level=0, inplace=True)
  
  # get state from incoming data set
  results_pd['state'] = history_pd['state'].iloc[0]
  # --------------------------------------
  
  # return expected dataset
  return results_pd[ ['ds', 'state', 'y', 'yhat', 'yhat_upper', 'yhat_lower'] ]  

# COMMAND ----------

# MAGIC %md There's a lot taking place within our function, but if you compare the first two blocks of code within which the model is being trained and a forecast is being built to the cells in the previous portion of this notebook, you'll see the code is pretty much the same as before. It's only in the assembly of the required result set that truly new code is being introduced and it consists of fairly standard Pandas dataframe manipulations.

# COMMAND ----------

# MAGIC %md Now let's call our pandas function to build our forecasts.  We do this by grouping our historical dataset around state.  We then apply our function to each group and tack on today's date as our *training_date* for data management purposes:
# MAGIC
# MAGIC **UPDATE** Per the previous update note, we are now using applyInPandas() to call a pandas function instead of a pandas UDF.

# COMMAND ----------

# DBTITLE 1,Apply Forecast Function to Each Store-Item Combination
from pyspark.sql.functions import current_date

results = (
  state_history
    .groupBy('state')
      .applyInPandas(forecast_state, schema=result_schema)
    .withColumn('training_date', current_date() )
    )

results.createOrReplaceTempView('new_forecasts')

display(results)

# COMMAND ----------

# MAGIC %md We we are likely wanting to report on our forecasts, so let's save them to a queryable table structure:

# COMMAND ----------

# DBTITLE 1,Persist Forecast Output
# MAGIC %sql
# MAGIC -- create forecast table
# MAGIC drop table main.default.covid19_state_forecasts;
# MAGIC create table if not exists main.default.covid19_state_forecasts (
# MAGIC   date date,
# MAGIC   state string,
# MAGIC   cases float,
# MAGIC   cases_predicted float,
# MAGIC   cases_predicted_upper float,
# MAGIC   cases_predicted_lower float,
# MAGIC   training_date date
# MAGIC   )
# MAGIC using delta
# MAGIC partitioned by (date);
# MAGIC
# MAGIC -- load data to it
# MAGIC merge into main.default.covid19_state_forecasts f
# MAGIC using new_forecasts n 
# MAGIC on f.date = n.ds and f.state = n.state
# MAGIC when matched then update set f.date = n.ds,
# MAGIC   f.state = n.state,
# MAGIC   f.cases = n.y,
# MAGIC   f.cases_predicted = n.yhat,
# MAGIC   f.cases_predicted_upper = n.yhat_upper,
# MAGIC   f.cases_predicted_lower = n.yhat_lower,
# MAGIC   f.training_date = n.training_date
# MAGIC when not matched then insert (date,
# MAGIC   state,
# MAGIC   cases,
# MAGIC   cases_predicted,
# MAGIC   cases_predicted_upper,
# MAGIC   cases_predicted_lower,
# MAGIC   training_date)
# MAGIC values (n.ds,
# MAGIC   n.state,
# MAGIC   n.y,
# MAGIC   n.yhat,
# MAGIC   n.yhat_upper,
# MAGIC   n.yhat_lower,
# MAGIC   n.training_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from main.default.covid19_state_forecasts limit 10;

# COMMAND ----------

# MAGIC %md But how good (or bad) is each forecast?  Using the pandas function technique, we can generate evaluation metrics for each state forecast as follows:

# COMMAND ----------

# DBTITLE 1,Apply Same Techniques to Evaluate Each Forecast
# schema of expected result set
eval_schema =StructType([
  StructField('training_date', DateType()),
  StructField('state', StringType()),
  StructField('mae', FloatType()),
  StructField('mse', FloatType()),
  StructField('rmse', FloatType())
  ])

# define function to calculate metrics
def evaluate_forecast( evaluation_pd: pd.DataFrame ) -> pd.DataFrame:
  
  # get state in incoming data set
  training_date = evaluation_pd['training_date'].iloc[0]
  state = evaluation_pd['state'].iloc[0]
  
  # calculate evaluation metrics
  mae = mean_absolute_error( evaluation_pd['cases'], evaluation_pd['cases_predicted'] )
  mse = mean_squared_error( evaluation_pd['cases'], evaluation_pd['cases_predicted'] )
  rmse = sqrt( mse )
  
  # assemble result set
  results = {'training_date':[training_date], 'state':[state], 'mae':[mae], 'mse':[mse], 'rmse':[rmse]}
  return pd.DataFrame.from_dict( results )

# calculate metrics
results = (
  spark
    .table('main.default.covid19_state_forecasts')
    .filter('date < \'2022-05-14\'') # limit evaluation to periods where we have historical data
    .select('training_date', 'state', 'cases', 'cases_predicted')
    .groupBy('training_date', 'state')
    .applyInPandas(evaluate_forecast, schema=eval_schema)
    )

results.createOrReplaceTempView('new_forecast_evals')

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from new_forecast_evals
# MAGIC limit 10;
# MAGIC

# COMMAND ----------

# MAGIC %md Once again, we will likely want to report the metrics for each forecast, so we persist these to a queryable table:

# COMMAND ----------

# DBTITLE 1,Persist Evaluation Metrics
# MAGIC %sql
# MAGIC
# MAGIC create table if not exists main.default.covid19_forecast_evals (
# MAGIC   state string,
# MAGIC   mae float,
# MAGIC   mse float,
# MAGIC   rmse float,
# MAGIC   training_date date
# MAGIC   )
# MAGIC using delta
# MAGIC partitioned by (training_date);
# MAGIC
# MAGIC insert into main.default.covid19_forecast_evals
# MAGIC select
# MAGIC   state,
# MAGIC   mae,
# MAGIC   mse,
# MAGIC   rmse,
# MAGIC   training_date
# MAGIC from new_forecast_evals;

# COMMAND ----------

# MAGIC %md We now have constructed a forecast for each state and generated basic evaluation metrics for each:

# COMMAND ----------

# DBTITLE 1,Visualize Forecasts
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   state,
# MAGIC   date,
# MAGIC   cases_predicted,
# MAGIC   cases_predicted_upper,
# MAGIC   cases_predicted_lower
# MAGIC FROM main.default.covid19_state_forecasts a
# MAGIC WHERE training_date=current_date() AND state='California'
# MAGIC ORDER BY state

# COMMAND ----------

# MAGIC %md And for each of these, we can retrieve a measure of help us assess the reliability of each forecast:

# COMMAND ----------

# DBTITLE 1,Retrieve Evaluation Metrics
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   state,
# MAGIC   mae,
# MAGIC   mse,
# MAGIC   rmse
# MAGIC FROM main.default.covid19_forecast_evals a
# MAGIC WHERE training_date=current_date() AND state='California'
# MAGIC ORDER BY state
