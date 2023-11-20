# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Delta Live Tables
# MAGIC %md
# MAGIC
# MAGIC Delta Live Tables (DLT) makes it easy to build and manage reliable data pipelines that deliver high quality data on Delta Lake. 
# MAGIC
# MAGIC DLT helps data engineering teams simplify ETL development and management with declarative pipeline development, automatic data testing, and deep visibility for monitoring and recovery.
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2021/09/Live-Tables-Pipeline.png" width=1012/>
# MAGIC
# MAGIC __Demo steps__
# MAGIC   1. Explore Delta Live Table pipeline UI
# MAGIC   1. Run SQL dlt pipeline
# MAGIC   1. Run python dlt pipeline
# MAGIC   1. Analyse dlt pipeline event logs
# MAGIC       - Audit logging
# MAGIC       - Lineage
# MAGIC       - Data quality
# MAGIC       - Backlog metrics
# MAGIC       - Databricks Enhanced Autoscaling events
# MAGIC       - Runtime information

# COMMAND ----------

# Creates the input box at the top of the notebook.
dbutils.widgets.text('pipeline_id', 'f28d2e0e-8508-4ca2-864c-837a54fde3af', 'Pipeline ID')
#dbutils.widgets.removeAll()

# COMMAND ----------

# DBTITLE 1,Read dlt pipeline event logs
#Ref: https://learn.microsoft.com/en-us/azure/databricks/workflows/delta-live-tables/delta-live-tables-event-log

event_log_path = f"dbfs:/pipelines/{dbutils.widgets.get('pipeline_id')}/system/events"
event_log_raw = spark.read.format('delta').load(event_log_path)
event_log_raw.createOrReplaceTempView("event_log_raw")
display(event_log_raw)

# COMMAND ----------


latest_update_id = spark.sql("SELECT origin.update_id FROM event_log_raw WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1").collect()[0].update_id
print(f"latest_update_id:{latest_update_id}")

spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# MAGIC %sql
# MAGIC select '${latest_update.id}' as latest_update_id
# MAGIC

# COMMAND ----------

# DBTITLE 1,Audit log
# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name ,origin.update_id
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'user_action'
# MAGIC --and origin.update_id = '${latest_update.id}'
# MAGIC order by timestamp desc
# MAGIC

# COMMAND ----------

# DBTITLE 1,Lineage
# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
# MAGIC FROM event_log_raw WHERE event_type = 'flow_definition' AND origin.update_id = '${latest_update.id}'

# COMMAND ----------

# DBTITLE 1,Data quality
# MAGIC %sql
# MAGIC SELECT
# MAGIC   row_expectations.dataset as dataset,
# MAGIC   row_expectations.name as expectation,
# MAGIC   SUM(row_expectations.passed_records) as passing_records,
# MAGIC   SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       explode(
# MAGIC         from_json(
# MAGIC           details :flow_progress :data_quality :expectations,
# MAGIC           "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>"
# MAGIC         )
# MAGIC       ) row_expectations
# MAGIC     FROM
# MAGIC       event_log_raw
# MAGIC     WHERE
# MAGIC       event_type = 'flow_progress'
# MAGIC       AND origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   row_expectations.dataset,
# MAGIC   row_expectations.name

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from dlt_test_py.clickstream_raw
# MAGIC where curr_title is null
# MAGIC limit 10

# COMMAND ----------

# DBTITLE 1,Databricks Enhanced Autoscaling events
# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'RESIZING' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as starting_num_executors,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'SUCCEEDED' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as succeeded_num_executors,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'PARTIALLY_SUCCEEDED' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as partially_succeeded_num_executors,
# MAGIC   Double(
# MAGIC     case
# MAGIC       when details :autoscale.status = 'FAILED' then details :autoscale.requested_num_executors
# MAGIC       else null
# MAGIC     end
# MAGIC   ) as failed_num_executors
# MAGIC FROM
# MAGIC   event_log_raw
# MAGIC WHERE
# MAGIC   event_type = 'autoscale'
# MAGIC   AND origin.update_id = '${latest_update.id}'
# MAGIC order by timestamp
