# Databricks notebook source
dbutils.widgets.text('dayNumber','7');

# COMMAND ----------

var_day_num=int(dbutils.widgets.get('dayNumber'))

# COMMAND ----------

dbutils.jobs.taskValues.set(key='weekoutput',value=var_day_num)

# COMMAND ----------

