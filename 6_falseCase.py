# Databricks notebook source
var=dbutils.jobs.taskValues.get(taskkey='weekday_lookup',key='weekoutput')

# COMMAND ----------

print(var)