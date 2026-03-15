# Databricks notebook source
# MAGIC %md
# MAGIC ##Autoloader

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA netflix_catalog.net_schema;

# COMMAND ----------

check_point="abfss://silver@netflixprosdk.dfs.core.windows.net/checkpoint"

# COMMAND ----------

df = spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", check_point)\
    .load("abfss://raw@netflixprosdk.dfs.core.windows.net")

# COMMAND ----------

display(df, checkpointLocation=check_point + "/display")

# COMMAND ----------

df.writeStream\
    .option("checkpointLocation", check_point)\
    .trigger(processingTime="10 seconds")\
    .start('abfss://bronze@netflixprosdk.dfs.core.windows.net/netflix_titles')