# Databricks notebook source
# MAGIC %md
# MAGIC ##silver notebook lookup tables

# COMMAND ----------

dbutils.widgets.text("source_folder","netflix_directors")
dbutils.widgets.text("target_folder","netflix_directors")

# COMMAND ----------

src_folder= dbutils.widgets.get("source_folder")
trg_folder = dbutils.widgets.get("target_folder")

# COMMAND ----------

df= spark.read.format("csv")\
    .option("header",True)\
        .option("inferSchema",True)\
        .load(f"abfss://bronze@netflixprosdk.dfs.core.windows.net/{src_folder}")

# COMMAND ----------

display(df)

# COMMAND ----------

df.write.format("delta").mode("append").option("path",f"abfss://silver@netflixprosdk.dfs.core.windows.net/{trg_folder}").save()

# COMMAND ----------

