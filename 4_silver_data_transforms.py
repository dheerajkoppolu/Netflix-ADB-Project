# Databricks notebook source
# MAGIC %md
# MAGIC ###silver data transformations

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df= spark.read.format('delta')\
    .option('header',True)\
        .option('inferSchema',True)\
        .load('abfss://bronze@netflixprosdk.dfs.core.windows.net/netflix_titles')

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.fillna({"duration_minutes":0,"duration_seasons":0,"release_year":1})

# COMMAND ----------

display(df)

# COMMAND ----------

bad_records = df.filter(~col("duration_minutes").rlike("^[0-9]+$"))
display(bad_records)

# COMMAND ----------

df = df.filter(col("duration_minutes").rlike("^[0-9]+$"))

# COMMAND ----------

df=df.withColumn('duration_minutes',col('duration_minutes').cast(IntegerType()))\
    .withColumn('duration_seasons',col('duration_seasons').cast(IntegerType()))

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn('ShortTitle',split(col('title'),':')[0])

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.withColumn('rating',split(col('rating'),'-')[0])
display(df)

# COMMAND ----------

df=df.withColumn('type_flag',when(col('type')=='Movie',1)\
    .when(col('type')=='TV Show',2).otherwise(0))
display(df)

# COMMAND ----------

from pyspark.sql.window import Window
df=df.withColumn('dense_ranking',dense_rank().over(Window.orderBy(col('duration_minutes').desc())))

# COMMAND ----------

display(df)

# COMMAND ----------

df=df.createOrReplaceGlobalTempView("my_view")

# COMMAND ----------

df=spark.sql("""
SELECT * FROM global_temp.my_view """)

# COMMAND ----------

df.display()

# COMMAND ----------

df_vis=df.groupBy('type').agg(count("*").alias("Total_count"))
display(df_vis)

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("abfss://silver@netflixprosdk.dfs.core.windows.net/netflix_titles")

# COMMAND ----------

