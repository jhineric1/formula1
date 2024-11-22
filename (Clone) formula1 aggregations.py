# Databricks notebook source
race_results_df = spark.read.format("parquet").load("/mnt/S3data/formula1/gold/abudhabi2020")
display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

race_results_df.select(count("*")).show()

# COMMAND ----------

race_results_df.select(count("name")).show()

# COMMAND ----------

race_results_df.select(countDistinct("name")).show()

# COMMAND ----------

race_results_df.select(sum("points")).show()

# COMMAND ----------

race_results_df.filter("name = 'Red Bull'").select(sum("points")).withColumnRenamed("sum(points)", "total points").show()

# COMMAND ----------

results_df = spark.read.format("parquet").load("/mnt/S3data/formula1/gold/results_2020")
display(results_df)

# COMMAND ----------

results_df.groupBy("full_name").sum("points").show()

# COMMAND ----------

results_df.groupBy("full_name") \
    .agg(sum("points"), countDistinct("name")) \
    .show()

# COMMAND ----------

race_results_demo_df = spark.read.format("parquet").load("/mnt/S3data/formula1/gold/results_2019_2020")
display(race_results_demo_df)

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

race_results_demo_df.select(count("*")).show()

# COMMAND ----------

race_results_demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

race_results_demo_df.filter("full_name = 'Lewis Hamilton'").select(sum("points")).show()

# COMMAND ----------

real_race_results = race_results_demo_df \
    .groupBy("race_year", "full_name") \
    .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"))

real_race_results.withColumn("rank", rank().over(driverRankSpec)).show(100)
