# Databricks notebook source
circuits_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/circuits")
display(circuits_df)

# COMMAND ----------

drivers_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/drivers")

# COMMAND ----------

from pyspark.sql.functions import col, concat, lit
drivers_select_df = drivers_df \
    .withColumn("full_name", concat(col('forename'), lit(' '), col('surname'))) \
    .select("driver_id", "number", "full_name", "nationality")
display(drivers_select_df)

# COMMAND ----------

constructors_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/constructors") \
    .withColumnRenamed("name", "team_name")
display(constructors_df)

# COMMAND ----------

results_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/results") \
    .withColumnRenamed("time", "race_time") \
    .select("driver_id", "constructor_id", "race_id", "number", "grid", "fastest_lap_time", "race_time", "points") 
display(results_df)

# COMMAND ----------

races_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/races") \
    .filter("race_year in (2019, 2020)") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("time", "race_time") \
    .withColumnRenamed("date", "race_date")
display(races_df)

# COMMAND ----------

pit_stops_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/pitstops")
display(pit_stops_df)

# COMMAND ----------


results_drivers_cons_df = results_df.join(drivers_select_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
    .join(races_df, results_df.race_id == races_df.race_id, "inner") \
    .select(drivers_select_df.full_name, drivers_select_df.number, constructors_df.name, results_df.grid, results_df.fastest_lap_time, results_df.race_time, results_df.points)

# COMMAND ----------

display(results_drivers_cons_df)

# COMMAND ----------

results_drivers_cons_df.write.mode("overwrite").parquet("/mnt/S3data/formula1/gold/results_2020")

# COMMAND ----------

results_drivers_demo_df = results_df.join(drivers_select_df, results_df.driver_id == drivers_df.driver_id, "inner") \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, "inner") \
    .join(races_df, results_df.race_id == races_df.race_id, "inner") \
    .join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
    .select(races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.location, drivers_select_df.full_name, drivers_select_df.number, drivers_select_df.nationality, constructors_df.team_name, results_df.grid, results_df.fastest_lap_time, races_df.race_time, results_df.points)
display(results_drivers_demo_df)

# COMMAND ----------

results_drivers_demo_df.write.mode("overwrite").parquet("/mnt/S3data/formula1/gold/results_2019_2020")
