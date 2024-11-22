# Databricks notebook source
# MAGIC %md
# MAGIC ##Circuits

# COMMAND ----------

circuits_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC Column Selection

# COMMAND ----------

from pyspark.sql.functions import col
circuits_selected_df1 = circuits_df.select('circuitId', 'circuitRef', 'name', 'location', 'country', 'lat', 'lng', 'alt')
circuits_selected_df2 = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"])
circuits_selected_df3 = circuits_df.select(col("circuitId"), col("circuitRef"), col("name").alias("circuit_name"))

# COMMAND ----------

circuits_renamed_df = circuits_selected_df1.withColumnRenamed("circuitId", "circuit_id") \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitute")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

circuits_final_df2 = circuits_final_df.withColumn("env", lit("dev"))

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/silver/circuits")

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Races

# COMMAND ----------

races_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/races")

# COMMAND ----------

races_selected_df = races_df.select("raceId", "year", "round", "circuitId", "name", "date", "time")

# COMMAND ----------

races_renamed_df = races_selected_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("year", "race_year") \
    .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat, col, lit
races_final_df = races_renamed_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

races_final_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/silver/races")
#mnt/S3data/formula1/silver/
#circuits_final_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/silver/circuits")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Constructors

# COMMAND ----------

constructors_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/constructors")

# COMMAND ----------

constructors_dropped_df = constructors_df.drop(constructors_df['url'])

# COMMAND ----------

constructors_renamed_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

constructors_final_df = constructors_renamed_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/silver/constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drivers

# COMMAND ----------

drivers_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/drivers")

# COMMAND ----------

from pyspark.sql.functions import col
drivers_dropped_df = drivers_df.drop(col('url'))

# COMMAND ----------

drivers_flat_df = drivers_dropped_df.select("driverId", "driverRef", "number", "code", "name.forename", "name.surname", "dob", "nationality")
display(drivers_flat_df)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
drivers_renamed_df = drivers_flat_df.withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("driverRef", "driver_ref") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

drivers_renamed_df.write.mode("overwrite").parquet("/mnt/S3data/formula1/silver/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results

# COMMAND ----------

results_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/results")

# COMMAND ----------

results_dropped_df = results_df.drop(col('statusid'))

# COMMAND ----------

results_renamed_df = results_dropped_df.withColumnRenamed("resultId", "result_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorId", "constructor_id") \
    .withColumnRenamed("positionText", "position_text") \
    .withColumnRenamed("positionOrder", "position_order") \
    .withColumnRenamed("fastestLap", "fastest_lap") \
    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_renamed_df.write.mode("overwrite").partitionBy('race_id').parquet("/mnt/S3data/formula1/silver/results")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pitstops

# COMMAND ----------

pitstops_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/pit_stops")

# COMMAND ----------

pitstops_renamed_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

pitstops_renamed_df.write.mode("overwrite").parquet("/mnt/S3data/formula1/silver/pitstops")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lap Times

# COMMAND ----------

laptimes_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/lap_times")

# COMMAND ----------

laptimes_renamed_df = laptimes_df.withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

laptimes_renamed_df.write.mode("overwrite").parquet("/mnt/S3data/formula1/silver/laptimes")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Qualifying

# COMMAND ----------

qualifying_df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/qualifying")

# COMMAND ----------

qualifying_renamed_df = qualifying_df.withColumnRenamed("qualifyingId", "qualifying_id") \
    .withColumnRenamed("raceId", "race_id") \
    .withColumnRenamed("driverId", "driver_id") \
    .withColumnRenamed("constructorID", "constructor_id") \
    .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

qualifying_renamed_df.write.mode("overwrite").parquet("/mnt/S3data/formula1/silver/qualifying")

# COMMAND ----------

# MAGIC %fs ls mnt/S3data/formula1/silver
