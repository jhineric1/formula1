# Databricks notebook source

encoded_secret_key = secret_key.replace("/", "%2F")
aws_bucket_name = "ej2022formula1"
mount_name = "S3data"

dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", f"/mnt/{mount_name}")

display(dbutils.fs.ls(f"/mnt/{mount_name}"))

# COMMAND ----------

# MAGIC %fs ls mnt/S3data/formula1

# COMMAND ----------

# MAGIC %md
# MAGIC ##Circuits

# COMMAND ----------

circuits_df = spark.read.csv("dbfs:/mnt/S3data/formula1/circuits.csv")

# COMMAND ----------

circuits_df = spark.read \
    .option("header" , True) \
    .csv("dbfs:/mnt/S3data/formula1/circuits.csv")

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType

circuits_schema = StructType(fields=[StructField("circuitID", IntegerType(), False),
                  StructField("circuitRef", StringType(), True),
                  StructField("name", StringType() ,True),
                  StructField("location", StringType() ,True),
                  StructField("country", StringType(), True),
                  StructField("lat", DoubleType() ,True),
                  StructField("lng", DoubleType(),True),
                  StructField("alt", IntegerType(),True),
                  StructField("url", StringType(),True)]);

# COMMAND ----------

circuitsdf = spark.read \
		.option("header", True) \
		.schema(circuits_schema) \
.csv("dbfs:/mnt/S3data/formula1/circuits.csv");

# COMMAND ----------

display(circuitsdf)

# COMMAND ----------

circuitsdf.createOrReplaceTempView("bronze_circuits_dataset")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE bronze_circuitsdf
# MAGIC 	(CircuitID INTEGER, circuitRef STRING, name STRING, location_ STRING, country STRING, lat DOUBLE, lng DOUBLE, alt INTEGER, url STRING)
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO bronze_circuitsdf
# MAGIC SELECT * FROM bronze_circuits_dataset;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('bronze_circuitsdf', 1)

# COMMAND ----------

circuitsdf.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/bronze2/circuits")
#circuitsdf.write.mode("overwrite").format("delta").saveAsTable("bronze_circuits")


# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/S3data/formula1/bronze2/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

df2 = df.filter("country='Malaysia'")

# COMMAND ----------

display(df2)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.logStore.crossCloud.fatal" , "false")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Races

# COMMAND ----------

races_df = spark.read.csv("dbfs:/mnt/S3data/formula1/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType, TimestampType

races_schema = StructType(fields=[StructField("raceID", IntegerType(), False),
                  StructField("year", IntegerType(), True),
                  StructField("round", IntegerType() ,True),
                  StructField("circuitId", IntegerType() ,True),
                  StructField("name", StringType(), True),
                  StructField("date", DateType() ,True),
                  StructField("time", StringType(),True),
                  StructField("url", StringType(),True)]);

# COMMAND ----------

racesdf = spark.read \
		.option("header", True) \
		.schema(races_schema) \
.csv("dbfs:/mnt/S3data/formula1/races.csv");

# COMMAND ----------

display(racesdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Constructors

# COMMAND ----------

constructor_df = spark.read.json("dbfs:/mnt/S3data/formula1/constructors.json")

# COMMAND ----------

display(constructor_df)

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructorsdf = spark.read \
    .schema(constructors_schema) \
    .json("dbfs:/mnt/S3data/formula1/constructors.json")

# COMMAND ----------

display(constructorsdf)

# COMMAND ----------

constructorsdf.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/bronze2/constructors")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Drivers

# COMMAND ----------

drivers_df = spark.read.json("dbfs:/mnt/S3data/formula1/drivers.json")

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType, DateType, TimestampType

name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)])

# COMMAND ----------

driverssdf = spark.read \
    .schema(drivers_schema) \
    .json("dbfs:/mnt/S3data/formula1/drivers.json")

# COMMAND ----------

display(driverssdf)

# COMMAND ----------

driverssdf.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/bronze2/drivers")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusid", StringType(), True)
                                    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Results

# COMMAND ----------

resultsdf = spark.read \
    .schema(results_schema) \
    .json("dbfs:/mnt/S3data/formula1/results.json")

# COMMAND ----------

resultsdf.write.mode("overwrite").partitionBy("raceId").format("parquet").save("/mnt/S3data/formula1/bronze2/results")

# COMMAND ----------


pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", IntegerType(), True),
                                      StructField("lap", IntegerType() , True),
                                      StructField("time", StringType() , True),
                                      StructField("duration", FloatType() , True),
                                      StructField("milliseconds", IntegerType(), True)])

# COMMAND ----------

# MAGIC %md
# MAGIC ##Pit Stops

# COMMAND ----------

pit_stops_df = spark.read \
    .schema(pit_stops_schema) \
    .json("dbfs:/mnt/S3data/formula1/pit_stops.json")

# COMMAND ----------

pit_stops_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/bronze2/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Lap Times

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True),])

# COMMAND ----------

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv("/mnt/S3data/formula1/lap_times/lap_times_split*.csv")

# COMMAND ----------

lap_times_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/bronze2/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Races

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True)])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv("/mnt/S3data/formula1/races.csv")

# COMMAND ----------

races_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/bronze2/races")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Qualifying

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

qualifying_schema = StructType(fields=[StructField("qualifyingId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                       StructField("constructorId", IntegerType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("position", IntegerType(), True),
                                       StructField("q1", StringType(), True),
                                       StructField("q2", StringType(), True),
                                       StructField("q3", StringType(), True)])

# COMMAND ----------

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .json("dbfs:/mnt/S3data/formula1/qualifying/qualifying_split_*.json")

# COMMAND ----------

qualifying_df.write.mode("overwrite").format("parquet").save("/mnt/S3data/formula1/bronze2/qualifying")

# COMMAND ----------

# MAGIC %fs ls mnt/S3data/formula1/
