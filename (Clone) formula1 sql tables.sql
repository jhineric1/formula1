-- Databricks notebook source
-- MAGIC %md
-- MAGIC #External Tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

desc database f1_raw

-- COMMAND ----------

drop table f1_raw.circuits;

CREATE TABLE IF NOT EXISTS f1_raw.circuits
(
  circuitRef STRING,
  name STRING,
  location STRING,
  country STRING,
  lat DOUBLE,
  lng DOUBLE,
  alt INT,
  url STRING
)
USING csv
OPTIONS(path "dbfs:/mnt/S3data/formula1/circuits.csv", header true)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.races
(
  raceID INT,
  year INT,
  round INT,
  circuitId INT,
  name STRING,
  date DATE,
  time STRING,
  url STRING
)
USING csv
OPTIONS(path "dbfs:/mnt/S3data/formula1/races.csv", header true)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.constructors
(
  constructorId INT,
  constructorRef STRING,
  name STRING,
  nationality STRING,
  url STRING
)
USING JSON
OPTIONS(path "dbfs:/mnt/S3data/formula1/constructors.json")

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS f1_raw.drivers
(
  driverId INT,
  driverRef STRING,
  number INT,
  code STRING,
  name STRUCT<forename : STRING, surname : STRING>,
  dob DATE,
  nationality STRING,
  url STRING
)
USING json
OPTIONS(path "dbfs:/mnt/S3data/formula1/drivers.json")

-- COMMAND ----------

drop table if exists f1_raw.results;
create table if not exists f1_raw.results
(
  resultId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  number INT, grid INT, position INT,
  positionText STRING, positionOrder INT,
  points INT, laps INT, time STRING, 
  milliseconds INT, fastestLap INT,
  rank INT, fastestLapTime STRING, fastestLapSpeed FLOAT,
  statusId STRING
)
using json
OPTIONS(path "dbfs:/mnt/S3data/formula1/results.json")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops
(
  driverId INT,
  duration STRING,
  lap INT,
  milliseconds INT,
  raceId INT,
  stop INT,
  time STRING
)
USING JSON
OPTIONS(path "dbfs:/mnt/S3data/formula1/pit_stops.json", multiLine true)

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_times;
CREATE TABLE IF NOT EXISTS f1_raw.lap_times
(
  raceId INT,
  driverId INT,
  lap INT,
  position INT,
  time STRING,
  milliseconds INT
)
using CSV
OPTIONS(path "/mnt/S3data/formula1/lap_times/")

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying
(
  qualifyingId INT,
  raceId INT,
  constructorId INT,
  number INT,
  position INT,
  q1 STRING, q2 STRING, q3 STRING
)
USING JSON
OPTIONS(path "dbfs:/mnt/S3data/formula1/qualifying/", multiLine true)

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Managed tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/S3data/formula1/silver/circuits"

-- COMMAND ----------

SELECT * FROM f1_processed.circuits

-- COMMAND ----------


