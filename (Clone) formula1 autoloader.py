# Databricks notebook source
# MAGIC %fs ls mnt/S3data/formula1

# COMMAND ----------

directory = '/mnt/S3data/formula1/f1db_csv/'
df = spark.read.option("Header", True).csv(directory)
schema = df.schema

# COMMAND ----------

df1 = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.useNotifications", "true") \
    .option("cloudFiles.region", "us-east-2") \
    .schema(schema) \
    .load(directory)

# COMMAND ----------

df1.writeStream.format("delta") \
    .option("checkpointLocation", "/mnt/S3data/formula1/f1db_checkpoint/") \
    .start("/mnt/S3data/formula1/f1db_landing")

# COMMAND ----------


