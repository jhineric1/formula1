# Databricks notebook source
# MAGIC %md
# MAGIC ##Filter

# COMMAND ----------

races_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019") \
    .withColumnRenamed("name", "race_name")
#races_filtered_df1 = races_df.filter("race_year = 2019 and round <= 5")
#races_filtered_df2 = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Join

# COMMAND ----------

circuits_df = spark.read.format("parquet").load("/mnt/S3data/formula1/silver/circuits") \
    .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Inner Join (default)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_filtered_df.circuit_id, "inner").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_filtered_df.race_name, races_filtered_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Outer Join

# COMMAND ----------

circuits_filtered_df = circuits_df.filter("circuit_id < 70")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_filtered_df, circuits_filtered_df.circuit_id == races_filtered_df.circuit_id, "left").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_filtered_df.race_name, races_filtered_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_filtered_df.circuit_id, "semi").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Anti Join

# COMMAND ----------

race_circuits_df = circuits_df.join(races_filtered_df, circuits_df.circuit_id == races_filtered_df.circuit_id, "anti").select(circuits_df.circuit_name, circuits_df.location, circuits_df.country)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross Join (cartesian product)

# COMMAND ----------

race_circuits_df = races_filtered_df.crossJoin(circuits_filtered_df)

# COMMAND ----------

race_circuits_df.count()

# COMMAND ----------


