# Databricks notebook source
# MAGIC %md
# MAGIC #Data Access

# COMMAND ----------

# Replace sensitive values with clean placeholders
STORAGE_ACCOUNT_NAME = "<YOUR_STORAGE_ACCOUNT_NAME>"
STORAGE_ACCOUNT_KEY = "<YOUR_STORAGE_ACCOUNT_KEY>"
CONNECTION_STRING = "<YOUR_SQL_CONNECTION_STRING>"

# COMMAND ----------

# MAGIC %md
# MAGIC #Database Creation

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG adb_nyc_ifrah

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS gold

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Reading and Writing and Creating Delta Tables
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC **Storage Variables**

# COMMAND ----------

silver = "abfss://silver@nyctaxistorageifrah.dfs.core.windows.net"
gold = "abfss://gold@nyctaxistorageifrah.dfs.core.windows.net"

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Zone**

# COMMAND ----------

df_trip_zone = (
    spark.read.format("parquet")
    .option("inferSchema", True)
    .option("header", True)
    .load(f"{silver}/trip_zone")
)

# COMMAND ----------

df_trip_zone.display()

# COMMAND ----------

(
    df_trip_zone.write.format("delta")
    .mode("overwrite")
    .save(f"{gold}/trip_zone")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.trip_zone
# MAGIC AS SELECT * FROM delta.`abfss://gold@nyctaxistorageifrah.dfs.core.windows.net/trip_zone`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_zone

# COMMAND ----------

# MAGIC %md
# MAGIC **Trip Type**

# COMMAND ----------

df_trip_type = (
    spark.read.format("parquet")
    .option("inferSchema", True)
    .option("header", True)
    .load(f"{silver}/trip_type")
)

# COMMAND ----------

(
    df_trip_type.write.format("delta")
    .mode("overwrite")
    .save(f"{gold}/trip_type")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.trip_type
# MAGIC AS SELECT * FROM delta.`abfss://gold@nyctaxistorageifrah.dfs.core.windows.net/trip_type`;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.trip_type

# COMMAND ----------

# MAGIC %md
# MAGIC **Trips**

# COMMAND ----------

df_trip = (
    spark.read.format("parquet")
    .option("inferSchema", True)
    .option("header", True)
    .load(f"{silver}/trips2023data")
)

# COMMAND ----------

(
    df_trip.write.format("delta")
    .mode("overwrite")
    .save(f"{gold}/trips2023data")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.trips2023data
# MAGIC AS SELECT * FROM delta.`abfss://gold@nyctaxistorageifrah.dfs.core.windows.net/trips2023data`;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trips2023data

# COMMAND ----------

# MAGIC %md
# MAGIC **Learning Delta Lake**

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC update gold.trip_zone
# MAGIC set Borough = 'EMR'
# MAGIC where LocationID = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from gold.trip_zone
# MAGIC where LocationID = 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.trip_zone

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history gold.trip_zone

# COMMAND ----------

