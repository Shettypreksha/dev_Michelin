# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC input_stream_path="dbfs:/mnt/nlyadls/raw/iot_data/"
# MAGIC
# MAGIC output="dbfs:/mnt/nlyadls/raw/iot_data_output"

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists iot

# COMMAND ----------

# MAGIC %python
# MAGIC (
# MAGIC spark.readStream
# MAGIC .format("cloudFiles")
# MAGIC .option("cloudFiles.format", "json")
# MAGIC .option("cloudFiles.schemaLocation",f"{output}/Preksha/iot/bronze/schema")
# MAGIC .option("cloudFiles.InfercolumnTypes",True)
# MAGIC .load(input_stream_path)
# MAGIC .writeStream
# MAGIC .option("checkpointLocation", f"{output}/Preksha/iot/bronze/checkpoint")
# MAGIC .option("path",f"{output}/Preksha/iot/bronze/bronze_files")
# MAGIC .trigger(once=True)
# MAGIC .option("mergeSchema","true")
# MAGIC .table("iot.bronze")
# MAGIC )  

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from iot.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from iot.bronze

# COMMAND ----------


