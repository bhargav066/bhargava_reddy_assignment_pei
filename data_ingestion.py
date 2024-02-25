# Databricks notebook source
# MAGIC %run "./data_ingestion_utils"

# COMMAND ----------

load_data_to_delta("/FileStore/tables/Product.csv", "csv", "full","sales", "product_raw")

# COMMAND ----------

load_data_to_delta("/FileStore/tables/Customer.xlsx", "excel", "full","sales", "customer_raw")

# COMMAND ----------

load_data_to_delta("/FileStore/tables/Order.json", "json", "full","sales", "order_raw")

# COMMAND ----------

df = spark.read.option("header", "true").csv(file_path)
