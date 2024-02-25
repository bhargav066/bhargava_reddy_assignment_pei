# Databricks notebook source
from pyspark.sql.functions import col, count, isnan, when

# COMMAND ----------

def count_missing_values(df, column_name):
    return df.filter((col(column_name).isNull()) | (col(column_name) == "") | (isnan(col(column_name)))).count()

# COMMAND ----------

def check_data_quality(orders_df, customers_df, products_df):
    # Completeness check for Orders
    missing_order_ids = count_missing_values(orders_df, "order_id")
    print(f"Missing order IDs: {missing_order_ids}")

    missing_customer_ids = count_missing_values(customers_df, "customer_id")
    print(f"Missing Customer IDs: {missing_customer_ids}")

    missing_product_ids = count_missing_values(products_df, "product_id")
    print(f"Missing Customer IDs: {missing_product_ids}")

    # Consistency check for Customer IDs and Product IDs in Orders
    inconsistent_customers = orders_df.join(customers_df, "customer_id", "left_anti").select("customer_id").distinct().count()
    inconsistent_products = products_df.join(orders_df, "product_id", "left_anti").select("product_id").distinct().count()
    print(f"Inconsistent customer IDs: {inconsistent_customers}")
    print(f"Inconsistent product IDs: {inconsistent_products}")


# COMMAND ----------

orders_df = spark.table('sales.order_raw')
customers_df = spark.table('sales.customer_raw')
products_df = spark.table('sales.product_raw')
check_data_quality(orders_df, customers_df, products_df)

# COMMAND ----------


