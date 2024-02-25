# Databricks notebook source
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# COMMAND ----------

from  pyspark.sql.functions import col, round, year, sum, to_date

# COMMAND ----------

order_df = spark.table('sales.order_raw').drop("row_id").withColumn('order_date', to_date(col('order_date'), "dd/MM/yyyy"))
customer_df = spark.table('sales.customer_raw')
product_df = spark.table('sales.product_raw').withColumnRenamed("state", "product_state")

# COMMAND ----------


enriched_df_1 = order_df.join(customer_df, "customer_id").join(product_df, "product_id")

# COMMAND ----------

enriched_df_1.write.format("delta").mode("overwrite").saveAsTable("sales.enriched_customers_products")

# COMMAND ----------

enriched_df_1 = spark.table('sales.enriched_customers_products')

# COMMAND ----------

enriched_df_2 = enriched_df_1.select(*order_df.columns,
    enriched_df_1["customer_name"].alias("customer"),
    enriched_df_1["country"],
    enriched_df_1["category"].alias("product_category"),
    enriched_df_1["sub_category"].alias("product_sub_category")
).withColumn("profit", round(col("profit"), 2))


# COMMAND ----------

enriched_df_2.write.format("delta").mode("overwrite").saveAsTable("sales.enriched_orders")

# COMMAND ----------

enriched_df_2 =  spark.table('sales.enriched_orders')

# COMMAND ----------

aggregate_df = enriched_df_2.groupBy(
    year("order_date").alias("year"),
    col("product_category"),
    col("product_sub_category"),
    col("customer"),
      col("customer_id")
).agg(
    sum("profit").alias("total_profit")
)

# COMMAND ----------

aggregate_df.write.format("delta").mode("overwrite").saveAsTable("sales.aggregate_profit")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, SUM(total_profit) AS profit
# MAGIC FROM sales.aggregate_profit
# MAGIC GROUP BY year  

# COMMAND ----------

# MAGIC %md
# MAGIC ### Profit by Year, product_category
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT year, product_category, SUM(total_profit) AS profit
# MAGIC FROM sales.aggregate_profit
# MAGIC GROUP BY year, product_category

# COMMAND ----------

# MAGIC %md
# MAGIC ### profit by customer

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer, SUM(total_profit) AS profit
# MAGIC FROM sales.aggregate_profit
# MAGIC GROUP BY customer_id, customer

# COMMAND ----------

# MAGIC %md
# MAGIC ### profit by customer, year

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, customer,year, SUM(total_profit) AS profit
# MAGIC FROM sales.aggregate_profit
# MAGIC GROUP BY customer_id, customer, year

# COMMAND ----------


