# Databricks notebook source
silver_path = "/Volumes/workspace/default/etl_sales/silver/sales"

silver_df = spark.read.format("delta").load(silver_path)


# COMMAND ----------

silver_df.display()

# COMMAND ----------

from pyspark.sql.functions import col


# COMMAND ----------

customer_df = silver_df.select(col("customer_id"),
                               col("order_id"),
                               col("total_amount"))

# COMMAND ----------

customer_df.display()

# COMMAND ----------

from pyspark.sql.functions import sum, count, avg

customer_metrics_df = (
    customer_df
    .groupBy("customer_id")
    .agg(
        sum("total_amount").alias("total_spend"),
        count("order_id").alias("total_orders"),
        avg("total_amount").alias("avg_order_value")
    )
)
display(customer_metrics_df)

# COMMAND ----------

customer_metrics_df.display()
