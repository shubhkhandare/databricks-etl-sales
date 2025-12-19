# Databricks notebook source
silver_path = "/Volumes/workspace/default/etl_sales/silver/sales/"

silver_df = spark.read.format("delta").load(silver_path)

# COMMAND ----------

silver_df.display()

# COMMAND ----------

from pyspark.sql.functions import col

product_metrics = silver_df.select(
                                    col("product_id"),
                                    col("order_id"),
                                    col("total_amount")
)

# COMMAND ----------

product_metrics.display()

# COMMAND ----------

from pyspark.sql.functions import sum,avg,count
product_metrics = (product_metrics
                   .groupBy("product_id")
                   .agg(count("order_id").alias("total_orders"),
                        sum("total_amount").alias("total_revenue"),
                        avg("total_amount").alias("avg_revenue"))
                   )
 

# COMMAND ----------

product_metrics.display()

# COMMAND ----------

gold_path = "/Volumes/workspace/default/etl_sales/gold/product_metrics"

product_metrics.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)


# COMMAND ----------

spark.read.format("delta") \
    .load(gold_path) \
    .show(10)

