# Databricks notebook source
silver_path = "/Volumes/workspace/default/etl_sales/silver/sales/"

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)

# COMMAND ----------

silver_df.display()
silver_df.printSchema()


# COMMAND ----------

from pyspark.sql.functions import col, to_date


# COMMAND ----------

sales_df = silver_df.select(
    col("order_id"),
    col("order_date"),
    col("total_amount")
)


# COMMAND ----------

sales_df.display()


# COMMAND ----------

from pyspark.sql.functions import sum, avg, max, min

daily_sales_df = sales_df.groupBy(
    "order_date"
).agg(
    sum("total_amount").alias("total_sales")
)

# COMMAND ----------

daily_sales_df.orderBy("order_date").show()


# COMMAND ----------

gold_path = "/Volumes/workspace/default/etl_sales/gold/daily_sales"

daily_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)


# COMMAND ----------

spark.read.format("delta") \
    .load(gold_path) \
    .orderBy("order_date") \
    .show()

