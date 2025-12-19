# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG workspace;
# MAGIC USE SCHEMA default;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW silver_sales AS
# MAGIC SELECT *
# MAGIC FROM delta.`/Volumes/workspace/default/etl_sales/silver/sales`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_sales LIMIT 5;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_sales_sql
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC   CAST(order_date AS DATE) AS order_date,
# MAGIC   SUM(total_amount)        AS total_revenue,
# MAGIC   COUNT(order_id)    AS total_orders,
# MAGIC   AVG(total_amount)        AS avg_order_value
# MAGIC FROM silver_sales
# MAGIC GROUP BY CAST(order_date AS DATE);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gold_daily_sales_sql
# MAGIC ORDER BY order_date;
# MAGIC
