# Databricks notebook source
# MAGIC %md
# MAGIC Reading Bronze Delta

# COMMAND ----------

bronze_path = "/Volumes/workspace/default/etl_sales/bronze/sales"

df_bronze = spark.read.format("delta").load(bronze_path)

df_bronze.show()
df_bronze.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #Basic data cleaning (Silver rules)

# COMMAND ----------

# MAGIC %md
# MAGIC Cast data types explicitly

# COMMAND ----------

from pyspark.sql.functions import col

df_clean  = (df_bronze
             .withColumn("order_id",col("order_id").cast("int"))
             .withColumn("customer_id",col("customer_id").cast("int"))
             .withColumn("quantity",col("quantity").cast("int"))
             .withColumn("unit_price",col("unit_price").cast("int"))
             .withColumn("order_date",col("order_date").cast("date"))
             )

# COMMAND ----------

# MAGIC %md
# MAGIC Remove invalid records

# COMMAND ----------

df_valid = (
    df_clean
    .filter(col("order_id").isNotNull())
    .filter(col("quantity") > 0)
    .filter(col("unit_price") > 0)
)


# COMMAND ----------

# MAGIC %md
# MAGIC Deduplication (core Silver logic)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_timestamp").desc())

df_dedup = (df_valid
            .withColumn("rn",row_number().over(window_spec))
            .filter(col("rn")==1)
            .drop("rn")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Add derived columns

# COMMAND ----------

from pyspark.sql.functions import expr

df_silver = ( df_dedup
             .withColumn("total_amount", expr("quantity * unit_price"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC Write Silver Delta

# COMMAND ----------

silver_path = "/Volumes/workspace/default/etl_sales/silver/sales"

(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .save(silver_path)
)


# COMMAND ----------

spark.read.format("delta").load(silver_path).display()
