# databricks-etl-sales
# Databricks ETL Sales Project

End-to-end ETL pipeline implemented using Databricks following
Bronze, Silver, and Gold architecture.

## Tech Stack
- Databricks Community Edition
- PySpark
- Spark SQL
- Delta Lake
- GitHub


## Databricks Workflow

Job Name: etl_sales_pipeline

Task Order:
1. bronze_ingestion
2. silver_transform
3. gold_daily_sales
4. gold_customer_metrics
5. gold_product_metrics

Trigger:
- Manual (can be scheduled daily)

Cluster:
- Single node (Databricks Free)


## Layers Explained

### Bronze
Raw data ingestion from source files into Delta tables.

### Silver
Data cleaning, deduplication, and schema enforcement.

### Gold
Aggregated tables for analytics and reporting.


## Key Features
- Bronze–Silver–Gold data modeling
- Incremental processing using Delta Lake MERGE
- SQL and PySpark transformations
- Git-based notebook versioning
- Production-style Databricks workflow

## How to Run
1. Clone the repository into Databricks Repos
2. Configure Databricks Workflow tasks
3. Run the workflow manually or schedule it





