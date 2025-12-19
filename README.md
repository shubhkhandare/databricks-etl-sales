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




## Project Structure
databricks-etl-sales/
├── notebooks/
│   ├── bronze_ingestion.py
│   ├── silver_cleaning.py
│   ├── gold_aggregation.sql
│
├── sql/
│   └── exploratory_queries.sql
│
├── docs/
│   └── README.md
│
└── README.md



## Layers Explained

### Bronze
Raw data ingestion from source files into Delta tables.

### Silver
Data cleaning, deduplication, and schema enforcement.

### Gold
Aggregated tables for analytics and reporting.

## Status
🚧 In Progress – building step by step

