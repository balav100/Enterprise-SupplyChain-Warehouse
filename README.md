# Enterprise Supply Chain Data Warehouse Pipeline

## Overview

The Enterprise Supply Chain Data Warehouse Pipeline is a production-style Data Engineering project that processes and transforms over **180,000+ supply chain transactions** into a dimensional warehouse for business analytics. The solution leverages **PySpark**, **PostgreSQL**, **Apache Airflow**, **Docker**, and **Streamlit** to build a scalable ETL ecosystem that supports reporting, data quality validation, and executive decision-making.

The project follows a modern Data Warehouse architecture consisting of:

* Raw Data Ingestion
* Staging Layer
* Dimensional Modeling
* Fact Table Construction
* Data Quality Framework
* Workflow Orchestration
* Business Analytics Dashboard

---

# Architecture

```text
Supply Chain Dataset (CSV)
            │
            ▼
      PySpark ETL
            │
            ▼
    PostgreSQL Staging
   (stg_supply_chain)
            │
            ▼
 ┌─────────────────────┐
 │ Dimension Tables    │
 └─────────────────────┘
            │
            ▼
      fact_orders
            │
            ▼
   Analytics SQL Views
            │
            ▼
 Apache Airflow DAG
            │
            ▼
 Streamlit Dashboard
```

---

# Tech Stack

### Data Engineering

* PySpark
* PostgreSQL
* Apache Airflow
* Docker
* Redis

### Analytics & Visualization

* Streamlit
* Plotly
* Pandas

### Programming

* Python 3.11

### Data Quality

* Custom Validation Framework
* Business Rule Checks
* Duplicate Detection
* Null Detection

### Version Control & DevOps

* Git
* GitHub
* GitHub Actions (CI/CD)

---

# Project Structure

```text
Enterprise-SupplyChain-Warehouse
│
├── airflow
│   ├── dags
│   │   └── supply_chain_pipeline.py
│   ├── logs
│   ├── plugins
│   └── docker-compose.yaml
│
├── data
│   └── supply_chain_data.csv
│
├── database
│   ├── config.py
│   └── warehouse_schema.sql
│
├── spark_jobs
│   │
│   ├── load_staging.py
│   ├── build_fact_orders.py
│   ├── schema_check.py
│   ├── date_check.py
│   │
│   └── dimensions
│       ├── build_customer_dimension.py
│       ├── build_product_dimension.py
│       ├── build_category_dimension.py
│       ├── build_shipping_dimension.py
│       ├── build_region_dimension.py
│       └── build_date_dimension.py
│
├── data_quality
│   ├── check_row_counts.py
│   ├── check_nulls.py
│   ├── check_duplicates.py
│   ├── check_business_rules.py
│   └── run_quality_checks.py
│
├── dashboard
│   └── app.py
│
├── requirements.txt
└── README.md
```

---

# Data Warehouse Model

## Staging Table

```sql
stg_supply_chain
```

Rows Loaded:

```text
180,519
```

---

## Dimension Tables

| Table        |   Rows |
| ------------ | -----: |
| dim_customer | 20,652 |
| dim_product  |    118 |
| dim_category |     51 |
| dim_shipping |     12 |
| dim_region   |     23 |
| dim_date     |  1,127 |

---

## Fact Table

```sql
fact_orders
```

Rows Loaded:

```text
180,519
```

---

# ETL Pipeline

## Step 1 — Load Staging

```bash
python spark_jobs/load_staging.py
```

Functions:

* Reads CSV dataset
* Cleans schema
* Standardizes column names
* Loads PostgreSQL staging table

Output:

```sql
stg_supply_chain
```

---

## Step 2 — Build Dimensions

```bash
python spark_jobs/dimensions/build_customer_dimension.py

python spark_jobs/dimensions/build_product_dimension.py

python spark_jobs/dimensions/build_category_dimension.py

python spark_jobs/dimensions/build_shipping_dimension.py

python spark_jobs/dimensions/build_region_dimension.py

python spark_jobs/dimensions/build_date_dimension.py
```

Outputs:

```sql
dim_customer
dim_product
dim_category
dim_shipping
dim_region
dim_date
```

---

## Step 3 — Build Fact Table

```bash
python spark_jobs/build_fact_orders.py
```

Output:

```sql
fact_orders
```

---

# Data Quality Framework

The project includes a custom validation framework.

## Row Count Validation

```bash
python data_quality/check_row_counts.py
```

---

## Null Validation

```bash
python data_quality/check_nulls.py
```

---

## Duplicate Detection

```bash
python data_quality/check_duplicates.py
```

---

## Business Rule Validation

```bash
python data_quality/check_business_rules.py
```

---

## Execute All Checks

```bash
python data_quality/run_quality_checks.py
```

---

# Apache Airflow Orchestration

The pipeline is orchestrated using Apache Airflow.

## DAG Workflow

```text
load_staging
      │
      ▼
build_customer_dimension
      │
      ▼
build_category_dimension
      │
      ▼
build_product_dimension
      │
      ▼
build_shipping_dimension
      │
      ▼
build_region_dimension
      │
      ▼
build_date_dimension
      │
      ▼
build_fact_orders
      │
      ▼
run_quality_checks
```

---

## Start Airflow

```bash
cd airflow

docker compose up -d
```

Access:

```text
http://localhost:8081
```

---

# Analytics Layer

## Sales Summary

```sql
analytics.sales_summary
```

Provides:

* Revenue
* Profit
* Orders
* Monthly trends

---

## Customer Analysis

```sql
analytics.customer_analysis
```

Provides:

* Customer segment performance
* Revenue contribution

---

## Shipping Performance

```sql
analytics.shipping_performance
```

Provides:

* Shipping efficiency
* Delivery metrics

---

## Region Performance

```sql
analytics.region_performance
```

Provides:

* Regional sales
* Regional profitability

---

# Dashboard

Launch dashboard:

```bash
streamlit run dashboard/app.py
```

Features:

### KPI Metrics

* Total Revenue
* Total Profit
* Total Orders

### Visualizations

* Monthly Revenue Trend
* Customer Segment Analysis
* Regional Revenue Analysis
* Shipping Performance Dashboard

---

# Docker

Start infrastructure:

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

Services:

* PostgreSQL
* Airflow Scheduler
* Airflow Worker
* Airflow Triggerer
* Airflow API Server
* Redis

---

# Business Impact

This project demonstrates how modern organizations can transform raw operational supply chain data into a centralized analytics platform capable of supporting:

* Supply Chain Optimization
* Customer Analytics
* Revenue Analysis
* Regional Performance Monitoring
* Shipping Efficiency Tracking
* Executive Decision Support

---

# Key Achievements

* Processed **180,519+ supply chain transactions**
* Built a dimensional warehouse using **Star Schema**
* Developed **6 Dimension Tables** and **1 Fact Table**
* Implemented scalable ETL pipelines using **PySpark**
* Automated orchestration using **Apache Airflow**
* Built a custom **Data Quality Validation Framework**
* Created interactive executive dashboards using **Streamlit**
* Containerized infrastructure using **Docker**

---

# Author

**Balasubramaniam V**

B.Tech Computer Science and Engineering

Hindustan Institute of Technology and Science

GitHub: [https://github.com/balav100](https://github.com/balav100)

LinkedIn: [https://www.linkedin.com/in/balasubramaniam-v-280675359](https://www.linkedin.com/in/balasubramaniam-v-280675359)

---

## License

This project is intended for educational, portfolio, and research purposes.
