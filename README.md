# northwind-lakehouse-dbt-databricks

An **end-to-end Lakehouse analytics project on Azure**, demonstrating a modern data stack:

**ADF → ADLS Gen2 → Databricks (Unity Catalog) → dbt → SQL Endpoint → Power BI**

This project simulates a production-style analytics platform, including ingestion, transformation, governance, CI/CD, and a business intelligence layer.

---

## Project Overview

This repository demonstrates how to build a **secure, scalable Lakehouse architecture** using Azure technologies and dbt.

### Technologies used

| Layer            | Technology         |
| ---------------- | ------------------ |
| Data ingestion   | Azure Data Factory |
| Storage          | ADLS Gen2          |
| Lakehouse engine | Azure Databricks   |
| Governance       | Unity Catalog      |
| Transformation   | dbt                |
| CI/CD            | GitHub Actions     |
| Analytics        | Power BI           |


---

## Architecture

The platform follows a **modern Lakehouse architecture on Azure**, inspired by the deployed solution architecture shown below.

The architecture integrates **data ingestion, storage, transformation, governance, and analytics** into a unified data platform.

## End-to-End Data Flow

The pipeline consists of the following stages:

```text
        Source Data (Northwind)
                │
                ▼
        Azure Data Factory
                │
                ▼
        ADLS Gen2 Landing Zone (Parquet)
                │
                ▼
        Unity Catalog External Location
                │
                ▼
        Databricks Lakehouse
                │
                ▼
        dbt Transformation
        (Bronze → Silver → Gold)
                │
                ▼
        Databricks SQL Endpoint
                │
                ▼
        Power BI Analytics
```

### 1. Data Source

In this demo project, the data source is **Northwind operational data extracts.**

In a real enterprise scenario, this layer would typically include systems such as:

- ERP systems (SAP / Dynamics / Oracle)

- CRM platforms

- operational databases

- external APIs

For this demo, the dataset is exported and ingested into the platform using **Azure Data Factory.**


### 2. Data Ingestion – Azure Data Factory

Azure Data Factory (ADF) orchestrates the ingestion pipeline.

Responsibilities:

- Extract source data

- Write snapshots to the **ADLS landing zone**

- Partition data by ingestion date

- Enable incremental ingestion

Example landing structure:

```text
abfss://landing@panmaisonadls.dfs.core.windows.net/northwind/Orders/
2026-02-12/Orders/.parquet
2026-02-13/Orders/.parquet
```
This structure supports:

- incremental processing

- historical reprocessing

- traceable ingestion.


### 3. Data Landing Zone – ADLS Gen2

All raw data is stored in **Azure Data Lake Storage Gen2.**

Characteristics:

- stored as **Parquet files**

- partitioned by ingestion date

- immutable raw layer

This zone acts as the **data lake entry point** for the Lakehouse platform.


### 4. Databricks Lakehouse

Azure Databricks provides the **Lakehouse compute and storage layer.**

Key components:

**Storage Layer**

- ADLS Gen2

- Parquet and Delta Lake formats

**Transactional Metadata**

- Delta Lake

- ACID transactions

- time travel

- scalable metadata management


### 5. Unity Catalog – Governance Layer

The platform uses **Unity Catalog** for centralized governance.

Capabilities include:

- access control

- data lineage

- auditing

- data discovery

- Delta Sharing

Unity Catalog manages:

```text
catalog
schemas
tables
permissions
external locations
```
External locations allow Databricks to access ADLS **without embedding storage keys in code.**

### 6. Data Transformation – dbt

**dbt (Data Build Tool)** is used for SQL-based data transformation.

The project implements the **Medallion architecture:**

#### Bronze Layer

Purpose:

- ingest raw landing data

- minimal transformation

- convert Parquet → Delta

Additional metadata columns are added:

```text
load_date
source_file
```

These support **lineage and debugging.**

#### Silver Layer

Purpose:

- clean and standardize data

- apply business rules

- enforce data quality

Typical transformations:

- type casting

- deduplication

- null handling

- data validation


#### Gold Layer

Purpose:

- create analytics-ready models

The Gold layer follows a Star Schema design.

Example models:

```text
dim_date
dim_product
dim_customer

fact_orders
fact_invoice
fact_invoice_freight
```

These models power downstream analytics.

### 7. Data Quality

dbt provides built-in data quality testing.

Examples implemented:

- not_null

- unique

- relationships

- accepted value checks

These tests ensure:

- referential integrity

- schema stability

- reliable analytical models.

### 8. Query Layer – Databricks SQL

Databricks **SQL Warehouse** exposes the Gold layer for analytics tools.

Benefits:

- serverless scaling

- optimized BI queries

- secure access via Unity Catalog

This layer acts as the analytics endpoint.

### 9. Analytics – Power BI

Power BI connects to the **Databricks SQL endpoint.**

The dashboard provides:

- Revenue KPIs

- QoQ growth analysis

- product performance

- category analysis

- geographic sales distribution

---

## Repository structure

```md
northwind-lakehouse-dbt-databricks
│
├─ dbt/
│  ├─ dbt_project.yml
│  ├─ packages.yml
│  ├─ models/
│  │  ├─ bronze/
│  │  │  └─ northwind/
│  │  │     ├─ bronze_orders.sql
│  │  │     ├─ ...
│  │  │     └─ schema.yml
│  │  ├─ silver/
│  │  │  └─ northwind/
│  │  │     ├─ stg_orders.sql
│  │  │     ├─ ...
│  │  │     └─ schema.yml
│  │  └─ gold/
│  │     └─ northwind/
│  │        └─BIStarSchema/
│  │          ├─ fact_order.sql
│  │          ├─ ...
│  │          └─ schema.yml
│  │            
│  ├─ macros/
│  └─ tests/
│
├─ analytics/
│  └─ powerbi/
│     ├─ food_report.pbix
│     └─ dashboard.png
│
├─ docs/
│  ├─ architecture.png
│  └─ star_schema.png
│
├─ .github/workflows/
│  ├─ ci.yml
│  └─ cd.yml
│
└─ README.md
```

---

## Prerequisites

- Azure Databricks workspace with **Unity Catalog enabled**
- ADLS Gen2 Storage Account (landing container)
- Unity Catalog:
  - **Storage Credential**
  - **External Location** pointing to the landing container
- A Databricks **SQL Warehouse** (recommended: separate **dev** and **prod**)

---

## How to run (dbt Cloud)

1. Create a Databricks connection (SQL Warehouse)

    - Server Hostname
    - HTTP Path
    - Auth (OAuth or PAT depending on workspace policy)

2. Create environments:
    - dev → dev SQL Warehouse + dev schema
    - prod → prod SQL Warehouse + prod schema

3. Run in /dbt:
    - dbt deps
    -   dbt build --select path:models/bronze/northwind

## How to run (dbt-core local)
Install adapter:
```bash
pip install dbt-databricks
dbt deps
dbt debug -t dev --profiles-dir .
dbt build -t dev --select path:models/bronze/northwind --profiles-dir .
dbt test  -t dev --select path:models/bronze/northwind --profiles-dir .
```
Note: profiles.yml should use environment variables (no secrets committed).

## Bronze model (example)

**What the Bronze layer does**

Bronze reads parquet from the date-partitioned landing folders and adds:

- **load_date** parsed from the folder path  
- **source_file** for traceability  

**Example access path**
```bash
parquet.`abfss://landing@panmaisonadls.dfs.core.windows.net/northwind/Orders/*/Orders`
```

## CI/CD (GitHub Actions)

**CI – Pull Request validation**

- dbt deps  
- dbt compile  
- dbt build (changed models only; fallback to Bronze model on first run)

**CD – Production deployment**

Triggered on push to **main**

- Runs **dbt build -t prod** using the production SQL Warehouse
---
## GitHub Secrets & Variables

**Add Repository Secrets**

- **DATABRICKS_HOST** (e.g., https://dbc-xxxx.cloud.databricks.com)  
- **DATABRICKS_TOKEN** (PAT)  
- **DATABRICKS_HTTP_PATH_DEV** (SQL Warehouse HTTP path)  
- **DATABRICKS_HTTP_PATH_PROD** (SQL Warehouse HTTP path)  

**Add Repository Variables (optional but recommended)**

- **DBT_CATALOG** (e.g., erp_northwind)  
- **DBT_SCHEMA_DEV** (e.g., dev)  
- **DBT_SCHEMA_PROD** (e.g., prod)  

---

## Notes on cost control

**Recommended practices**


- Prefer **SQL Warehouse auto-stop** (10–15 minutes)  
- Avoid **always-on clusters** for development  
- Use **incremental models** to prevent full historical scans  