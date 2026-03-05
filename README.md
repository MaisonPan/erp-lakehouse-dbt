# northwind-lakehouse-dbt-databricks

An **end-to-end Lakehouse analytics project on Azure**, demonstrating a modern data stack:
**ADF → ADLS Gen2 → Databricks (Unity Catalog) → dbt → Power BI**
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


## What this project demonstrates

- **Cloud ingestion pattern**: ADF → ADLS landing zone (daily folders)
- **Secure access**: Unity Catalog External Location (no account keys in code)
- **Lakehouse modeling**: Parquet landing → Delta Bronze (incremental)
- **Data quality & observability basics**: dbt tests + reproducible builds + docs
- **CI/CD**: GitHub Actions CI on PR, CD on merge to `main`
- **Cost-aware development**: SQL Warehouse Serverless vs clusters (guidance included)

---

## Architecture

**Flow**

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
Power BI Analytics

**Data layout (landing)**
Example:
abfss://landing@panmaisonadls.dfs.core.windows.net/northwind/Orders/
2026-02-12/Orders/.parquet
2026-02-13/Orders/.parquet

### Key design choices
- **No secrets in code**: access via UC External Location (Managed Identity / Credential)
- **Bronze is incremental**: avoids re-scanning full history
- **Audit columns on ingestion**:
  - `load_date` parsed from folder date
  - `source_file` from `input_file_name()` for lineage

---

## Current status (implemented)

✅ Landing data available in ADLS (date-partitioned Parquet)  
✅ Databricks SQL can read landing parquet paths (including wildcards)  
✅ dbt project structure created with Bronze/Silver/Gold schemas  
✅ **Bronze Northwind coverage: 26 datasets**
- `models/bronze/northwind/` contains Bronze models and tests
- `models/bronze/northwind/schema.yml` includes:
  - `not_null` checks for `load_date` / `source_file`
  - key constraints for core entity tables (e.g., Orders, Products, Customers)
  - relationships to enforce referential integrity where stable
  - `dbt_utils` helpers for range and accepted values where applicable

---

```md
## Repository structure
```text
northwind-lakehouse-dbt-databricks
│
├─ dbt/
│  ├─ dbt_project.yml
│  ├─ packages.yml
│  ├─ models/
│  │  ├─ bronze/
│  │  │  └─ northwind/
│  │  │     ├─ bronze_orders.sql
│  │  │     └─ schema.yml
│  │  ├─ silver/
│  │  └─ gold/
│  │     ├─ dimensions/
│  │     └─ facts/
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
- **DBT_SCHEMA_DEV** (e.g., hongwei)  
- **DBT_SCHEMA_PROD** (e.g., prod)  

---

## Notes on cost control

**Recommended practices**

- Prefer **SQL Warehouse auto-stop** (10–15 minutes)  
- Avoid **always-on clusters** for development  
- Use **incremental models** to prevent full historical scans  

---

## Roadmap

**Next improvements**

- [ ] Add **Silver** models (typed & cleaned)  
- [ ] Add **Gold** marts (analytics-ready facts/dims + KPI views)
- [ ] Improve incremental strategy per dataset (append vs merge / SCD)
- [ ] Publish dbt docs (GitHub Pages)
- [ ] Add automated data freshness + volume anomaly checks
- [ ] Add architecture diagram + UC governance notes (GRANTS, ownership)
