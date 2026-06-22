# ShopScope — Azure Data Engineering Pipeline

> End-to-end cloud data pipeline processing the Olist Brazilian e-commerce dataset across Azure Data Factory, ADLS Gen2, Databricks, and Delta Lake, orchestrated with Apache Airflow.


---

## What This Project Does

ShopScope ingests 9 Olist e-commerce datasets (99K+ orders, 1M+ geolocation records, 112K+ order items) from raw CSV files into a cloud lakehouse. The pipeline validates data quality at ingestion, transforms and enriches records into an analytics-ready master table, and writes curated output to Delta Lake — with the entire workflow orchestrated and monitored by Apache Airflow.

The goal is a production-style data engineering workflow: layered storage, automated quality gates, business-level transformations, and reliable orchestration.

---

## Architecture

<img width="2720" height="2080" alt="shopscope_architecture" src="https://github.com/user-attachments/assets/ccad0a7f-1214-46bf-8008-73d6589f185f" />

---

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2, Delta Lake |
| Processing | Azure Databricks, Apache Spark, PySpark |
| Orchestration | Apache Airflow |
| Language | Python, SQL |

---

## Dataset

The [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) contains 9 relational CSV files covering the full order lifecycle:

| File | Records | Columns |
|---|---|---|
| olist_customers_dataset.csv | 99,441 | 5 |
| olist_geolocation_dataset.csv | 1,000,163 | 5 |
| olist_order_items_dataset.csv | 112,650 | 7 |
| olist_order_payments_dataset.csv | 103,886 | 5 |
| olist_order_reviews_dataset.csv | 104,162 | 7 |
| olist_orders_dataset.csv | 99,441 | 8 |
| olist_products_dataset.csv | 32,951 | 9 |
| olist_sellers_dataset.csv | 3,095 | 4 |
| product_category_name_translation.csv | 71 | 2 |

---

## Pipeline Walkthrough

### Step 1: Data Ingestion using Azure Data Factory

The first stage of the pipeline focuses on data ingestion.

Azure Data Factory is used to extract transaction data from the source system and load it into Azure Data Lake Storage Gen2.

This stage separates ingestion from processing and ensures that source data is preserved before any transformations occur.

Key Activities:

Pipeline creation in Azure Data Factory
Data movement from source to cloud storage
Automated ingestion workflow
Raw data landing in ADLS Gen2

<img width="1470" height="799" alt="Screenshot 2026-06-20 at 12 20 12 PM" src="https://github.com/user-attachments/assets/99a6c6d7-72b1-4fa7-9d1f-c53f77cae4ee" />
<img width="1470" height="799" alt="Screenshot 2026-06-20 at 12 30 42 PM" src="https://github.com/user-attachments/assets/2c12f25f-3525-4e17-b4d5-4a67c495277f" />
<img width="1470" height="805" alt="Screenshot 2026-06-22 at 10 40 17 AM" src="https://github.com/user-attachments/assets/fcdca774-33f7-461a-9d7a-b237bf2ad810" />


Outcome:

All source transaction data is successfully transferred into the Raw Layer of Azure Data Lake Storage Gen2.

---

### Stage Step 2: Raw Data Storage in Azure Data Lake Storage Gen2

After ingestion, transaction files are stored in the Raw Layer of Azure Data Lake Storage Gen2.

The raw layer serves as the system of record and preserves original datasets before validation and transformation.

Benefits:

Data traceability
Historical retention
Reprocessing capability
Centralized cloud storage

Outcome:

Raw transaction datasets are available for downstream processing.
```
shopscope2026/
├── raw/          ← original Olist CSVs land here
├── processed/    ← validated, transformed data
├── exports/      ← downstream-ready outputs
└── archive/      ← historical snapshots
```

<img width="1470" height="799" alt="Screenshot 2026-06-20 at 4 47 28 PM" src="https://github.com/user-attachments/assets/43fc6ff3-6f4d-45f4-abf0-63e106c12542" />
<img width="1470" height="799" alt="Screenshot 2026-06-21 at 12 03 47 AM" src="https://github.com/user-attachments/assets/2990399a-5289-4d9c-a4d0-4ba63da9900b" />
<img width="1470" height="799" alt="Screenshot 2026-06-20 at 12 44 02 PM" src="https://github.com/user-attachments/assets/cc25985d-ba1c-4f8a-876e-f5d06054dd94" />


---

### Step 3 — Data Validation (`01_validate_raw`)

Before any transformation, all 9 datasets pass through a validation notebook in Azure Databricks. Validation runs 6 rule categories across every dataset:

| Check | What it catches |
|---|---|
| Null values | Missing values in critical fields |
| Duplicate records | Exact-row duplicates across the dataset |
| Required columns | Fields missing from schema definition |
| Review score validity | Scores outside [1, 2, 3, 4, 5] or malformed |
| Price validation | Negative or null price values |
| Dataset completeness | Row count below expected thresholds |

**Validation results from the Olist data:**

| Dataset | Issues | Detail |
|---|---|---|
| geolocation | 1 WARNING | 261,831 duplicate rows (26.2% of dataset) |
| reviews | 4 WARNINGS | 85 duplicate rows; 88.5% null comment titles; 60.6% null comment messages; 2,557 malformed review scores |
| All others | 0 issues | — |
| CRITICAL issues | **None** | Pipeline continues |

The pipeline halts automatically if any CRITICAL-severity issue is detected. Warnings are logged and the pipeline proceeds.

<img width="1470" height="805" alt="Screenshot 2026-06-22 at 10 50 14 AM" src="https://github.com/user-attachments/assets/fa33354e-54a4-463f-a363-426cd43d34a9" />
<img width="1470" height="805" alt="Screenshot 2026-06-22 at 10 50 00 AM" src="https://github.com/user-attachments/assets/8512f5c1-7580-4bfd-be38-61abe0146410" />
<img width="1470" height="805" alt="Screenshot 2026-06-22 at 10 49 50 AM" src="https://github.com/user-attachments/assets/6eb955d0-c050-4a5f-98f7-9f5b13afa65c" />



---

### Step 4 — Transformation (`02_build_master_transactions`)

Validated data is joined, enriched, and aggregated into a single order-level master table. Transformations include:

- **Customer enrichment** — join customer city, state, zip to each order
- **Product category enrichment** — map Portuguese category names to English translations
- **Order-level aggregation** — total value, freight, item count per order
- **Payment aggregation** — total payment value, payment type, installment count
- **Review sentiment classification** — classify review scores into sentiment buckets (positive / neutral / negative)
- **Delivery performance metrics** — compute delivery duration (order → delivery), flag late deliveries
- **Revenue metrics** — gross revenue, freight ratio per order
- **Purchase date analytics** — extract purchase month, day of week, hour

<img width="1470" height="805" alt="Screenshot 2026-06-22 at 10 50 43 AM" src="https://github.com/user-attachments/assets/ee48b50f-4776-4896-81ae-79391c4c68d4" />
<img width="1470" height="805" alt="Screenshot 2026-06-22 at 10 50 35 AM" src="https://github.com/user-attachments/assets/b4f8801e-9206-4170-bdb0-c82b02ae61aa" />

---

### Step 5 — Delta Lake Write *(in progress)*

The curated `master_transactions` table is written to Delta Lake in the `processed/` container. Delta Lake provides:

- ACID transactions for reliable writes
- Schema enforcement across pipeline runs
- Time travel for point-in-time data recovery
- Optimized columnar storage for analytical queries

**Planned schema (master_transactions):**

```
order_id, customer_id, customer_city, customer_state,
order_status, purchase_timestamp, purchase_month, purchase_day_of_week,
total_order_value, total_freight, item_count,
total_payment_value, payment_type, payment_installments,
review_score, sentiment_category,
product_category_english, delivery_duration_days, is_late_delivery,
seller_id, seller_city, seller_state
```

---

### Step 6 — Orchestration (Apache Airflow) *(in progress)*

Apache Airflow orchestrates the full pipeline end-to-end:

```
[ADF Ingestion trigger]
        ↓
[run_validation_notebook]
        ↓
[check_critical_failures] ── CRITICAL? ──→ [alert + halt]
        ↓ (pass)
[run_master_transactions_notebook]
        ↓
[write_delta_table]
        ↓
[pipeline_complete_notification]
```

Features: scheduled runs, dependency management, automatic retries on failure, Slack/email alerts on critical failures.

---

## Business Insights Available

Once the Delta table is populated, it supports analytics including:

- Monthly and quarterly revenue trends
- Top-performing product categories by revenue and volume
- Customer distribution by state (Brazil geography)
- On-time vs late delivery rate by seller and region
- Payment method split and average installment count
- Review sentiment trends by category and time period

---

## Current Status

| Stage | Status |
|---|---|
| ADF ingestion pipeline | ✅ Complete |
| ADLS Gen2 storage layout | ✅ Complete |
| Databricks workspace + ADLS auth | ✅ Complete |
| Data validation notebook | ✅ Complete |
| Master transactions notebook | ✅ Complete |
| Delta Lake write | 🔄 In progress |
| Airflow orchestration | 🔄 In progress |

---

## Repository Structure

```
shopscope/
├── notebooks/
│   ├── 01_validate_raw.py
│   └── 02_build_master_transactions.py
├── airflow/
│   └── shopscope_dag.py
├── screenshots/
│   ├── adf_pipeline.png
│   ├── adls_containers.png
│   ├── databricks_workspace.png
│   ├── validation_report.png
│   └── master_transactions.png
├── docs/
│   └── architecture.png
└── README.md
```

---

## Key Technical Decisions

**Why Delta Lake over plain Parquet?** Delta Lake adds ACID transactions and schema enforcement. For a multi-stage pipeline where a failed run could partially overwrite data, this prevents corrupted analytical reads.

**Why validate before transform?** Catching data quality issues early (at raw ingestion) means transformation logic stays clean and never has to handle malformed inputs. The halt-on-CRITICAL pattern means downstream consumers never see broken data.

**Why ADLS Gen2 over Blob Storage?** Gen2 adds hierarchical namespace (folder semantics), which maps cleanly to the raw/processed/exports/archive container layout and integrates directly with Databricks via ABFS.

---

## Setup & Reproduction

> Requires: Azure subscription, Databricks workspace, Azure Data Factory, Python 3.8+

1. Upload Olist CSVs to `{storage-account}/raw/` in ADLS Gen2
2. Configure Databricks linked service in ADF
3. Run `01_validate_raw` notebook — review validation report
4. Run `02_build_master_transactions` notebook
5. *(coming)* Deploy `airflow/shopscope_dag.py` to your Airflow instance

Full setup instructions will be added as the project reaches completion.

---

## Future Enhancements

- Incremental data loading (process only new records)
- Delta Lake optimization (OPTIMIZE + ZORDER on order_id, purchase_month)
- Real-time ingestion via Event Hubs
- Data quality monitoring dashboard (Streamlit or Databricks SQL)
- CI/CD deployment pipeline for notebooks
- Unit tests for transformation logic

---

*Built with Azure Databricks · PySpark · Delta Lake · Apache Airflow · Azure Data Lake Storage Gen2*
