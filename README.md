
# ShopScope — Retail Campaign Intelligence Platform

Retail campaign intelligence platform — ETL pipeline, customer RFM segmentation, discount dependency analysis, and regional campaign analytics using Python, Airflow, PySpark, SQL, and Tableau.

ShopScope is an end-to-end retail analytics pipeline built around a real e-commerce dataset (Olist, 100K+ orders). It automates retail data workflows using an Airflow DAG (6 tasks), computes customer intelligence metrics including RFM segmentation and health scoring, identifies discount-dependent customers, analyses regional campaign performance, and detects anomalies in sales patterns.

The pipeline runs: raw CSV → validation → PySpark aggregations → SQL KPI layer → customer intelligence → Tableau dashboard exports.

Built to explore enterprise analytics workflows: Airflow orchestration, PySpark-style distributed aggregations, SQL KPI modelling, and Tableau dashboard design — using a production-inspired architecture rather than a notebook analysis.

---

## Problem Statement 

Retail companies collect millions of transactions across brands, regions, channels, and seasons — but most analytics tools only show *what already happened*. They don't help teams answer forward-looking questions like:

- Which customers are likely to churn and should be targeted in the next campaign?
- Which regions are underperforming relative to their historical baseline?
- Which customers only buy during discounts — and are they profitable?
- Which campaigns actually drove revenue, and which just increased cost?
- Where are the highest-opportunity customer segments right now?

**ShopScope is built to answer these questions.** It ingests raw retail transaction data, runs it through a validated ETL pipeline, computes customer intelligence scores and RFM segments, analyses campaign and regional performance, detects anomalies, and delivers everything through three business-ready Tableau dashboards.

---

## Architecture 

```
┌─────────────────────────────────────────────────────────────────────┐
│                     AIRFLOW ORCHESTRATION LAYER                     │
│          Daily DAG: extract → validate → transform → load → export  │
│               Retries · SLA monitoring · Task dependencies          │
└────────────────────────────┬────────────────────────────────────────┘
                             │
           ┌─────────────────┴─────────────────┐
           │                                   │
  ┌────────▼────────┐                ┌─────────▼────────┐
  │  Data ingestion  │                │   PySpark layer   │
  │  Load · validate │                │  Aggregations ·   │
  │  clean · schema  │                │  KPI computation  │
  └────────┬────────┘                └─────────┬────────┘
           └─────────────────┬─────────────────┘
                             │
                    ┌────────▼────────┐
                    │  SQL KPI layer   │
                    │ customer_kpis ·  │
                    │ campaign_metrics │
                    │ regional_metrics │
                    └────────┬────────┘
                             │
           ┌─────────────────┴──────────────────┐
           │                                    │
  ┌────────▼────────┐                 ┌─────────▼────────┐
  │   Customer       │                 │    Campaign       │
  │  intelligence    │                 │    analytics      │
  │  Health score ·  │                 │  ROI · regional · │
  │  RFM · segments  │                 │  discount impact  │
  └────────┬────────┘                 └─────────┬────────┘
           └─────────────────┬──────────────────┘
                             │
                    ┌────────▼────────┐
                    │ Anomaly detection│
                    │ Statistical ·    │
                    │ rolling average  │
                    └────────┬────────┘
                             │
           ┌─────────────────┼──────────────────┐
           │                 │                  │
  ┌────────▼──────┐ ┌────────▼──────┐ ┌────────▼──────┐
  │   Campaign     │ │   Customer    │ │  Geographic   │
  │   dashboard    │ │   dashboard   │ │   dashboard   │
  │  ROI · funnel  │ │ Health · RFM  │ │  Map · region │
  └───────────────┘ └───────────────┘ └───────────────┘
                    Tableau (production) · Streamlit (local)
```

---
## Working Pipeline 
<img width="579" height="460" alt="Screenshot 2026-05-28 at 3 39 10 PM" src="https://github.com/user-attachments/assets/824c04f8-214f-4f7a-93ec-5bb03f276ccc" />




---
 
## Tech stack

| Layer | Tool | Purpose |
|---|---|---|
| Orchestration | Apache Airflow | Daily DAG scheduling, task dependencies, retries |
| Scalable processing | PySpark | Customer aggregations, regional KPIs, channel metrics |
| Warehouse / SQL | Apache Hive concepts · SQLite (local) | Partitioned storage, analytical queries |
| Cloud concepts | Azure Blob Storage · Azure Databricks | Production-oriented design patterns |
| ML / scoring | Scikit-learn | Customer health scoring, lightweight segmentation |
| Dashboards | Tableau | Campaign, customer, and geographic dashboards |
| Local dashboard | Streamlit | Development and demo interface |
| Data processing | Python · Pandas · NumPy | ETL logic, transformations, validation |

---

## Dataset

**Brazilian E-Commerce Public Dataset by Olist** (available on Kaggle)

Contains: customer data · order transactions · product information · payment records · timestamps · seller data · delivery metrics · customer reviews · geographic information

This dataset naturally supports:
- Customer segmentation and RFM analysis
- Campaign and discount impact analysis
- Regional performance and geographic visualisation
- Retention and churn analysis
- KPI generation across brands and channels

---

## Project structure

```
shopscope/
│
├── data/
│   ├── raw/                          # Original Olist dataset files
│   ├── processed/                    # Cleaned and transformed tables
│   └── exports/                      # Dashboard-ready CSV exports
│
├── airflow_dags/
│   └── retail_campaign_pipeline.py   # Main Airflow DAG (6-task pipeline)
│
├── pipeline/
│   ├── extract.py                    # Load raw CSVs, schema check
│   ├── validate.py                   # Data quality rules, quarantine logic
│   ├── transform.py                  # Clean, derive KPI columns, joins
│   ├── spark_jobs.py                 # PySpark aggregation workflows
│   └── load.py                       # Write to warehouse / SQLite
│
├── analytics/
│   ├── customer_health.py            # Health score computation
│   ├── discount_dependency.py        # Discount-only buyer identification
│   ├── campaign_analysis.py          # Campaign ROI and effectiveness
│   ├── anomaly_detection.py          # Statistical anomaly detection
│   └── regional_analysis.py          # Region-level opportunity analysis
│
├── sql/
│   ├── customer_kpis.sql             # Repeat rate, frequency, LTV queries
│   ├── campaign_metrics.sql          # Conversion, ROI, uplift queries
│   └── regional_metrics.sql          # City and region aggregations
│
├── dashboard/
│   ├── tableau_exports/              # Aggregated CSVs for Tableau
│   └── streamlit_app.py             # Local dashboard (5 pages)
│
├── notebooks/
│   ├── eda.ipynb                     # Exploratory data analysis
│   └── business_analysis.ipynb      # Business insight notebooks
│
├── docs/
│   └── architecture.png             # Pipeline architecture diagram
│
├── requirements.txt
└── README.md
```

---

## Core modules

### Module 1 — Data ingestion and validation

Loads raw Olist CSV files, enforces schema, removes duplicates, handles nulls, standardises category and location fields, and parses timestamps into usable formats.

**Output:** Clean, validated retail transaction tables ready for analytics processing.

**Key validation rules applied:**
- Required column presence check
- Null value handling in critical fields
- Order amount range validation
- Timestamp format parsing and standardisation
- Duplicate order ID detection
- Category and city name standardisation

---

### Module 2 — Airflow ETL pipeline

Automates the full analytics workflow using a scheduled Airflow DAG.

**DAG task flow:**
```
extract_data
    ↓
validate_data
    ↓
transform_transactions
    ↓
generate_customer_kpis
    ↓
run_campaign_analysis
    ↓
export_dashboard_data
```

**Schedule:** Daily at 02:00  
**Retries:** 2 retries with 5-minute delay  
**Timeout:** 1-hour execution SLA per run

This simulates how production analytics teams automate daily reporting pipelines — the same pattern used at large retail companies.

---

### Module 3 — PySpark aggregation layer

Handles scalable retail transaction processing using PySpark DataFrames — demonstrating understanding of distributed-style data processing.

**Aggregations computed:**
- Customer-level: total spend, order count, avg order value, repeat frequency
- Regional: revenue by city and state, conversion trends by region
- Monthly KPIs: revenue trend, customer acquisition, churn indicators
- Category: product category performance by season
- Channel: app vs web vs in-store revenue contribution

**Key metrics generated:**
- Total revenue per customer / region / category
- Average order value
- Repeat purchase frequency
- Simplified customer lifetime value
- Discount utilisation rate

---

### Module 4 — Customer intelligence engine

Generates customer-level business intelligence used directly by the marketing team for targeting decisions.

**Customer health score** is computed from:
- Purchase frequency
- Recency (days since last order)
- Average order value
- Return activity
- Discount dependency ratio

**Customer segments produced:**

| Segment | Definition |
|---|---|
| High Value | High frequency, high spend, low discount dependency |
| Loyal Customers | Consistent purchase history, moderate spend |
| Discount Dependent | Majority of purchases during promotional periods |
| At-Risk Customers | Declining recency, previously active |
| Inactive Customers | No activity in 90+ days |

**Discount dependency analysis** — identifies customers who purchase almost exclusively during high-discount periods, helping the business understand true margin contribution per customer.

---

### Module 5 — Campaign analytics engine

Analyses marketing effectiveness across dimensions that directly inform campaign planning.

**Regional campaign analysis:**
- Revenue contribution by region
- Repeat purchase rate post-campaign
- Geographic conversion trends

**Discount impact analysis:**
- Profitability vs discount rate
- High-discount customer behaviour patterns
- Low-margin customer group identification

**Channel performance:**
- App vs web vs in-store revenue share
- Channel-level customer retention rates

**Seasonal analysis:**
- Monthly sales trend by category
- Seasonal demand patterns across product types

---

### Module 6 — Anomaly detection

Identifies unusual patterns in business data that signal problems or opportunities requiring investigation.

**Detection examples:**
- Sudden drop in daily order volume
- Abnormal refund or return spikes
- Underperforming campaign regions
- Unusually high discount dependency in a customer cohort
- Declining repeat purchase activity

**Method:** Statistical threshold-based detection combined with rolling average comparison — accessible and explainable without complex ML overhead.

---

### Module 7 — Tableau dashboards

Three executive-level dashboards built for business stakeholders.

**Dashboard 1 — Campaign intelligence**
- Campaign ROI and conversion trends
- Regional performance map
- Seasonal revenue patterns
- Marketing effectiveness by channel

**Dashboard 2 — Customer intelligence**
- Customer health score distribution
- High-value and at-risk customer groups
- Discount-dependent user segments
- Repeat purchase and churn-risk indicators

**Dashboard 3 — Geographic opportunity**
- Regional revenue heatmap
- High-opportunity and underperforming zones
- Customer density by city
- Campaign targeting recommendations by region

---

## Key business KPIs

### Customer metrics
- Repeat purchase rate
- Average order value
- Customer purchase frequency
- Discount utilisation rate
- Customer health score (composite)

### Campaign metrics
- Campaign ROI
- Conversion rate
- Revenue uplift (campaign vs baseline)
- Retention improvement

### Regional metrics
- Regional revenue contribution
- Top-performing cities
- High-opportunity vs underperforming regions

---

## How to run locally

```bash
# 1. Clone the repository
git clone https://github.com/shreyajoshi144/shopscope-retail-analytics.git
cd shopscope-retail-analytics

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download the Olist dataset from Kaggle
# Place CSV files inside data/raw/

# 4. Run the ETL pipeline
python pipeline/extract.py
python pipeline/validate.py
python pipeline/transform.py
python pipeline/load.py

# 5. Run analytics modules
python analytics/customer_health.py
python analytics/campaign_analysis.py
python analytics/regional_analysis.py

# 6. Export dashboard data
python analytics/export_dashboard_data.py

# 7. Launch local dashboard
streamlit run dashboard/streamlit_app.py
```

Dashboard opens at `http://localhost:8501`

For Airflow (optional):
```bash
pip install apache-airflow
airflow standalone
# Copy airflow_dags/retail_campaign_pipeline.py to ~/airflow/dags/
```

---

## Requirements

```
pandas>=2.0
numpy>=1.24
scikit-learn>=1.3
plotly>=5.15
streamlit>=1.25
matplotlib>=3.7
seaborn>=0.12
sqlalchemy>=2.0
```

Optional:
```
apache-airflow>=2.7    # Workflow orchestration
pyspark>=3.4           # Distributed processing
```


---

*Built as part of a retail analytics portfolio project exploring enterprise-grade data pipeline design.*
