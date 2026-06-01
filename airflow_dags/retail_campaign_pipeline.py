"""
ShopScope — Module 2 | airflow_dags/retail_campaign_pipeline.py
────────────────────────────────────────────────────────────────
PURPOSE:
  This is the Airflow DAG — the master orchestrator for ShopScope.
  It runs all pipeline steps in the correct order, every day at midnight.
  If any task fails, the run stops immediately and logs the failure.

WHAT AIRFLOW DOES FOR YOU:
  - Schedules the pipeline automatically (no cron needed)
  - Tracks every run in a visual UI (localhost:8080)
  - Retries failed tasks automatically
  - Shows you task duration, success/failure history
  - Lets you re-run individual tasks without rerunning everything

DAG TASK ORDER:
  extract_data
      ↓
  validate_data
      ↓
  transform_data
      ↓
  run_spark_jobs          ← Module 3 (PySpark)
      ↓
  generate_customer_kpis  ← SQL aggregation layer
      ↓
  export_dashboard_data   ← writes exports/ for Tableau

HOW TO RUN:
  1. Install Airflow:
        pip install apache-airflow
        airflow db init
        airflow webserver --port 8080    (in terminal 1)
        airflow scheduler                (in terminal 2)

  2. Copy this file to your Airflow DAGs folder:
        cp airflow_dags/retail_campaign_pipeline.py ~/airflow/dags/

  3. Open http://localhost:8080
     Find "shopscope_retail_campaign_pipeline"
     Toggle it ON and hit "Trigger DAG"

WHY THIS MATTERS FOR YOUR RESUME:
  "Engineered Apache Airflow DAG orchestrating 6-task retail analytics
   pipeline with automated scheduling, dependency management, retry logic,
   and failure alerting — processing Olist e-commerce data daily"
"""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# ── Airflow imports ───────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty  import EmptyOperator
from airflow.utils.dates      import days_ago

# ── Add project root to path so pipeline modules are importable ───────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level="INFO", format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("shopscope.dag")


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — DAG DEFAULT ARGUMENTS
# These apply to every task unless overridden on the task itself.
# ══════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner":            "shreya_joshi",
    "depends_on_past":  False,        # don't wait for yesterday's run to succeed
    "start_date":       days_ago(1),  # start from yesterday so first run fires immediately
    "retries":          2,            # retry each task up to 2 times on failure
    "retry_delay":      timedelta(minutes=5),   # wait 5 min between retries
    "email_on_failure": False,        # set True + add email config in production
    "email_on_retry":   False,
}


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — TASK FUNCTIONS
# Each function is one step in the pipeline.
# Airflow calls these functions; they import and run our pipeline modules.
# ══════════════════════════════════════════════════════════════════════════════

def task_extract_data(**context) -> dict:
    """
    TASK 1 — Extract
    Loads all 9 Olist CSV files into memory.
    Pushes dataset row counts to Airflow XCom so the next task can log them.
    """
    logger.info("=" * 50)
    logger.info("TASK 1 | extract_data | Starting")
    logger.info("=" * 50)

    from pipeline.extract import load_all_datasets, get_dataset_summary

    datasets = load_all_datasets()

    if not datasets:
        raise ValueError(
            "No datasets loaded. Ensure Olist CSVs are in data/raw/. "
            "Download: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
        )

    # Build a summary dict to push to XCom (Airflow's inter-task communication)
    summary = {name: len(df) for name, df in datasets.items()}
    total   = sum(summary.values())

    logger.info(f"TASK 1 | Loaded {len(datasets)} datasets | {total:,} total rows")

    # XCom push — next tasks can pull this with: context['ti'].xcom_pull(task_ids='extract_data')
    context["ti"].xcom_push(key="dataset_row_counts", value=summary)
    context["ti"].xcom_push(key="datasets_loaded",    value=list(datasets.keys()))

    return summary


def task_validate_data(**context) -> dict:
    """
    TASK 2 — Validate
    Runs 6 data quality rules on all loaded datasets.
    FAILS THE TASK if any CRITICAL issue is found — stops the pipeline.
    """
    logger.info("=" * 50)
    logger.info("TASK 2 | validate_data | Starting")
    logger.info("=" * 50)

    from pipeline.extract   import load_all_datasets
    from pipeline.validate  import validate_all, has_critical_issues

    datasets = load_all_datasets()
    report   = validate_all(datasets)

    # Convert report to serializable dict for XCom
    if not report.empty:
        issues_summary = report.groupby("severity").size().to_dict()
        warnings  = issues_summary.get("WARNING", 0)
        criticals = issues_summary.get("CRITICAL", 0)
    else:
        warnings, criticals = 0, 0

    logger.info(f"TASK 2 | Validation: {criticals} CRITICAL, {warnings} WARNING")

    if has_critical_issues(report):
        # This raises an exception → Airflow marks the task FAILED
        # The DAG stops here. transform_data and all downstream tasks are skipped.
        raise ValueError(
            f"Data quality CRITICAL issues found ({criticals} issues). "
            "Review validation report and fix source data before retrying."
        )

    context["ti"].xcom_push(key="validation_warnings",  value=warnings)
    context["ti"].xcom_push(key="validation_criticals", value=criticals)

    logger.info("TASK 2 | Validation PASSED — safe to proceed")
    return {"warnings": warnings, "criticals": criticals}


def task_transform_data(**context) -> dict:
    """
    TASK 3 — Transform
    Cleans all datasets and builds master_transactions.csv.
    This is the file every downstream task reads from.
    """
    logger.info("=" * 50)
    logger.info("TASK 3 | transform_data | Starting")
    logger.info("=" * 50)

    from pipeline.extract   import load_all_datasets
    from pipeline.transform import run_transform

    datasets = load_all_datasets()
    clean    = run_transform(datasets)

    if "master" not in clean:
        raise RuntimeError(
            "Master transactions table was not produced. "
            "Check transform.py logs for errors."
        )

    master_rows = len(clean["master"])
    master_cols = len(clean["master"].columns)

    logger.info(f"TASK 3 | Master table: {master_rows:,} rows × {master_cols} columns")
    logger.info("TASK 3 | Saved to data/processed/master_transactions.csv")

    context["ti"].xcom_push(key="master_rows", value=master_rows)
    context["ti"].xcom_push(key="master_cols", value=master_cols)

    return {"master_rows": master_rows, "master_cols": master_cols}


def task_run_spark_jobs(**context) -> dict:
    """
    TASK 4 — PySpark Aggregations (Module 3)
    Runs all 4 PySpark jobs:
      - customer aggregations
      - regional aggregations
      - monthly KPI aggregations
      - category aggregations

    Saves outputs to data/exports/ for Tableau and downstream analytics.
    """
    logger.info("=" * 50)
    logger.info("TASK 4 | run_spark_jobs | Starting")
    logger.info("=" * 50)

    from pipeline.spark_jobs import run_all_spark_jobs

    results = run_all_spark_jobs()

    logger.info(f"TASK 4 | Spark jobs complete: {len(results)} outputs produced")

    context["ti"].xcom_push(key="spark_outputs", value=list(results.keys()))
    return results


def task_generate_kpis(**context) -> dict:
    """
    TASK 5 — SQL KPI Generation
    Reads master_transactions.csv and runs SQL-style aggregations using pandas.
    Produces the KPI summary files that Module 4 (Customer Intelligence) reads.
    """
    logger.info("=" * 50)
    logger.info("TASK 5 | generate_customer_kpis | Starting")
    logger.info("=" * 50)

    import pandas as pd
    from config import PROCESSED_DIR, EXPORTS_DIR

    master_path = PROCESSED_DIR / "master_transactions.csv"
    if not master_path.exists():
        raise FileNotFoundError(
            f"master_transactions.csv not found at {master_path}. "
            "Ensure Task 3 (transform_data) completed successfully."
        )

    master = pd.read_csv(master_path, low_memory=False)
    logger.info(f"TASK 5 | Loaded master: {len(master):,} rows")

    # ── KPI 1: Customer-level metrics ─────────────────────────────────────────
    customer_kpis = master.groupby("customer_unique_id").agg(
        total_orders       = ("order_id",          "nunique"),
        total_spent        = ("total_order_value",  "sum"),
        avg_order_value    = ("total_order_value",  "mean"),
        total_items        = ("n_items",            "sum"),
        avg_review_score   = ("review_score",       "mean"),
        discount_orders    = ("discount_applied",   "sum"),
        first_purchase     = ("order_purchase_timestamp", "min"),
        last_purchase      = ("order_purchase_timestamp", "max"),
    ).reset_index()

    # Discount dependency rate
    customer_kpis["discount_rate"] = (
        customer_kpis["discount_orders"] / customer_kpis["total_orders"]
    ).round(4)

    customer_kpis_path = EXPORTS_DIR / "customer_kpis.csv"
    customer_kpis.to_csv(customer_kpis_path, index=False)
    logger.info(f"TASK 5 | Customer KPIs: {len(customer_kpis):,} customers → {customer_kpis_path}")

    # ── KPI 2: Monthly revenue summary ────────────────────────────────────────
    if "purchase_year" in master.columns and "purchase_month" in master.columns:
        monthly_kpis = master.groupby(["purchase_year", "purchase_month"]).agg(
            total_revenue  = ("total_order_value", "sum"),
            total_orders   = ("order_id",           "nunique"),
            avg_order_value= ("total_order_value",  "mean"),
            unique_customers=("customer_unique_id", "nunique"),
        ).reset_index()
        monthly_kpis_path = EXPORTS_DIR / "monthly_kpis.csv"
        monthly_kpis.to_csv(monthly_kpis_path, index=False)
        logger.info(f"TASK 5 | Monthly KPIs: {len(monthly_kpis)} months → {monthly_kpis_path}")

    # ── KPI 3: Category revenue ────────────────────────────────────────────────
    if "category_en" in master.columns:
        category_kpis = master.groupby("category_en").agg(
            total_revenue   = ("total_order_value", "sum"),
            total_orders    = ("order_id",           "nunique"),
            avg_order_value = ("total_order_value",  "mean"),
            avg_review_score= ("review_score",       "mean"),
        ).reset_index().sort_values("total_revenue", ascending=False)
        category_kpis_path = EXPORTS_DIR / "category_kpis.csv"
        category_kpis.to_csv(category_kpis_path, index=False)
        logger.info(f"TASK 5 | Category KPIs: {len(category_kpis)} categories → {category_kpis_path}")

    context["ti"].xcom_push(key="customer_kpi_count", value=len(customer_kpis))
    return {"customer_kpis": len(customer_kpis)}


def task_export_dashboard_data(**context) -> dict:
    """
    TASK 6 — Export for Tableau
    Reads all KPI CSVs from data/exports/ and confirms they're ready.
    In production, this task would push to Azure Blob Storage or a Tableau Server.
    """
    logger.info("=" * 50)
    logger.info("TASK 6 | export_dashboard_data | Starting")
    logger.info("=" * 50)

    import pandas as pd
    from config import EXPORTS_DIR

    expected_files = [
        "customer_kpis.csv",
        "monthly_kpis.csv",
        "category_kpis.csv",
        "customer_aggregations.csv",   # from Spark
        "regional_aggregations.csv",   # from Spark
    ]

    export_summary = {}
    missing_files  = []

    for fname in expected_files:
        fpath = EXPORTS_DIR / fname
        if fpath.exists():
            df = pd.read_csv(fpath)
            export_summary[fname] = len(df)
            logger.info(f"TASK 6 | ✓ {fname}: {len(df):,} rows")
        else:
            missing_files.append(fname)
            logger.warning(f"TASK 6 | ✗ {fname}: NOT FOUND")

    if missing_files:
        logger.warning(f"TASK 6 | Missing exports: {missing_files}")
        logger.warning("TASK 6 | Some Tableau dashboards may have incomplete data")
    else:
        logger.info("TASK 6 | All export files ready for Tableau")

    # ── In production: upload to Azure Blob Storage ────────────────────────────
    # from azure.storage.blob import BlobServiceClient
    # blob_client = BlobServiceClient.from_connection_string(os.environ["AZURE_CONN_STR"])
    # for fname in expected_files:
    #     blob_client.get_blob_client("shopscope-exports", fname).upload_blob(...)
    # logger.info("TASK 6 | Uploaded to Azure Blob Storage")

    logger.info("=" * 50)
    logger.info("PIPELINE COMPLETE — All 6 tasks finished")
    logger.info(f"Exports ready in: {EXPORTS_DIR}")
    logger.info("=" * 50)

    context["ti"].xcom_push(key="export_summary", value=export_summary)
    return export_summary


# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — DAG DEFINITION
# This is where we wire everything together.
# ══════════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="shopscope_retail_campaign_pipeline",

    description=(
        "ShopScope end-to-end retail analytics pipeline: "
        "Extract → Validate → Transform → Spark KPIs → Export for Tableau"
    ),

    default_args=default_args,

    # Run once per day at midnight
    schedule_interval="0 0 * * *",

    # Don't backfill all missed runs if the DAG was paused
    catchup=False,

    # Tags show up in the Airflow UI for filtering
    tags=["shopscope", "retail", "analytics", "etl", "pyspark"],

    # Maximum concurrent runs (prevent overlap if pipeline takes >24h)
    max_active_runs=1,

    # Rendered fields for UI documentation
    doc_md="""
    ## ShopScope Retail Campaign Intelligence Pipeline

    **Owner:** Shreya Joshi
    **Schedule:** Daily at midnight

    ### What this DAG does
    Runs the full ShopScope ETL pipeline over Brazilian e-commerce (Olist) data:
    1. **Extract** — loads 9 CSV datasets (~1M rows total)
    2. **Validate** — enforces 6 data quality rules, stops on critical failures
    3. **Transform** — builds master_transactions.csv with derived business metrics
    4. **Spark Jobs** — runs 4 PySpark aggregations (customer, regional, monthly, category)
    5. **KPI Generation** — computes customer health metrics, discount dependency
    6. **Export** — writes Tableau-ready CSVs to data/exports/

    ### Outputs
    - `data/processed/master_transactions.csv` — unified analytics table
    - `data/exports/customer_kpis.csv` — per-customer business metrics
    - `data/exports/monthly_kpis.csv` — monthly revenue trends
    - `data/exports/regional_aggregations.csv` — geographic insights
    - `data/exports/category_kpis.csv` — category performance
    """,
) as dag:

    # ── Task definitions ───────────────────────────────────────────────────────
    start = EmptyOperator(task_id="pipeline_start")

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=task_extract_data,
    )

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=task_validate_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=task_transform_data,
    )

    spark_jobs = PythonOperator(
        task_id="run_spark_jobs",
        python_callable=task_run_spark_jobs,
        # Give Spark tasks more time before timeout
        execution_timeout=timedelta(hours=1),
    )

    kpis = PythonOperator(
        task_id="generate_customer_kpis",
        python_callable=task_generate_kpis,
    )

    export = PythonOperator(
        task_id="export_dashboard_data",
        python_callable=task_export_dashboard_data,
    )

    end = EmptyOperator(task_id="pipeline_end")

    # ── Task dependencies (the >> operator = "then run") ───────────────────────
    start >> extract >> validate >> transform >> spark_jobs >> kpis >> export >> end
