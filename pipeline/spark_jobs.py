import logging
import sys
from pathlib import Path

import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import PROCESSED_DIR, EXPORTS_DIR, LOG_FORMAT, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.spark")

# ── Try to import PySpark; fall back to pandas if not available ───────────────
try:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from pyspark.sql.types import DoubleType, IntegerType
    SPARK_AVAILABLE = True
    logger.info("PySpark available — using distributed processing")
except ImportError:
    SPARK_AVAILABLE = False
    logger.warning(
        "PySpark not installed. Running in pandas fallback mode.\n"
        "Install with: pip install pyspark\n"
        "All outputs are identical — only the engine differs."
    )

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — SPARK SESSION FACTORY
# ══════════════════════════════════════════════════════════════════════════════

def get_spark_session() -> "SparkSession":
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("ShopScope_RetailAnalytics")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession ready | version: {spark.version}")
    return spark

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — SPARK JOBS
# ══════════════════════════════════════════════════════════════════════════════

# ── JOB 1: Customer Aggregations ─────────────────────────────────────────────

def customer_aggregations_spark(spark: "SparkSession") -> pd.DataFrame:
    master_path = str(PROCESSED_DIR / "master_transactions.csv")
    logger.info(f"[SPARK] Job 1: customer_aggregations | source: {master_path}")

    df = spark.read.csv(master_path, header=True, inferSchema=True)
    logger.info(f"[SPARK] Loaded {df.count():,} rows into Spark")

    # Core aggregation
    customer_agg = df.groupBy("customer_unique_id").agg(
        F.countDistinct("order_id")         .alias("total_orders"),
        F.sum("total_order_value")          .alias("total_revenue"),
        F.avg("total_order_value")          .alias("avg_order_value"),
        F.sum("n_items")                    .alias("total_items"),
        F.avg("review_score")               .alias("avg_review_score"),
        F.sum(F.col("discount_applied")
              .cast(IntegerType()))         .alias("discount_orders"),
        F.min("order_purchase_timestamp")   .alias("first_purchase"),
        F.max("order_purchase_timestamp")   .alias("last_purchase"),
        F.first("customer_state")           .alias("customer_state"),
    )

    # Derived columns
    customer_agg = customer_agg.withColumn(
        "is_repeat_customer",
        F.when(F.col("total_orders") > 1, True).otherwise(False)
    ).withColumn(
        "discount_rate",
        (F.col("discount_orders") / F.col("total_orders")).cast(DoubleType())
    ).withColumn(
        "is_discount_dependent",
        F.when(F.col("discount_rate") > 0.5, True).otherwise(False)
    )

    # Window function: rank customers by total_revenue (high → low)
    revenue_window = Window.orderBy(F.desc("total_revenue"))
    customer_agg = customer_agg.withColumn(
        "revenue_rank", F.rank().over(revenue_window)
    )

    # Convert to pandas for saving
    result = customer_agg.toPandas()
    out_path = EXPORTS_DIR / "customer_aggregations.csv"
    result.to_csv(out_path, index=False)

    logger.info(f"[SPARK] Job 1 complete: {len(result):,} customers → {out_path}")
    return result

def customer_aggregations_pandas() -> pd.DataFrame:
    """Pandas fallback for customer_aggregations when PySpark is not installed."""
    master_path = PROCESSED_DIR / "master_transactions.csv"
    if not master_path.exists():
        logger.error("[PANDAS] master_transactions.csv not found — run Module 1 first")
        return pd.DataFrame()

    df = pd.read_csv(master_path, low_memory=False)
    logger.info(f"[PANDAS] Job 1: customer_aggregations | {len(df):,} rows")

    agg = df.groupby("customer_unique_id").agg(
        total_orders       = ("order_id",             "nunique"),
        total_revenue      = ("total_order_value",     "sum"),
        avg_order_value    = ("total_order_value",     "mean"),
        total_items        = ("n_items",               "sum"),
        avg_review_score   = ("review_score",          "mean"),
        discount_orders    = ("discount_applied",      "sum"),
        first_purchase     = ("order_purchase_timestamp", "min"),
        last_purchase      = ("order_purchase_timestamp", "max"),
        customer_state     = ("customer_state",        "first"),
    ).reset_index()

    agg["is_repeat_customer"]   = agg["total_orders"] > 1
    agg["discount_rate"]        = (agg["discount_orders"] / agg["total_orders"]).round(4)
    agg["is_discount_dependent"]= agg["discount_rate"] > 0.5
    agg["revenue_rank"]         = agg["total_revenue"].rank(ascending=False, method="min").astype(int)

    out_path = EXPORTS_DIR / "customer_aggregations.csv"
    agg.to_csv(out_path, index=False)
    logger.info(f"[PANDAS] Job 1 complete: {len(agg):,} customers → {out_path}")
    return agg

# ── JOB 2: Regional Aggregations ─────────────────────────────────────────────

def regional_aggregations_spark(spark: "SparkSession") -> pd.DataFrame:
    master_path = str(PROCESSED_DIR / "master_transactions.csv")
    logger.info(f"[SPARK] Job 2: regional_aggregations")

    df = spark.read.csv(master_path, header=True, inferSchema=True)

    # Step 1: compute repeat customers at customer level first
    customer_level = df.groupBy("customer_unique_id", "customer_state").agg(
        F.countDistinct("order_id").alias("cust_orders"),
    )

    repeat_customers = customer_level.withColumn(
        "is_repeat", F.when(F.col("cust_orders") > 1, 1).otherwise(0)
    ).groupBy("customer_state").agg(
        F.count("customer_unique_id").alias("unique_customers"),
        F.sum("is_repeat").alias("repeat_customers"),
    )

    # Step 2: order-level regional metrics
    regional = df.groupBy("customer_state").agg(
        F.sum("total_order_value")            .alias("total_revenue"),
        F.countDistinct("order_id")           .alias("total_orders"),
        F.avg("total_order_value")            .alias("avg_order_value"),
        F.avg("review_score")                 .alias("avg_review_score"),
        F.avg(F.col("discount_applied")
              .cast(IntegerType()))           .alias("discount_order_rate"),
    )
    # Step 3: join repeat customers onto regional
    regional = regional.join(repeat_customers, on="customer_state", how="left")

    regional = regional.withColumn(
        "repeat_rate",
        (F.col("repeat_customers") / F.col("unique_customers")).cast(DoubleType())
    ).withColumn(
        "revenue_per_customer",
        (F.col("total_revenue") / F.col("unique_customers")).cast(DoubleType())
    )

    result = regional.orderBy(F.desc("total_revenue")).toPandas()
    out_path = EXPORTS_DIR / "regional_aggregations.csv"
    result.to_csv(out_path, index=False)
    logger.info(f"[SPARK] Job 2 complete: {len(result)} states → {out_path}")
    return result

def regional_aggregations_pandas() -> pd.DataFrame:
    """Pandas fallback for regional_aggregations."""
    master_path = PROCESSED_DIR / "master_transactions.csv"
    if not master_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(master_path, low_memory=False)
    logger.info(f"[PANDAS] Job 2: regional_aggregations | {len(df):,} rows")

    # Repeat customers per state
    cust_orders = df.groupby(["customer_unique_id", "customer_state"])["order_id"].nunique().reset_index()
    cust_orders.columns = ["customer_unique_id", "customer_state", "cust_orders"]
    cust_orders["is_repeat"] = cust_orders["cust_orders"] > 1

    repeat_by_state = cust_orders.groupby("customer_state").agg(
        unique_customers  = ("customer_unique_id", "nunique"),
        repeat_customers  = ("is_repeat",          "sum"),
    ).reset_index()

    # Order-level metrics by state
    regional = df.groupby("customer_state").agg(
        total_revenue      = ("total_order_value", "sum"),
        total_orders       = ("order_id",           "nunique"),
        avg_order_value    = ("total_order_value",  "mean"),
        avg_review_score   = ("review_score",       "mean"),
        discount_order_rate= ("discount_applied",   "mean"),
    ).reset_index()

    regional = regional.merge(repeat_by_state, on="customer_state", how="left")
    regional["repeat_rate"]         = (regional["repeat_customers"] / regional["unique_customers"]).round(4)
    regional["revenue_per_customer"]= (regional["total_revenue"]    / regional["unique_customers"]).round(2)
    regional = regional.sort_values("total_revenue", ascending=False)

    out_path = EXPORTS_DIR / "regional_aggregations.csv"
    regional.to_csv(out_path, index=False)
    logger.info(f"[PANDAS] Job 2 complete: {len(regional)} states → {out_path}")
    return regional

# ── JOB 3: Monthly KPI Aggregations ──────────────────────────────────────────

def monthly_kpi_aggregations_spark(spark: "SparkSession") -> pd.DataFrame:
    master_path = str(PROCESSED_DIR / "master_transactions.csv")
    logger.info(f"[SPARK] Job 3: monthly_kpi_aggregations")

    df = spark.read.csv(master_path, header=True, inferSchema=True)

    monthly = df.groupBy("purchase_year", "purchase_month").agg(
        F.sum("total_order_value")          .alias("total_revenue"),
        F.countDistinct("order_id")         .alias("total_orders"),
        F.avg("total_order_value")          .alias("avg_order_value"),
        F.countDistinct("customer_unique_id").alias("unique_customers"),
        F.avg(F.col("discount_applied")
              .cast(IntegerType()))         .alias("discount_rate"),
        F.avg("review_score")              .alias("avg_review_score"),
    ).orderBy("purchase_year", "purchase_month")

    # Window: month-over-month revenue growth using lag()
    time_window = Window.orderBy("purchase_year", "purchase_month")
    monthly = monthly.withColumn(
        "prev_month_revenue",
        F.lag("total_revenue", 1).over(time_window)
    ).withColumn(
        "mom_revenue_growth",
        ((F.col("total_revenue") - F.col("prev_month_revenue"))
         / F.col("prev_month_revenue")).cast(DoubleType())
    )

    result = monthly.toPandas()
    out_path = EXPORTS_DIR / "monthly_kpis.csv"
    result.to_csv(out_path, index=False)
    logger.info(f"[SPARK] Job 3 complete: {len(result)} months → {out_path}")
    return result

def monthly_kpi_aggregations_pandas() -> pd.DataFrame:
    """Pandas fallback for monthly_kpi_aggregations."""
    master_path = PROCESSED_DIR / "master_transactions.csv"
    if not master_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(master_path, low_memory=False)
    logger.info(f"[PANDAS] Job 3: monthly_kpi_aggregations | {len(df):,} rows")

    monthly = df.groupby(["purchase_year", "purchase_month"]).agg(
        total_revenue      = ("total_order_value",  "sum"),
        total_orders       = ("order_id",            "nunique"),
        avg_order_value    = ("total_order_value",   "mean"),
        unique_customers   = ("customer_unique_id",  "nunique"),
        discount_rate      = ("discount_applied",    "mean"),
        avg_review_score   = ("review_score",        "mean"),
    ).reset_index().sort_values(["purchase_year", "purchase_month"])

    # Month-over-month growth using pandas shift() (same as Spark lag())
    monthly["prev_month_revenue"] = monthly["total_revenue"].shift(1)
    monthly["mom_revenue_growth"] = (
        (monthly["total_revenue"] - monthly["prev_month_revenue"])
        / monthly["prev_month_revenue"]
    ).round(4)

    out_path = EXPORTS_DIR / "monthly_kpis.csv"
    monthly.to_csv(out_path, index=False)
    logger.info(f"[PANDAS] Job 3 complete: {len(monthly)} months → {out_path}")
    return monthly


# ── JOB 4: Category Aggregations ─────────────────────────────────────────────

def category_aggregations_spark(spark: "SparkSession") -> pd.DataFrame:
    master_path = str(PROCESSED_DIR / "master_transactions.csv")
    logger.info(f"[SPARK] Job 4: category_aggregations")

    df = spark.read.csv(master_path, header=True, inferSchema=True)

    category = df.groupBy("category_en").agg(
        F.sum("total_order_value")          .alias("total_revenue"),
        F.countDistinct("order_id")         .alias("total_orders"),
        F.avg("total_order_value")          .alias("avg_order_value"),
        F.avg("review_score")              .alias("avg_review_score"),
        F.avg(F.col("discount_applied")
              .cast(IntegerType()))         .alias("discount_rate"),
        F.sum(F.col("discount_applied")
              .cast(IntegerType()))         .alias("discount_orders"),
        F.countDistinct("customer_unique_id").alias("unique_customers"),
    )

    # Revenue rank among categories
    cat_window = Window.orderBy(F.desc("total_revenue"))
    category = category.withColumn("revenue_rank", F.rank().over(cat_window))

    # Flag high-discount categories
    category = category.withColumn(
        "high_discount_category",
        F.when(F.col("discount_rate") > 0.3, True).otherwise(False)
    )

    result = category.orderBy(F.desc("total_revenue")).toPandas()
    out_path = EXPORTS_DIR / "category_aggregations.csv"
    result.to_csv(out_path, index=False)
    logger.info(f"[SPARK] Job 4 complete: {len(result)} categories → {out_path}")
    return result


def category_aggregations_pandas() -> pd.DataFrame:
    """Pandas fallback for category_aggregations."""
    master_path = PROCESSED_DIR / "master_transactions.csv"
    if not master_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(master_path, low_memory=False)
    logger.info(f"[PANDAS] Job 4: category_aggregations | {len(df):,} rows")

    category = df.groupby("category_en").agg(
        total_revenue      = ("total_order_value", "sum"),
        total_orders       = ("order_id",           "nunique"),
        avg_order_value    = ("total_order_value",  "mean"),
        avg_review_score   = ("review_score",       "mean"),
        discount_rate      = ("discount_applied",   "mean"),
        discount_orders    = ("discount_applied",   "sum"),
        unique_customers   = ("customer_unique_id", "nunique"),
    ).reset_index()

    category["revenue_rank"]         = category["total_revenue"].rank(ascending=False, method="min").astype(int)
    category["high_discount_category"]= category["discount_rate"] > 0.3
    category = category.sort_values("total_revenue", ascending=False)

    out_path = EXPORTS_DIR / "category_aggregations.csv"
    category.to_csv(out_path, index=False)
    logger.info(f"[PANDAS] Job 4 complete: {len(category)} categories → {out_path}")
    return category

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — MAIN ORCHESTRATOR
# Called by Airflow Task 4 and also by run_module3.py
# ══════════════════════════════════════════════════════════════════════════════

def run_all_spark_jobs() -> dict[str, pd.DataFrame]:
    logger.info("=" * 60)
    logger.info("ShopScope | Module 3 | PySpark Jobs Starting")
    logger.info(f"Engine: {'PySpark' if SPARK_AVAILABLE else 'Pandas (fallback)'}")
    logger.info("=" * 60)

    # Check master table exists
    master_path = PROCESSED_DIR / "master_transactions.csv"
    if not master_path.exists():
        raise FileNotFoundError(
            f"master_transactions.csv not found at {master_path}.\n"
            "Run Module 1 first: python run_module1.py"
        )

    results = {}

    if SPARK_AVAILABLE:
        spark = get_spark_session()
        try:
            logger.info("\nRunning Job 1/4: Customer Aggregations")
            results["customer_aggregations"]  = customer_aggregations_spark(spark)

            logger.info("\nRunning Job 2/4: Regional Aggregations")
            results["regional_aggregations"]  = regional_aggregations_spark(spark)

            logger.info("\nRunning Job 3/4: Monthly KPI Aggregations")
            results["monthly_kpis"]           = monthly_kpi_aggregations_spark(spark)

            logger.info("\nRunning Job 4/4: Category Aggregations")
            results["category_aggregations"]  = category_aggregations_spark(spark)

        finally:
            spark.stop()
            logger.info("[SPARK] Session stopped")
    else:
        # Pandas fallback — identical outputs, different engine
        logger.info("\nRunning Job 1/4: Customer Aggregations (pandas)")
        results["customer_aggregations"]  = customer_aggregations_pandas()

        logger.info("\nRunning Job 2/4: Regional Aggregations (pandas)")
        results["regional_aggregations"]  = regional_aggregations_pandas()

        logger.info("\nRunning Job 3/4: Monthly KPI Aggregations (pandas)")
        results["monthly_kpis"]           = monthly_kpi_aggregations_pandas()

        logger.info("\nRunning Job 4/4: Category Aggregations (pandas)")
        results["category_aggregations"]  = category_aggregations_pandas()

    # ── Summary ───────────────────────────────────────────────────────────────
    logger.info("\n── Module 3 Output Summary ──────────────────────────────────")
    for name, df in results.items():
        logger.info(f"  {name}: {len(df):,} rows × {len(df.columns)} cols")
    logger.info("── All jobs complete ─────────────────────────────────────────")

    return results

# ── Run directly to test Module 3 standalone ──────────────────────────────────
if __name__ == "__main__":
    results = run_all_spark_jobs()

    print("\n── Sample: Customer Aggregations (top 5 by revenue) ──")
    if "customer_aggregations" in results:
        top5 = results["customer_aggregations"].nlargest(5, "total_revenue")
        print(top5[["customer_unique_id", "total_orders", "total_revenue", "discount_rate", "revenue_rank"]].to_string(index=False))

    print("\n── Sample: Regional Aggregations ──")
    if "regional_aggregations" in results:
        print(results["regional_aggregations"][["customer_state", "total_revenue", "repeat_rate", "unique_customers"]].head(5).to_string(index=False))
