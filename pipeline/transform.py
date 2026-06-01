import logging
import pandas as pd
import numpy as np
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    PROCESSED_DIR, PROCESSED_FILES, TIMESTAMP_COLUMNS,
    LOG_FORMAT, LOG_LEVEL,
)

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.transform")

# ── Step 1: Parse timestamps ──────────────────────────────────────────────────

def parse_timestamps(name: str, df: pd.DataFrame) -> pd.DataFrame:
    """Convert timestamp string columns to pandas datetime objects."""
    df = df.copy()
    cols = TIMESTAMP_COLUMNS.get(name, [])

    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
            n_failed = df[col].isnull().sum()
            if n_failed > 0:
                logger.warning(f"{name}.{col}: {n_failed} rows failed timestamp parse")

    logger.info(f"[TIMESTAMPS] {name}: parsed {len(cols)} timestamp columns")
    return df

# ── Step 2: Standardize text columns ─────────────────────────────────────────

def standardize_text(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and strip all object (string) columns for consistent matching."""
    df = df.copy()
    text_cols = df.select_dtypes(include="object").columns

    for col in text_cols:
        df[col] = df[col].str.strip().str.lower()

    logger.info(f"[TEXT] Standardized {len(text_cols)} string columns")
    return df

# ── Step 3: Clean individual datasets ────────────────────────────────────────

def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    df = parse_timestamps("orders", df)
    df = standardize_text(df)

    # Filter to actionable statuses only
    analytics_statuses = {"delivered", "shipped", "invoiced"}
    before = len(df)
    df = df[df["order_status"].isin(analytics_statuses)].copy()
    logger.info(f"[ORDERS] Status filter: {before:,} → {len(df):,} rows")

    # Derived: delivery days (how long did delivery actually take?)
    df["delivery_days_actual"] = (
        df["order_delivered_customer_date"] - df["order_purchase_timestamp"]
    ).dt.days

    # Derived: delivery days estimated
    df["delivery_days_estimated"] = (
        df["order_estimated_delivery_date"] - df["order_purchase_timestamp"]
    ).dt.days

    # Derived: on_time delivery flag
    df["delivered_on_time"] = (
        df["order_delivered_customer_date"] <= df["order_estimated_delivery_date"]
    )

    # Derived: purchase year/month for time-series analysis
    df["purchase_year"]  = df["order_purchase_timestamp"].dt.year
    df["purchase_month"] = df["order_purchase_timestamp"].dt.month
    df["purchase_month_name"] = df["order_purchase_timestamp"].dt.strftime("%b")
    df["purchase_quarter"] = df["order_purchase_timestamp"].dt.quarter
    df["purchase_dow"] = df["order_purchase_timestamp"].dt.day_name()

    # Drop rows with no purchase timestamp (can't do time-series without it)
    df = df.dropna(subset=["order_purchase_timestamp"])

    logger.info(f"[ORDERS] Clean complete: {len(df):,} rows")
    return df


def clean_order_items(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean order_items. Compute order-level revenue metrics.
    - total_item_value = price + freight_value
    - discount flag placeholder (will be enriched later in Module 4)
    """
    df = standardize_text(df)

    # Remove rows with no price
    df = df.dropna(subset=["price"])
    df = df[df["price"] > 0]

    # Derived: total cost per item line
    df["total_item_value"] = df["price"] + df["freight_value"].fillna(0)

    # Order-level aggregation (we'll join this to orders)
    order_agg = df.groupby("order_id").agg(
        n_items          = ("order_item_id", "count"),
        total_price      = ("price", "sum"),
        total_freight    = ("freight_value", "sum"),
        total_order_value= ("total_item_value", "sum"),
        avg_item_price   = ("price", "mean"),
    ).reset_index()

    logger.info(f"[ORDER ITEMS] {len(df):,} line items → {len(order_agg):,} order aggregations")
    return df, order_agg

def clean_customers(df: pd.DataFrame) -> pd.DataFrame:
    """Clean customers. Standardize state codes."""
    df = standardize_text(df)
    df = df.drop_duplicates(subset=["customer_unique_id"])
    logger.info(f"[CUSTOMERS] Clean complete: {len(df):,} unique customers")
    return df

def clean_products(df: pd.DataFrame, category_names: pd.DataFrame | None) -> pd.DataFrame:
    """
    Clean products. Join English category names if available.
    Fill missing category with 'unknown'.
    """
    df = standardize_text(df)
    df["product_category_name"] = df["product_category_name"].fillna("unknown")

    # Join English translations if we have them
    if category_names is not None:
        category_names = standardize_text(category_names)
        df = df.merge(
            category_names,
            on="product_category_name",
            how="left"
        )
        df["category_en"] = df.get(
            "product_category_name_english",
            df["product_category_name"]
        ).fillna(df["product_category_name"])
    else:
        df["category_en"] = df["product_category_name"]

    logger.info(f"[PRODUCTS] Clean complete: {len(df):,} products")
    return df

def clean_payments(df: pd.DataFrame) -> pd.DataFrame:
    df = standardize_text(df)
    df = df.dropna(subset=["payment_value"])

    # Aggregate installments — total paid, payment method, num installments
    pay_agg = df.groupby("order_id").agg(
        total_payment     = ("payment_value", "sum"),
        payment_type      = ("payment_type", "first"),
        payment_installments = ("payment_installments", "max"),
    ).reset_index()

    logger.info(f"[PAYMENTS] {len(df):,} rows → {len(pay_agg):,} order-level payment records")
    return pay_agg


def clean_reviews(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean reviews. Keep review_score and compute sentiment bucket.
    1–2 = negative, 3 = neutral, 4–5 = positive
    """
    df = parse_timestamps("reviews", df)
    df = df.dropna(subset=["review_score"])
    df["review_score"] = df["review_score"].astype(int)

    df["sentiment"] = pd.cut(
        df["review_score"],
        bins=[0, 2, 3, 5],
        labels=["negative", "neutral", "positive"],
        right=True,
    )

    # One review per order (take latest if multiple)
    df = df.sort_values("review_creation_date", ascending=False)
    df = df.drop_duplicates(subset=["order_id"], keep="first")

    logger.info(f"[REVIEWS] Clean complete: {len(df):,} reviews")
    return df

# ── Step 4: Build master transactions table ───────────────────────────────────

def build_master_table(
    orders:       pd.DataFrame,
    order_agg:    pd.DataFrame,
    customers:    pd.DataFrame,
    products:     pd.DataFrame,
    order_items:  pd.DataFrame,
    payments:     pd.DataFrame,
    reviews:      pd.DataFrame,
) -> pd.DataFrame:
    logger.info("[MASTER] Building master transactions table...")

    # Start with orders as the spine
    master = orders.copy()

    # Join order aggregations (revenue per order)
    master = master.merge(order_agg, on="order_id", how="left")

    # Join customers
    customer_cols = ["customer_id", "customer_unique_id", "customer_state", "customer_city"]
    available_cols = [c for c in customer_cols if c in customers.columns]
    master = master.merge(customers[available_cols], on="customer_id", how="left")

    # Join payments
    master = master.merge(payments, on="order_id", how="left")

    # Join reviews
    review_cols = ["order_id", "review_score", "sentiment"]
    master = master.merge(reviews[review_cols], on="order_id", how="left")

    # Join one product per order (most expensive item in that order)
    if "product_id" in order_items.columns and "category_en" in products.columns:
        top_item = order_items.sort_values("price", ascending=False)
        top_item = top_item.drop_duplicates(subset=["order_id"], keep="first")
        top_item = top_item.merge(
            products[["product_id", "category_en"]],
            on="product_id",
            how="left",
        )
        master = master.merge(
            top_item[["order_id", "product_id", "category_en"]],
            on="order_id",
            how="left",
        )

    # Fill nulls with sensible defaults
    master["n_items"]           = master.get("n_items", pd.Series(1)).fillna(1).astype(int)
    master["total_order_value"] = master.get("total_order_value", pd.Series(0)).fillna(0)
    master["review_score"]      = master.get("review_score", pd.Series(np.nan))
    master["category_en"]       = master.get("category_en", pd.Series("unknown")).fillna("unknown")
    master["sentiment"]         = master.get("sentiment", pd.Series("unknown")).fillna("unknown")

    # Discount flag (will be computed properly in Module 4, placeholder here)
    # Logic: if payment < total_order_value, a discount was applied
    master["discount_applied"] = (
        master["total_payment"].fillna(0) < master["total_order_value"].fillna(0)
    )
    master["discount_amount"] = (
        master["total_order_value"].fillna(0) - master["total_payment"].fillna(0)
    ).clip(lower=0)

    logger.info(f"[MASTER] Built: {len(master):,} rows × {len(master.columns)} columns")
    return master

# ── Step 5: Save all processed files ─────────────────────────────────────────

def save_processed(name: str, df: pd.DataFrame) -> Path:
    """Save a clean DataFrame to data/processed/ as CSV."""
    filename = PROCESSED_FILES.get(name, f"{name}_clean.csv")
    filepath = PROCESSED_DIR / filename
    df.to_csv(filepath, index=False)
    logger.info(f"[SAVED] {name}: {filepath} ({len(df):,} rows)")
    return filepath

# ── Main transform pipeline ───────────────────────────────────────────────────

def run_transform(datasets: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    logger.info("=" * 60)
    logger.info("ShopScope | Module 1 | Transform Starting")
    logger.info("=" * 60)

    clean = {}

    # 1. Clean each table
    if "orders" in datasets:
        clean["orders"] = clean_orders(datasets["orders"])

    if "order_items" in datasets:
        clean["order_items"], clean["order_agg"] = clean_order_items(datasets["order_items"])

    if "customers" in datasets:
        clean["customers"] = clean_customers(datasets["customers"])

    if "products" in datasets:
        cat_names = datasets.get("category_names")
        clean["products"] = clean_products(datasets["products"], cat_names)

    if "payments" in datasets:
        clean["payments"] = clean_payments(datasets["payments"])

    if "reviews" in datasets:
        clean["reviews"] = clean_reviews(datasets["reviews"])

    # 2. Build master table
    if all(k in clean for k in ["orders", "order_agg", "customers", "products", "order_items", "payments", "reviews"]):
        clean["master"] = build_master_table(
            orders      = clean["orders"],
            order_agg   = clean["order_agg"],
            customers   = clean["customers"],
            products    = clean["products"],
            order_items = clean["order_items"],
            payments    = clean["payments"],
            reviews     = clean["reviews"],
        )

    # 3. Save all processed files
    for name, df in clean.items():
        if name != "order_agg":   # internal intermediate — don't save separately
            save_processed(name, df)

    logger.info("=" * 60)
    logger.info(f"Transform complete. {len(clean)} tables ready.")
    logger.info("=" * 60)

    return clean

# ── Run directly to test ──────────────────────────────────────────────────────
if __name__ == "__main__":
    from extract import load_all_datasets
    from validate import validate_all, has_critical_issues

    datasets = load_all_datasets()
    report   = validate_all(datasets)

    if has_critical_issues(report):
        print("STOP: Critical validation issues — transform aborted")
    else:
        clean = run_transform(datasets)
        print("\nMaster table columns:")
        if "master" in clean:
            print(clean["master"].dtypes)
            print(f"\nSample row:\n{clean['master'].iloc[0]}")
