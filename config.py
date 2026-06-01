import os
from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent

# ── Data directories ──────────────────────────────────────────────────────────
RAW_DIR       = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
EXPORTS_DIR   = BASE_DIR / "data" / "exports"

# Create dirs if they don't exist yet
for d in [RAW_DIR, PROCESSED_DIR, EXPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ── Olist dataset files (place these CSVs inside data/raw/) ───────────────────
# Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
OLIST_FILES = {
    "orders":         "olist_orders_dataset.csv",
    "order_items":    "olist_order_items_dataset.csv",
    "customers":      "olist_customers_dataset.csv",
    "products":       "olist_products_dataset.csv",
    "sellers":        "olist_sellers_dataset.csv",
    "payments":       "olist_order_payments_dataset.csv",
    "reviews":        "olist_order_reviews_dataset.csv",
    "category_names": "product_category_name_translation.csv",
    "geolocation":    "olist_geolocation_dataset.csv",
}

# ── Processed output filenames ────────────────────────────────────────────────
PROCESSED_FILES = {
    "orders":        "orders_clean.csv",
    "customers":     "customers_clean.csv",
    "products":      "products_clean.csv",
    "order_items":   "order_items_clean.csv",
    "payments":      "payments_clean.csv",
    "reviews":       "reviews_clean.csv",
    "master":        "master_transactions.csv",   # merged analytics table
}

# ── Data quality thresholds ───────────────────────────────────────────────────
# Module 1 will WARN if any column exceeds these null percentages
NULL_THRESHOLDS = {
    "orders":      0.05,   # max 5% nulls allowed
    "customers":   0.01,
    "order_items": 0.02,
    "payments":    0.02,
    "products":    0.10,   # product descriptions can be sparse
    "reviews":     0.30,   # review text is often missing — that's ok
}

# ── Column schemas ─────────────────────────────────────────────────────────────
# These are the columns we REQUIRE to exist and be non-null in each file.
# Validation will fail hard if any required column is missing entirely.
REQUIRED_COLUMNS = {
    "orders": [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_delivered_customer_date",
    ],
    "order_items": [
        "order_id", "product_id", "seller_id", "price", "freight_value",
    ],
    "customers": [
        "customer_id", "customer_unique_id", "customer_state",
    ],
    "products": [
        "product_id", "product_category_name",
    ],
    "payments": [
        "order_id", "payment_type", "payment_value",
    ],
    "reviews": [
        "order_id", "review_score",
    ],
}

# ── Timestamp columns to parse ─────────────────────────────────────────────────
TIMESTAMP_COLUMNS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "reviews": ["review_creation_date", "review_answer_timestamp"],
}

# ── Business rules ─────────────────────────────────────────────────────────────
VALID_ORDER_STATUSES = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "approved", "created",
]

MIN_PRICE     = 0.01    # orders below this are suspect
MAX_PRICE     = 50000   # orders above this need flagging

# ── Logging ────────────────────────────────────────────────────────────────────
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
