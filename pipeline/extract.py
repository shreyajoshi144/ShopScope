
import logging
import pandas as pd
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import RAW_DIR, OLIST_FILES, LOG_FORMAT, LOG_LEVEL

# ── Logger setup ──────────────────────────────────────────────────────────────
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.extract")


def load_single_dataset(name: str, filename: str) -> pd.DataFrame | None:
    filepath = RAW_DIR / filename

    if not filepath.exists():
        logger.warning(f"[MISSING] {name}: {filepath} not found — skipping")
        return None

    try:
        df = pd.read_csv(filepath, low_memory=False)
        logger.info(f"[LOADED]  {name}: {len(df):,} rows × {len(df.columns)} cols")
        return df

    except Exception as e:
        logger.error(f"[ERROR]   {name}: Failed to load — {e}")
        return None


def load_all_datasets() -> dict[str, pd.DataFrame]:
    """
    Load all Olist datasets defined in config.OLIST_FILES.

    Returns:
        dict mapping logical name → DataFrame
        e.g. {"orders": df_orders, "customers": df_customers, ...}

    Usage:
        datasets = load_all_datasets()
        orders = datasets["orders"]
    """
    logger.info("=" * 60)
    logger.info("ShopScope | Module 1 | Data Extraction Starting")
    logger.info("=" * 60)

    datasets = {}

    for name, filename in OLIST_FILES.items():
        df = load_single_dataset(name, filename)
        if df is not None:
            datasets[name] = df

    # ── Summary report ────────────────────────────────────────────────────────
    total_rows = sum(len(df) for df in datasets.values())
    logger.info("-" * 60)
    logger.info(f"Extraction complete: {len(datasets)}/{len(OLIST_FILES)} files loaded")
    logger.info(f"Total rows across all datasets: {total_rows:,}")

    missing = set(OLIST_FILES.keys()) - set(datasets.keys())
    if missing:
        logger.warning(f"Missing datasets: {missing}")
        logger.warning("Download from: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")

    return datasets


def get_dataset_summary(datasets: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Print a summary table showing rows, columns, and memory for each dataset.
    Useful for quick exploration after loading.
    """
    rows = []
    for name, df in datasets.items():
        rows.append({
            "dataset":  name,
            "rows":     len(df),
            "columns":  len(df.columns),
            "mem_mb":   round(df.memory_usage(deep=True).sum() / 1e6, 2),
            "col_names": ", ".join(df.columns.tolist()),
        })

    summary = pd.DataFrame(rows).sort_values("rows", ascending=False)
    print("\n── Dataset Summary ──────────────────────────────────────────")
    print(summary[["dataset", "rows", "columns", "mem_mb"]].to_string(index=False))
    print("─────────────────────────────────────────────────────────────\n")
    return summary


# ── Run directly to test extraction ───────────────────────────────────────────
if __name__ == "__main__":
    datasets = load_all_datasets()
    get_dataset_summary(datasets)

    # Quick peek at orders
    if "orders" in datasets:
        print("\nOrders sample:")
        print(datasets["orders"].head(3).to_string())
