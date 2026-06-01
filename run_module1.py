import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import LOG_FORMAT, LOG_LEVEL
from pipeline.extract   import load_all_datasets, get_dataset_summary
from pipeline.validate  import validate_all, has_critical_issues
from pipeline.transform import run_transform

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.main")

def run():
    print("\n" + "=" * 60)
    print("  ShopScope | Module 1 | Full Pipeline Run")
    print("=" * 60 + "\n")

    # ── Step 1: Extract ───────────────────────────────────────────────────────
    print("STEP 1: Extracting raw datasets...")
    datasets = load_all_datasets()

    if not datasets:
        print("\nERROR: No datasets loaded. Place Olist CSV files in data/raw/")
        print("Download: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce")
        sys.exit(1)

    get_dataset_summary(datasets)

    # ── Step 2: Validate ──────────────────────────────────────────────────────
    print("\nSTEP 2: Validating data quality...")
    report = validate_all(datasets)

    if has_critical_issues(report):
        print("\nERROR: Critical validation issues found. Review and fix before proceeding.")
        sys.exit(1)

    print(f"Validation complete: {len(report)} warnings, 0 critical issues")

    # ── Step 3: Transform ─────────────────────────────────────────────────────
    print("\nSTEP 3: Running transformations...")
    clean = run_transform(datasets)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  Module 1 Complete")
    print("=" * 60)
    print(f"  Tables produced: {len(clean)}")

    if "master" in clean:
        master = clean["master"]
        print(f"  Master table: {len(master):,} rows × {len(master.columns)} columns")
        print(f"  Output: data/processed/master_transactions.csv")

    print("\n  Ready for Module 2 (Airflow ETL Pipeline)")
    print("=" * 60 + "\n")

    return clean

if __name__ == "__main__":
    run()
