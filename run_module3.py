import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EXPORTS_DIR, LOG_FORMAT, LOG_LEVEL
from pipeline.spark_jobs import run_all_spark_jobs, SPARK_AVAILABLE

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.run_module3")

def run():
    print("\n" + "=" * 60)
    print("  ShopScope | Module 3 | PySpark Aggregations")
    print(f"  Engine: {'Apache PySpark' if SPARK_AVAILABLE else 'Pandas (install pyspark for full engine)'}")
    print("=" * 60 + "\n")

    results = run_all_spark_jobs()

    print("\n" + "=" * 60)
    print("  Module 3 Complete")
    print("=" * 60)

    for name, df in results.items():
        out = EXPORTS_DIR / f"{name}.csv"
        print(f"  {name}: {len(df):,} rows  →  {out}")

    print(f"\n  All exports ready in: {EXPORTS_DIR}")
    print("  Ready for Module 4 (Customer Intelligence Engine)")
    print("=" * 60 + "\n")

    return results

if __name__ == "__main__":
    run()
