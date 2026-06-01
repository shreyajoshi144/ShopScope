import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EXPORTS_DIR, LOG_FORMAT, LOG_LEVEL
from analytics.customer_health import run_customer_health_pipeline

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.run_module4")

def run():
    print("\n" + "=" * 60)
    print("  ShopScope | Module 4 | Customer Intelligence Engine")
    print("=" * 60 + "\n")

    results = run_customer_health_pipeline()

    print("\n" + "=" * 60)
    print("  Module 4 Complete")
    print("=" * 60)

    seg = results["customer_segments"]
    print(f"  Customers scored: {len(seg):,}")
    print(f"  Health score range: {seg['health_score'].min():.1f} – {seg['health_score'].max():.1f}")

    summary = results["segment_summary"]
    print(f"\n  Segments:")
    for _, row in summary.iterrows():
        print(f"    {row['segment']:22s}  {int(row['customer_count']):6,} customers  "
              f"Revenue share: {row['revenue_share']:.1%}")

    print(f"\n  Outputs: {EXPORTS_DIR}/customer_segments.csv")
    print(f"           {EXPORTS_DIR}/segment_summary.csv")
    print("  Ready for Module 5 (Campaign Analytics)")
    print("=" * 60 + "\n")

    return results

if __name__ == "__main__":
    run()
