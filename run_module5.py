import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EXPORTS_DIR, LOG_FORMAT, LOG_LEVEL
from analytics.campaign_analysis import run_campaign_analysis_pipeline

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.run_module5")

def run():
    print("\n" + "=" * 60)
    print("  ShopScope | Module 5 | Campaign Analytics Engine")
    print("=" * 60 + "\n")

    results = run_campaign_analysis_pipeline()

    print("\n" + "=" * 60)
    print("  Module 5 Complete")
    print("=" * 60)

    output_files = [
        "campaign_roi_summary.csv",
        "regional_campaign_targets.csv",
        "discount_impact_analysis.csv",
        "seasonal_trends.csv",
        "quarterly_trends.csv",
    ]
    print(f"\n  Outputs produced:")
    for f in output_files:
        p = EXPORTS_DIR / f
        if p.exists():
            import pandas as pd
            rows = len(pd.read_csv(p))
            print(f"    ✓ {f} ({rows:,} rows)")
        else:
            print(f"    ✗ {f} (not produced — check prerequisites)")

    print(f"\n  All files in: {EXPORTS_DIR}")
    print("  Ready for Module 6 (Anomaly Detection) and Module 7 (Tableau)")
    print("=" * 60 + "\n")

    return results

if __name__ == "__main__":
    run()
