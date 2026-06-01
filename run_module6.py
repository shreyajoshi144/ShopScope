import sys, logging
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import EXPORTS_DIR, LOG_FORMAT, LOG_LEVEL
from analytics.anomaly_detection import run_anomaly_detection_pipeline

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)

def run():
    print("\n" + "="*60)
    print("  ShopScope | Module 6 | Anomaly Detection")
    print("="*60 + "\n")

    result = run_anomaly_detection_pipeline()
    c = result["counts"]

    print("\n" + "="*60)
    print("  Module 6 Complete")
    print("="*60)
    print(f"  HIGH alerts:   {c['high']}")
    print(f"  MEDIUM alerts: {c['medium']}")
    print(f"  LOW alerts:    {c['low']}")
    print(f"\n  Outputs: {EXPORTS_DIR}/anomaly_alerts.csv")
    print("  Ready for Module 7 (Dashboard)")
    print("="*60 + "\n")
    return result

if __name__ == "__main__":
    run()
