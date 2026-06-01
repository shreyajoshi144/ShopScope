"""
PURPOSE:
  Detect unusual patterns in retail business data BEFORE they become
  expensive problems. This is what separates a reactive analytics team
  from a proactive one.

  Anomalies detected:
    1. Revenue anomalies        — sudden drops or spikes in monthly revenue
    2. Order volume anomalies   — abnormal drop in order count
    3. Discount spike anomalies — discount rate suddenly jumps (margin risk)
    4. Regional anomalies       — a state's performance drops vs its baseline
    5. Category anomalies       — a product category goes cold unexpectedly
    6. Customer churn signals   — at-risk customer count rising abnormally

DETECTION METHOD: Z-Score + Rolling Baseline
  z = |current_value - rolling_mean| / rolling_std
  severity:
    LOW    → z ≥ 2.0
    MEDIUM → z ≥ 2.5
    HIGH   → z ≥ 3.5

  Same method as InnoVista (which you already built) — but now applied
  to retail business KPIs instead of weather data.

OUTPUTS:
  data/exports/anomaly_alerts.csv        ← all detected anomalies
  data/exports/anomaly_summary.csv       ← count by type + severity

"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import EXPORTS_DIR, LOG_FORMAT, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.anomaly")

# ── Severity thresholds (same logic as InnoVista) ────────────────────────────
SEVERITY_LOW    = 2.0
SEVERITY_MEDIUM = 2.5
SEVERITY_HIGH   = 3.5

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — CORE Z-SCORE ENGINE
# ══════════════════════════════════════════════════════════════════════════════

def compute_zscore(series: pd.Series, window: int = 3) -> pd.Series:
    """
    Compute rolling Z-score for a time series.

    For each point:
      baseline_mean = mean of previous `window` points
      baseline_std  = std  of previous `window` points
      z = |current - baseline_mean| / baseline_std

    Args:
        series: Numeric time series (sorted chronologically)
        window: Rolling window size for baseline (default 3 months)

    Returns:
        Series of z-scores (NaN for first `window` points)
    """
    rolling_mean = series.shift(1).rolling(window=window, min_periods=2).mean()
    rolling_std  = series.shift(1).rolling(window=window, min_periods=2).std()

    # Avoid division by zero on flat series
    rolling_std = rolling_std.replace(0, np.nan)

    z = ((series - rolling_mean) / rolling_std).abs()
    return z.round(3)


def classify_severity(z: float) -> str:
    """Map z-score to LOW / MEDIUM / HIGH / NONE."""
    if pd.isna(z):
        return "NONE"
    if z >= SEVERITY_HIGH:
        return "HIGH"
    if z >= SEVERITY_MEDIUM:
        return "MEDIUM"
    if z >= SEVERITY_LOW:
        return "LOW"
    return "NONE"

def build_alert(
    anomaly_type: str,
    dimension:    str,
    period:       str,
    metric:       str,
    actual_value: float,
    baseline_mean:float,
    z_score:      float,
    direction:    str,   # "spike" or "drop"
    severity:     str,
) -> dict:
    """Build a standardised alert record."""
    return {
        "detected_at":   datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "anomaly_type":  anomaly_type,
        "dimension":     dimension,
        "period":        period,
        "metric":        metric,
        "actual_value":  round(actual_value,  2),
        "baseline_mean": round(baseline_mean, 2),
        "pct_deviation": round((actual_value - baseline_mean) / (baseline_mean + 1e-9) * 100, 1),
        "z_score":       z_score,
        "direction":     direction,
        "severity":      severity,
        "resolved":      False,
    }

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — DETECTOR FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def detect_revenue_anomalies(monthly_kpis: pd.DataFrame) -> list[dict]:
    """
    Detect months where revenue deviates significantly from the rolling baseline.
    Catches both drops (demand collapse, inventory issue) and spikes (data error,
    unusual one-off event that inflates reported numbers).
    """
    alerts = []
    if monthly_kpis is None or monthly_kpis.empty:
        return alerts

    df = monthly_kpis.sort_values(["purchase_year", "purchase_month"]).copy()
    df["period"] = df["purchase_year"].astype(str) + "-" + df["purchase_month"].astype(str).str.zfill(2)

    # Revenue anomalies
    df["revenue_z"] = compute_zscore(df["total_revenue"])

    # Order volume anomalies
    df["orders_z"] = compute_zscore(df["total_orders"])

    # Discount rate anomalies
    if "discount_rate" in df.columns:
        df["discount_z"] = compute_zscore(df["discount_rate"])

    rolling_mean_rev = df["total_revenue"].shift(1).rolling(3, min_periods=2).mean()
    rolling_mean_ord = df["total_orders"].shift(1).rolling(3, min_periods=2).mean()

    for i, row in df.iterrows():
        # Revenue
        z = row.get("revenue_z", np.nan)
        sev = classify_severity(z)
        if sev != "NONE":
            mean_val = rolling_mean_rev.loc[i] if not pd.isna(rolling_mean_rev.loc[i]) else row["total_revenue"]
            direction = "spike" if row["total_revenue"] > mean_val else "drop"
            alerts.append(build_alert(
                "revenue_anomaly", "monthly", row["period"],
                "total_revenue", row["total_revenue"], mean_val, z, direction, sev
            ))

        # Order volume
        z = row.get("orders_z", np.nan)
        sev = classify_severity(z)
        if sev != "NONE":
            mean_val = rolling_mean_ord.loc[i] if not pd.isna(rolling_mean_ord.loc[i]) else row["total_orders"]
            direction = "spike" if row["total_orders"] > mean_val else "drop"
            alerts.append(build_alert(
                "order_volume_anomaly", "monthly", row["period"],
                "total_orders", row["total_orders"], mean_val, z, direction, sev
            ))

        # Discount spike
        if "discount_z" in df.columns:
            z = row.get("discount_z", np.nan)
            sev = classify_severity(z)
            if sev != "NONE" and row.get("discount_rate", 0) > 0.25:
                alerts.append(build_alert(
                    "discount_spike", "monthly", row["period"],
                    "discount_rate", row.get("discount_rate", 0),
                    df["discount_rate"].shift(1).rolling(3, min_periods=2).mean().loc[i] or 0.1,
                    z, "spike", sev
                ))

    logger.info(f"[ANOMALY] Revenue/order detections: {len(alerts)} alerts")
    return alerts

def detect_regional_anomalies(regional_targets: pd.DataFrame) -> list[dict]:
    """
    Flag regions whose opportunity score is abnormally LOW
    (meaning they're significantly underperforming compared to peers).
    Also flag regions with anomalously high discount dependency.
    """
    alerts = []
    if regional_targets is None or regional_targets.empty:
        return alerts

    df = regional_targets.copy()

    # Opportunity score anomalies (regions performing far below average)
    mean_score = df["opportunity_score"].mean()
    std_score  = df["opportunity_score"].std()

    if std_score > 0:
        df["score_z"] = ((df["opportunity_score"] - mean_score) / std_score).abs()

        for _, row in df.iterrows():
            z   = row["score_z"]
            sev = classify_severity(z)
            if sev != "NONE" and row["opportunity_score"] < mean_score:
                alerts.append(build_alert(
                    "regional_underperformance",
                    str(row["customer_state"]).upper(),
                    "current",
                    "opportunity_score",
                    row["opportunity_score"],
                    mean_score,
                    round(z, 3),
                    "drop",
                    sev,
                ))

    # High discount dependency in a region
    mean_disc = df["discount_order_rate"].fillna(0).mean()
    std_disc  = df["discount_order_rate"].fillna(0).std()

    if std_disc > 0:
        for _, row in df.iterrows():
            disc_rate = row.get("discount_order_rate", 0) or 0
            z = abs(disc_rate - mean_disc) / std_disc
            sev = classify_severity(z)
            if sev != "NONE" and disc_rate > mean_disc:
                alerts.append(build_alert(
                    "regional_discount_dependency",
                    str(row["customer_state"]).upper(),
                    "current",
                    "discount_order_rate",
                    disc_rate,
                    mean_disc,
                    round(z, 3),
                    "spike",
                    sev,
                ))

    logger.info(f"[ANOMALY] Regional detections: {len(alerts)} alerts")
    return alerts

def detect_category_anomalies(category_agg: pd.DataFrame) -> list[dict]:
    """
    Flag product categories with abnormally low revenue share or
    abnormally high discount dependency compared to the category mean.
    """
    alerts = []
    if category_agg is None or category_agg.empty:
        return alerts

    df = category_agg.copy()
    df = df[df["total_orders"] > 10]   # ignore micro-categories

    # Revenue anomalies
    mean_rev = df["total_revenue"].mean()
    std_rev  = df["total_revenue"].std()

    if std_rev > 0:
        df["rev_z"] = ((df["total_revenue"] - mean_rev) / std_rev).abs()

        # Only flag categories MUCH lower than average (underperformers)
        for _, row in df[df["total_revenue"] < mean_rev].iterrows():
            z   = row["rev_z"]
            sev = classify_severity(z)
            if sev in ("MEDIUM", "HIGH"):
                alerts.append(build_alert(
                    "category_revenue_drop",
                    str(row["category_en"]),
                    "current",
                    "total_revenue",
                    row["total_revenue"],
                    mean_rev,
                    round(z, 3),
                    "drop",
                    sev,
                ))

    # Low review score anomaly (satisfaction risk)
    if "avg_review_score" in df.columns:
        mean_review = df["avg_review_score"].fillna(3).mean()
        std_review  = df["avg_review_score"].fillna(3).std()

        if std_review > 0:
            for _, row in df.iterrows():
                score = row.get("avg_review_score", 3) or 3
                z = abs(score - mean_review) / std_review
                sev = classify_severity(z)
                if sev != "NONE" and score < mean_review:
                    alerts.append(build_alert(
                        "category_satisfaction_drop",
                        str(row["category_en"]),
                        "current",
                        "avg_review_score",
                        score,
                        mean_review,
                        round(z, 3),
                        "drop",
                        sev,
                    ))

    logger.info(f"[ANOMALY] Category detections: {len(alerts)} alerts")
    return alerts


def detect_churn_signals(segment_summary: pd.DataFrame) -> list[dict]:
    """
    Flag if At-Risk or Inactive customer segments are unusually large.
    A swelling At-Risk segment is an early churn warning.
    """
    alerts = []
    if segment_summary is None or segment_summary.empty:
        return alerts

    total_customers = segment_summary["customer_count"].sum()
    if total_customers == 0:
        return alerts

    # Expected healthy thresholds (benchmarks from retail analytics literature)
    THRESHOLDS = {
        "At-Risk Customers":   0.25,   # alert if > 25% of base is at-risk
        "Inactive Customers":  0.40,   # alert if > 40% are inactive
        "Discount Dependent":  0.30,   # alert if > 30% are discount-dependent
    }

    for _, row in segment_summary.iterrows():
        seg   = row["segment"]
        share = row["customer_count"] / total_customers
        threshold = THRESHOLDS.get(seg)

        if threshold and share > threshold:
            excess  = share - threshold
            z_proxy = excess / 0.05   # treat each 5% excess as 1 sigma
            sev     = classify_severity(z_proxy)

            if sev != "NONE":
                alerts.append(build_alert(
                    "churn_signal",
                    seg,
                    "current",
                    "segment_share",
                    round(share, 4),
                    threshold,
                    round(z_proxy, 2),
                    "spike",
                    sev,
                ))
                logger.warning(
                    f"[CHURN SIGNAL] {seg}: {share:.1%} of customers "
                    f"(threshold {threshold:.0%}) → {sev}"
                )

    logger.info(f"[ANOMALY] Churn signal detections: {len(alerts)} alerts")
    return alerts

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — ALERT DEDUPLICATION + AUTO-RESOLVE
# ══════════════════════════════════════════════════════════════════════════════

def deduplicate_alerts(alerts: list[dict]) -> list[dict]:
    """
    Remove duplicate alerts (same type + dimension + metric).
    Keeps the highest severity version of each duplicate.
    """
    seen = {}
    severity_order = {"HIGH": 3, "MEDIUM": 2, "LOW": 1, "NONE": 0}

    for alert in alerts:
        key = (alert["anomaly_type"], alert["dimension"], alert["metric"])
        if key not in seen:
            seen[key] = alert
        else:
            existing_sev = severity_order.get(seen[key]["severity"], 0)
            new_sev      = severity_order.get(alert["severity"], 0)
            if new_sev > existing_sev:
                seen[key] = alert

    deduped = list(seen.values())
    logger.info(f"[ANOMALY] After dedup: {len(alerts)} → {len(deduped)} alerts")
    return deduped


def build_anomaly_summary(alerts: list[dict]) -> pd.DataFrame:
    """
    Aggregate alerts by type and severity for the dashboard.
    """
    if not alerts:
        return pd.DataFrame(columns=["anomaly_type", "severity", "count"])

    df = pd.DataFrame(alerts)
    summary = (
        df.groupby(["anomaly_type", "severity"])
          .size()
          .reset_index(name="count")
          .sort_values(["severity", "count"], ascending=[False, False])
    )
    return summary

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_anomaly_detection_pipeline() -> dict:
    """
    Run all anomaly detectors across all available KPI streams.

    Reads:
      data/exports/monthly_kpis.csv
      data/exports/regional_campaign_targets.csv
      data/exports/category_aggregations.csv
      data/exports/segment_summary.csv

    Writes:
      data/exports/anomaly_alerts.csv
      data/exports/anomaly_summary.csv
    """
    logger.info("=" * 60)
    logger.info("ShopScope | Module 6 | Anomaly Detection Engine")
    logger.info("=" * 60)

    def safe_load(filename: str) -> pd.DataFrame | None:
        p = EXPORTS_DIR / filename
        if p.exists():
            df = pd.read_csv(p)
            logger.info(f"[LOADED] {filename}: {len(df):,} rows")
            return df
        logger.warning(f"[MISSING] {filename} — skipping this detector")
        return None

    monthly_kpis     = safe_load("monthly_kpis.csv")
    regional_targets = safe_load("regional_campaign_targets.csv")
    category_agg     = safe_load("category_aggregations.csv")
    segment_summary  = safe_load("segment_summary.csv")

    # ── Run all detectors ─────────────────────────────────────────────────────
    all_alerts = []

    logger.info("\nDetector 1/4: Revenue & Order Volume Anomalies...")
    all_alerts += detect_revenue_anomalies(monthly_kpis)

    logger.info("Detector 2/4: Regional Anomalies...")
    all_alerts += detect_regional_anomalies(regional_targets)

    logger.info("Detector 3/4: Category Anomalies...")
    all_alerts += detect_category_anomalies(category_agg)

    logger.info("Detector 4/4: Churn Signals...")
    all_alerts += detect_churn_signals(segment_summary)

    # ── Deduplicate ───────────────────────────────────────────────────────────
    all_alerts = deduplicate_alerts(all_alerts)

    # ── Save ──────────────────────────────────────────────────────────────────
    alerts_df  = pd.DataFrame(all_alerts) if all_alerts else pd.DataFrame(
        columns=["detected_at","anomaly_type","dimension","period","metric",
                 "actual_value","baseline_mean","pct_deviation","z_score",
                 "direction","severity","resolved"]
    )
    summary_df = build_anomaly_summary(all_alerts)

    alerts_df.to_csv(EXPORTS_DIR / "anomaly_alerts.csv",  index=False)
    summary_df.to_csv(EXPORTS_DIR / "anomaly_summary.csv", index=False)

    # ── Report ────────────────────────────────────────────────────────────────
    high   = len(alerts_df[alerts_df["severity"] == "HIGH"])   if not alerts_df.empty else 0
    medium = len(alerts_df[alerts_df["severity"] == "MEDIUM"]) if not alerts_df.empty else 0
    low    = len(alerts_df[alerts_df["severity"] == "LOW"])    if not alerts_df.empty else 0

    logger.info(f"\n── Anomaly Detection Complete ───────────────────────────────")
    logger.info(f"  Total alerts: {len(all_alerts)}")
    logger.info(f"  HIGH:   {high}")
    logger.info(f"  MEDIUM: {medium}")
    logger.info(f"  LOW:    {low}")

    if high > 0:
        logger.warning("⚠  HIGH severity anomalies detected — review immediately")

    return {
        "alerts":  alerts_df,
        "summary": summary_df,
        "counts":  {"high": high, "medium": medium, "low": low},
    }

if __name__ == "__main__":
    result = run_anomaly_detection_pipeline()
    if not result["alerts"].empty:
        print("\n── All Anomaly Alerts ───────────────────────────────────────")
        print(result["alerts"][
            ["anomaly_type","dimension","metric","actual_value",
             "pct_deviation","z_score","severity"]
        ].to_string(index=False))
