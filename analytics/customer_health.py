"""
PURPOSE:
  Compute a Customer Health Score for every customer in the dataset.
  Segment customers into 5 business-meaningful groups.
  This is the core of what Gap Inc's Customer Analytics team does:
  identify WHO your best customers are, WHO is at risk of leaving,
  and WHO should receive which marketing campaign.

WHAT THIS FILE PRODUCES:
  data/exports/customer_segments.csv   ← every customer with segment + score
  data/exports/segment_summary.csv     ← count + revenue per segment (for Tableau)

HOW THE HEALTH SCORE WORKS (RFM-inspired):
  Three pillars, each scored 1–5:
    R = Recency   → how recently did they buy?
    F = Frequency → how many times did they buy?
    M = Monetary  → how much did they spend?
  Combined into a 0–100 health score with weights.
  Additional penalties for high discount dependency and low review scores.

SEGMENTS (used in marketing targeting):
  ┌──────────────────────┬────────────────────────────────────────────────┐
  │ Segment              │ Who they are                                   │
  ├──────────────────────┼────────────────────────────────────────────────┤
  │ High Value           │ High score, high spend, recent, repeat buyer   │
  │ Loyal Customers      │ Repeat buyers, moderate spend, regular recency │
  │ Discount Dependent   │ Only buys during discounts, low profit margin  │
  │ At-Risk Customers    │ Used to buy often, haven't come back recently  │
  │ Inactive Customers   │ Low score, old last purchase, single-order     │
  └──────────────────────┴────────────────────────────────────────────────┘

WHY THIS MATTERS FOR YOUR RESUME:
  "Developed RFM-based customer health scoring system segmenting 90K+
   customers into 5 campaign-actionable groups using recency, frequency,
   and monetary signals — driving targeting strategy for retention analytics"
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import EXPORTS_DIR, PROCESSED_DIR, LOG_FORMAT, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.customer_health")

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 1 — RFM SCORING
# Each dimension scored 1 (worst) to 5 (best) using quintile bucketing.
# Quintiles mean exactly 20% of customers in each bucket — no manual thresholds.
# ══════════════════════════════════════════════════════════════════════════════

def compute_rfm_scores(customer_agg: pd.DataFrame, reference_date: str | None = None) -> pd.DataFrame:
    df = customer_agg.copy()

    # ── Recency (R): days since last purchase ─────────────────────────────────
    if reference_date is None:
        ref = pd.Timestamp.now()
    else:
        ref = pd.Timestamp(reference_date)

    df["last_purchase"] = pd.to_datetime(df["last_purchase"], errors="coerce")
    df["days_since_purchase"] = (ref - df["last_purchase"]).dt.days.fillna(9999)

    # Recency: lower days = better = higher score (invert the quintile)
    df["r_score"] = pd.qcut(
        df["days_since_purchase"].rank(method="first"),
        q=5,
        labels=[5, 4, 3, 2, 1]   # ← inverted: low days → high score
    ).astype(int)

    # ── Frequency (F): number of orders ──────────────────────────────────────
    df["f_score"] = pd.qcut(
        df["total_orders"].rank(method="first"),
        q=5,
        labels=[1, 2, 3, 4, 5]
    ).astype(int)

    # ── Monetary (M): total revenue ───────────────────────────────────────────
    df["m_score"] = pd.qcut(
        df["total_revenue"].rank(method="first"),
        q=5,
        labels=[1, 2, 3, 4, 5]
    ).astype(int)

    # ── Combined RFM score (weighted) ─────────────────────────────────────────
    df["rfm_score"] = (
        df["r_score"] * 0.40 +
        df["m_score"] * 0.35 +
        df["f_score"] * 0.25
    ).round(2)

    logger.info(f"[RFM] Scores computed for {len(df):,} customers")
    logger.info(f"[RFM] Score distribution:\n{df['rfm_score'].describe().round(2)}")

    return df

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 2 — HEALTH SCORE
# Converts RFM into a clean 0–100 score with penalty adjustments.
# ══════════════════════════════════════════════════════════════════════════════

def compute_health_score(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Base score: scale rfm_score (1–5 range) to 0–100
    df["base_score"] = ((df["rfm_score"] - 1) / 4 * 100).round(1)

    # Discount dependency penalty (up to -15 points)
    discount_rate = df["discount_rate"].fillna(0)
    df["discount_penalty"] = (discount_rate * 15).round(1)

    # Low review penalty (up to -10 points for avg score < 2)
    avg_review = df["avg_review_score"].fillna(3)
    df["review_penalty"] = (
        ((2 - avg_review.clip(upper=2)) / 2 * 10)
        .clip(lower=0)
        .round(1)
    )

    # Repeat purchase bonus (+5 for repeat buyers)
    is_repeat = df.get("is_repeat_customer", pd.Series(False, index=df.index))
    df["repeat_bonus"] = is_repeat.astype(int) * 5

    # Final health score
    df["health_score"] = (
        df["base_score"]
        - df["discount_penalty"]
        - df["review_penalty"]
        + df["repeat_bonus"]
    ).clip(0, 100).round(1)

    # Health tier label (for Tableau colour encoding)
    df["health_tier"] = pd.cut(
        df["health_score"],
        bins=[-1, 30, 55, 75, 100],
        labels=["critical", "at_risk", "healthy", "champion"]
    )

    logger.info(f"[HEALTH] Score range: {df['health_score'].min():.1f} – {df['health_score'].max():.1f}")
    logger.info(f"[HEALTH] Tier distribution:\n{df['health_tier'].value_counts()}")

    return df

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 3 — CUSTOMER SEGMENTATION
# Maps every customer to one of 5 named business segments.
# Segments are used by the marketing team to choose campaigns.
# ══════════════════════════════════════════════════════════════════════════════

def assign_segments(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    revenue_p80 = df["total_revenue"].quantile(0.80)
    discount_rate = df["discount_rate"].fillna(0)
    is_repeat = df.get("is_repeat_customer", pd.Series(False, index=df.index))

    conditions = [
        (df["health_score"] >= 75) & (df["total_revenue"] >= revenue_p80),
        (discount_rate >= 0.6),
        (is_repeat) & (df["health_score"] >= 50),
        (is_repeat) & (df["health_score"].between(25, 50)),
    ]
    choices = [
        "High Value",
        "Discount Dependent",
        "Loyal Customers",
        "At-Risk Customers",
    ]

    df["segment"] = np.select(conditions, choices, default="Inactive Customers")

    # Segment priority order (used in campaign planning)
    segment_order = {
        "High Value":          1,
        "Loyal Customers":     2,
        "At-Risk Customers":   3,
        "Discount Dependent":  4,
        "Inactive Customers":  5,
    }
    df["segment_priority"] = df["segment"].map(segment_order)

    logger.info(f"[SEGMENTS] Distribution:\n{df['segment'].value_counts()}")

    return df

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 4 — DISCOUNT DEPENDENCY DEEP DIVE
# Identifies customers who ONLY buy during discounts.
# These customers reduce margin. Marketing should target them differently.
# ══════════════════════════════════════════════════════════════════════════════

def compute_discount_dependency(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    discount_rate = df["discount_rate"].fillna(0)

    # Dependency level
    df["discount_dependency_level"] = pd.cut(
        discount_rate,
        bins=[-0.001, 0.1, 0.3, 0.5, 0.8, 1.001],
        labels=["none", "low", "moderate", "high", "extreme"]
    )

    # Estimate revenue impact
    # Assume average discount depth = 20% of order value when applied
    ASSUMED_DISCOUNT_DEPTH = 0.20
    df["discount_revenue_impact"] = (
        df["total_revenue"] * discount_rate * ASSUMED_DISCOUNT_DEPTH
    ).round(2)

    logger.info(
        f"[DISCOUNT] Dependency levels:\n"
        f"{df['discount_dependency_level'].value_counts()}"
    )

    return df

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 5 — SEGMENT SUMMARY (for Tableau)
# Aggregates segment-level metrics for the Customer Intelligence Dashboard.
# ══════════════════════════════════════════════════════════════════════════════

def build_segment_summary(df: pd.DataFrame) -> pd.DataFrame:
    summary = df.groupby("segment").agg(
        customer_count     = ("customer_unique_id", "count"),
        total_revenue      = ("total_revenue",       "sum"),
        avg_revenue        = ("total_revenue",       "mean"),
        avg_health_score   = ("health_score",        "mean"),
        avg_orders         = ("total_orders",        "mean"),
        avg_discount_rate  = ("discount_rate",       "mean"),
        avg_review_score   = ("avg_review_score",    "mean"),
    ).reset_index()

    summary["revenue_share"] = (
        summary["total_revenue"] / summary["total_revenue"].sum()
    ).round(4)

    summary = summary.sort_values("total_revenue", ascending=False)

    # Campaign recommendation (what Gap's marketing team would do)
    campaign_map = {
        "High Value":         "VIP early access, loyalty rewards, premium campaigns",
        "Loyal Customers":    "Retention emails, referral incentives, upgrade nudges",
        "At-Risk Customers":  "Win-back campaigns, personalized offers, urgency messaging",
        "Discount Dependent": "Gradual discount reduction, value-based messaging",
        "Inactive Customers": "Re-engagement campaigns, broad awareness, heavy discount",
    }
    summary["recommended_campaign"] = summary["segment"].map(campaign_map)

    logger.info(f"\n── Segment Summary ──────────────────────────")
    for _, row in summary.iterrows():
        logger.info(
            f"  {row['segment']:22s} | "
            f"{int(row['customer_count']):6,} customers | "
            f"Revenue share: {row['revenue_share']:.1%} | "
            f"Avg health: {row['avg_health_score']:.1f}"
        )

    return summary

# ══════════════════════════════════════════════════════════════════════════════
# SECTION 6 — MAIN PIPELINE
# ══════════════════════════════════════════════════════════════════════════════

def run_customer_health_pipeline() -> dict[str, pd.DataFrame]:
    logger.info("=" * 60)
    logger.info("ShopScope | Module 4 | Customer Health Engine")
    logger.info("=" * 60)

    # ── Load customer aggregations from Module 3 ──────────────────────────────
    cust_agg_path = EXPORTS_DIR / "customer_aggregations.csv"

    # Fallback: try the KPI file from the Airflow task
    if not cust_agg_path.exists():
        cust_agg_path = EXPORTS_DIR / "customer_kpis.csv"

    if not cust_agg_path.exists():
        raise FileNotFoundError(
            f"customer_aggregations.csv not found at {EXPORTS_DIR}.\n"
            "Run Module 3 first: python run_module3.py"
        )

    df = pd.read_csv(cust_agg_path)
    logger.info(f"Loaded: {len(df):,} customers from {cust_agg_path.name}")

    # Ensure required columns exist with defaults if missing
    if "discount_rate" not in df.columns:
        df["discount_rate"] = 0.0
    if "is_repeat_customer" not in df.columns:
        df["is_repeat_customer"] = df.get("total_orders", 1) > 1
    if "avg_review_score" not in df.columns:
        df["avg_review_score"] = 3.0

    # ── Run all steps ─────────────────────────────────────────────────────────
    logger.info("\nStep 1/4: Computing RFM scores...")
    df = compute_rfm_scores(df)

    logger.info("\nStep 2/4: Computing health scores...")
    df = compute_health_score(df)

    logger.info("\nStep 3/4: Assigning segments...")
    df = assign_segments(df)
    df = compute_discount_dependency(df)

    logger.info("\nStep 4/4: Building segment summary...")
    summary = build_segment_summary(df)

    # ── Save outputs ──────────────────────────────────────────────────────────
    segments_path = EXPORTS_DIR / "customer_segments.csv"
    summary_path  = EXPORTS_DIR / "segment_summary.csv"

    df.to_csv(segments_path, index=False)
    summary.to_csv(summary_path, index=False)

    logger.info(f"\n[SAVED] customer_segments.csv → {len(df):,} customers")
    logger.info(f"[SAVED] segment_summary.csv   → {len(summary)} segments")

    return {"customer_segments": df, "segment_summary": summary}


# ── Run standalone ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    results = run_customer_health_pipeline()

    print("\n── Top 10 High Value Customers ──────────────────────────────")
    hv = results["customer_segments"]
    hv = hv[hv["segment"] == "High Value"].nlargest(10, "health_score")
    print(hv[["customer_unique_id", "total_orders", "total_revenue",
              "health_score", "segment"]].to_string(index=False))

    print("\n── Segment Summary ──────────────────────────────────────────")
    print(results["segment_summary"][
        ["segment", "customer_count", "total_revenue", "revenue_share", "avg_health_score"]
    ].to_string(index=False))
