"""
PURPOSE:
  Analyze marketing campaign effectiveness across 4 dimensions:
    1. Campaign ROI Analysis        → which campaigns generate profit vs just sales
    2. Regional Campaign Targeting  → which states to prioritize for campaigns
    3. Discount Impact Analysis     → are discounts helping or hurting margins?
    4. Seasonal Trend Analysis      → when to run campaigns for maximum impact

  This module answers the core business questions Gap's analytics team asks:
    "Should we run this campaign in São Paulo or Minas Gerais?"
    "Are our discount campaigns profitable after accounting for margin loss?"
    "What's the best month to launch our next retention campaign?"

OUTPUTS (all go to data/exports/):
  campaign_roi_summary.csv       ← revenue vs estimated profit by segment
  regional_campaign_targets.csv  ← scored regions for campaign prioritization
  discount_impact_analysis.csv   ← profitability impact of discount behavior
  seasonal_trends.csv            ← monthly + quarterly revenue trends

WHY THIS MATTERS FOR YOUR RESUME:
  "Built campaign analytics engine quantifying ROI across customer segments,
   identifying high-opportunity regions for targeted marketing, and measuring
   discount impact on profitability — directly supporting retention strategy"
"""

import logging
import pandas as pd
import numpy as np
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import EXPORTS_DIR, LOG_FORMAT, LOG_LEVEL

logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger("shopscope.campaign_analysis")

# SECTION 1 — CAMPAIGN ROI ANALYSIS
# For each customer segment, estimate:
#   - total revenue (what they spent)
#   - estimated gross margin (revenue - estimated COGS and discount costs)
#   - estimated campaign cost (what it would cost to reach them)
#   - campaign ROI = (margin - cost) / cost



# Business assumptions (these would come from a real cost model in production)
GROSS_MARGIN_RATE      = 0.35   # 35% gross margin on product revenue
DISCOUNT_MARGIN_IMPACT = 0.20   # discounts reduce margin by 20% of discount value
CAMPAIGN_COST_PER_CUSTOMER = {
    "High Value":         8.50,  # premium channels, personalized
    "Loyal Customers":    3.00,  # email + push
    "At-Risk Customers":  5.00,  # win-back, heavier touch
    "Discount Dependent": 2.00,  # mass channel
    "Inactive Customers": 1.50,  # cheap bulk channel
}
EXPECTED_RESPONSE_RATE = {
    "High Value":         0.35,
    "Loyal Customers":    0.22,
    "At-Risk Customers":  0.12,
    "Discount Dependent": 0.08,
    "Inactive Customers": 0.03,
}


def compute_campaign_roi(
    segment_summary: pd.DataFrame,
    customer_segments: pd.DataFrame,
) -> pd.DataFrame:
    logger.info("[ROI] Computing campaign ROI by segment...")

    df = segment_summary.copy()

    # ── Estimated gross margin ────────────────────────────────────────────────
    # Discount-heavy segments have reduced margins
    avg_discount_rate = df["avg_discount_rate"].fillna(0)
    df["estimated_margin"] = (
        df["total_revenue"] * GROSS_MARGIN_RATE
        * (1 - avg_discount_rate * DISCOUNT_MARGIN_IMPACT)
    ).round(2)

    # ── Campaign cost total ───────────────────────────────────────────────────
    df["cost_per_customer"] = df["segment"].map(CAMPAIGN_COST_PER_CUSTOMER).fillna(3.0)
    df["campaign_cost_total"] = (df["customer_count"] * df["cost_per_customer"]).round(2)

    # ── Expected responders & incremental revenue ─────────────────────────────
    df["expected_response_rate"] = df["segment"].map(EXPECTED_RESPONSE_RATE).fillna(0.05)
    df["expected_responders"]    = (df["customer_count"] * df["expected_response_rate"]).round(0).astype(int)

    # Incremental revenue = responders × avg order value × assumed 1.5 orders triggered
    df["incremental_revenue"] = (
        df["expected_responders"] * df["avg_revenue"] * 1.5
    ).round(2)

    df["incremental_margin"] = (df["incremental_revenue"] * GROSS_MARGIN_RATE).round(2)

    # ── ROI ───────────────────────────────────────────────────────────────────
    df["campaign_roi"] = (
        (df["incremental_margin"] - df["campaign_cost_total"])
        / df["campaign_cost_total"]
    ).round(4)

    df["campaign_roi_pct"] = (df["campaign_roi"] * 100).round(1)

    # ── Budget allocation recommendation ──────────────────────────────────────
    # Allocate more budget to segments with best ROI and highest revenue impact
    df["roi_score"]       = df["campaign_roi"].rank(pct=True)
    df["revenue_score"]   = df["total_revenue"].rank(pct=True)
    df["allocation_score"]= (df["roi_score"] * 0.6 + df["revenue_score"] * 0.4)

    total_score = df["allocation_score"].sum()
    df["recommended_budget_share"] = (df["allocation_score"] / total_score).round(3)

    # ROI tier
    df["roi_tier"] = pd.cut(
        df["campaign_roi"],
        bins=[-10, 0, 0.5, 1.5, 100],
        labels=["negative", "low", "good", "excellent"]
    )

    logger.info(f"\n── Campaign ROI by Segment ──────────────────────────────────")
    for _, row in df.iterrows():
        logger.info(
            f"  {row['segment']:22s} | "
            f"ROI: {row['campaign_roi_pct']:6.1f}% | "
            f"Budget share: {row['recommended_budget_share']:.1%}"
        )

    return df

# SECTION 2 — REGIONAL CAMPAIGN TARGETING
# Score each state for campaign prioritization.
# A high opportunity score = large market, high repeat rate, growing revenue.
# A low opportunity score = already saturated or structurally low-value.
#
# This feeds the Geographic Opportunity Dashboard in Tableau.


def compute_regional_targets(regional_agg: pd.DataFrame) -> pd.DataFrame:
    """
    Score every state for campaign targeting priority.

    Opportunity score components (each normalized 0–1):
      - Market size:       unique_customers (normalized)
      - Revenue density:   revenue_per_customer (normalized)
      - Growth potential:  inverse of current repeat_rate (lower = more room to grow)
      - Satisfaction:      avg_review_score (normalized)
      - Discount risk:     inverse of discount_order_rate (lower discount dep = safer)
    """
    logger.info("[REGIONAL] Scoring regions for campaign targeting...")

    df = regional_agg.copy()

    def norm(series: pd.Series) -> pd.Series:
        """Min-max normalize a series to 0–1 range."""
        rng = series.max() - series.min()
        if rng == 0:
            return pd.Series(0.5, index=series.index)
        return ((series - series.min()) / rng).round(4)

    # Score components
    df["score_market_size"]     = norm(df["unique_customers"])
    df["score_revenue_density"] = norm(df["revenue_per_customer"].fillna(0))
    df["score_growth_potential"]= norm(1 - df["repeat_rate"].fillna(0))
    df["score_satisfaction"]    = norm(df["avg_review_score"].fillna(3))
    df["score_low_discount_risk"]= norm(1 - df["discount_order_rate"].fillna(0))

    # Composite opportunity score (weighted)
    df["opportunity_score"] = (
        df["score_market_size"]      * 0.30 +
        df["score_revenue_density"]  * 0.25 +
        df["score_growth_potential"] * 0.20 +
        df["score_satisfaction"]     * 0.15 +
        df["score_low_discount_risk"]* 0.10
    ).round(4)

    # Priority tier
    df["campaign_priority"] = pd.qcut(
        df["opportunity_score"],
        q=3,
        labels=["low_priority", "medium_priority", "high_priority"]
    )

    # Campaign type recommendation by tier
    def recommend_campaign_type(row):
        if row["campaign_priority"] == "high_priority":
            return "Aggressive retention + acquisition campaigns"
        elif row["repeat_rate"] < 0.10:
            return "First-time buyer conversion campaigns"
        elif row["discount_order_rate"] > 0.40:
            return "Value messaging — reduce discount dependency"
        else:
            return "Standard retention and loyalty campaigns"

    df["campaign_recommendation"] = df.apply(recommend_campaign_type, axis=1)

    df = df.sort_values("opportunity_score", ascending=False)

    logger.info(f"\n── Top 5 Target Regions ────────────────────────────────────")
    top5 = df.head(5)
    for _, row in top5.iterrows():
        logger.info(
            f"  {str(row['customer_state']).upper():4s} | "
            f"Score: {row['opportunity_score']:.3f} | "
            f"{row['campaign_priority']} | "
            f"Customers: {int(row['unique_customers']):,}"
        )

    out_path = EXPORTS_DIR / "regional_campaign_targets.csv"
    df.to_csv(out_path, index=False)
    logger.info(f"[SAVED] regional_campaign_targets.csv → {len(df)} regions")

    return df

# SECTION 3 — DISCOUNT IMPACT ANALYSIS
# Are we giving away margin for no incremental revenue?
# This analysis separates high-discount vs low-discount customer behavior.

def compute_discount_impact(customer_segments: pd.DataFrame) -> pd.DataFrame:

    logger.info("[DISCOUNT] Analyzing discount impact on profitability...")

    df = customer_segments.copy()
    discount_rate = df["discount_rate"].fillna(0)

    # ── Split into discount bands ─────────────────────────────────────────────
    df["discount_band"] = pd.cut(
        discount_rate,
        bins=[-0.001, 0.10, 0.30, 0.60, 1.001],
        labels=["minimal (0–10%)", "moderate (10–30%)", "high (30–60%)", "extreme (60%+)"]
    )

    # ── Revenue and margin by band ────────────────────────────────────────────
    band_analysis = df.groupby("discount_band", observed=True).agg(
        customer_count     = ("customer_unique_id", "count"),
        total_revenue      = ("total_revenue",       "sum"),
        avg_revenue        = ("total_revenue",       "mean"),
        avg_orders         = ("total_orders",        "mean"),
        avg_health_score   = ("health_score",        "mean"),
    ).reset_index()

    # Estimated margin per band
    band_analysis["estimated_margin_rate"] = (
        GROSS_MARGIN_RATE
        - band_analysis["discount_band"].map({
            "minimal (0–10%)":  0.01,
            "moderate (10–30%)":0.04,
            "high (30–60%)":    0.08,
            "extreme (60%+)":   0.15,
        }).fillna(0)
    )
    band_analysis["estimated_margin"] = (
        band_analysis["total_revenue"] * band_analysis["estimated_margin_rate"]
    ).round(2)

    band_analysis["revenue_share"] = (
        band_analysis["total_revenue"] / band_analysis["total_revenue"].sum()
    ).round(4)

    band_analysis["margin_share"] = (
        band_analysis["estimated_margin"] / band_analysis["estimated_margin"].sum()
    ).round(4)

    # ── Insight: is discount revenue proportionally profitable? ──────────────
    band_analysis["margin_efficiency"] = (
        band_analysis["margin_share"] / band_analysis["revenue_share"]
    ).round(4)
    # margin_efficiency < 1.0 means this band contributes less margin than revenue → discount drag

    # ── Estimated total margin erosion ────────────────────────────────────────
    baseline_margin = df["total_revenue"].sum() * GROSS_MARGIN_RATE
    actual_margin   = band_analysis["estimated_margin"].sum()
    erosion         = baseline_margin - actual_margin

    logger.info(f"[DISCOUNT] Baseline margin (no discounts): ${baseline_margin:,.0f}")
    logger.info(f"[DISCOUNT] Estimated actual margin:        ${actual_margin:,.0f}")
    logger.info(f"[DISCOUNT] Estimated margin erosion:       ${erosion:,.0f}")

    out_path = EXPORTS_DIR / "discount_impact_analysis.csv"
    band_analysis.to_csv(out_path, index=False)
    logger.info(f"[SAVED] discount_impact_analysis.csv → {len(band_analysis)} bands")

    return band_analysis

# SECTION 4 — SEASONAL TREND ANALYSIS
# When does revenue peak? When do campaigns have maximum impact?
# This informs Gap's seasonal marketing calendar.

def compute_seasonal_trends(monthly_kpis: pd.DataFrame) -> pd.DataFrame:

    logger.info("[SEASONAL] Computing seasonal trends...")

    df = monthly_kpis.copy()

    # Ensure correct column types
    if "purchase_year" in df.columns and "purchase_month" in df.columns:
        df = df.sort_values(["purchase_year", "purchase_month"])

        # ── Seasonal index ────────────────────────────────────────────────────
        # Seasonal index = month revenue / overall monthly average
        # index > 1.0 = above-average month (campaign opportunity)
        # index < 1.0 = below-average month (maintain / low investment)
        avg_monthly_revenue = df["total_revenue"].mean()
        df["seasonal_index"] = (df["total_revenue"] / avg_monthly_revenue).round(3)

        df["is_peak_month"] = df["seasonal_index"] > 1.15    # >15% above average
        df["is_trough_month"]= df["seasonal_index"] < 0.85   # >15% below average

        # ── Campaign timing recommendation ────────────────────────────────────
        def campaign_timing(row):
            if row["is_peak_month"]:
                return "High season — maximize campaign spend, acquisition focus"
            elif row["is_trough_month"]:
                return "Low season — reduce spend, loyalty retention focus"
            else:
                return "Normal season — standard campaign cadence"

        df["campaign_timing"] = df.apply(campaign_timing, axis=1)

    # ── Quarterly rollup ──────────────────────────────────────────────────────
    quarterly = None
    if "purchase_year" in df.columns and "purchase_month" in df.columns:
        df["purchase_quarter"] = ((df["purchase_month"] - 1) // 3 + 1)

        quarterly = df.groupby(["purchase_year", "purchase_quarter"]).agg(
            quarterly_revenue     = ("total_revenue",    "sum"),
            quarterly_orders      = ("total_orders",     "sum"),
            quarterly_customers   = ("unique_customers", "sum"),
            avg_monthly_revenue   = ("total_revenue",    "mean"),
        ).reset_index()

        quarterly["qoq_growth"] = quarterly["quarterly_revenue"].pct_change().round(4)

        quarterly_path = EXPORTS_DIR / "quarterly_trends.csv"
        quarterly.to_csv(quarterly_path, index=False)
        logger.info(f"[SAVED] quarterly_trends.csv → {len(quarterly)} quarters")

    # ── Peak months ───────────────────────────────────────────────────────────
    if "is_peak_month" in df.columns:
        peak_months = df[df["is_peak_month"]][["purchase_year", "purchase_month", "total_revenue", "seasonal_index"]]
        logger.info(f"\n── Peak months (seasonal index > 1.15) ──────────────────")
        logger.info(peak_months.to_string(index=False))

    out_path = EXPORTS_DIR / "seasonal_trends.csv"
    df.to_csv(out_path, index=False)
    logger.info(f"[SAVED] seasonal_trends.csv → {len(df)} months")

    return df

# SECTION 5 — MAIN PIPELINE

def run_campaign_analysis_pipeline() -> dict[str, pd.DataFrame]:
    """
    Run the full campaign analytics pipeline.

    Reads from:
      data/exports/segment_summary.csv       (Module 4)
      data/exports/customer_segments.csv     (Module 4)
      data/exports/regional_aggregations.csv (Module 3)
      data/exports/monthly_kpis.csv          (Module 3)

    Writes to:
      data/exports/campaign_roi_summary.csv
      data/exports/regional_campaign_targets.csv
      data/exports/discount_impact_analysis.csv
      data/exports/seasonal_trends.csv
    """
    logger.info("=" * 60)
    logger.info("ShopScope | Module 5 | Campaign Analytics Engine")
    logger.info("=" * 60)

    results = {}

    # ── Load all required inputs ──────────────────────────────────────────────
    def load_required(filename: str, label: str) -> pd.DataFrame | None:
        path = EXPORTS_DIR / filename
        if not path.exists():
            logger.warning(f"[MISSING] {label}: {filename} not found — skipping")
            return None
        df = pd.read_csv(path)
        logger.info(f"[LOADED] {label}: {len(df):,} rows")
        return df

    segment_summary    = load_required("segment_summary.csv",        "Segment summary")
    customer_segments  = load_required("customer_segments.csv",      "Customer segments")
    regional_agg       = load_required("regional_aggregations.csv",  "Regional aggregations")
    monthly_kpis       = load_required("monthly_kpis.csv",           "Monthly KPIs")

    # ── Run each analysis ─────────────────────────────────────────────────────
    if segment_summary is not None and customer_segments is not None:
        logger.info("\nStep 1/4: Campaign ROI Analysis...")
        roi = compute_campaign_roi(segment_summary, customer_segments)
        roi_path = EXPORTS_DIR / "campaign_roi_summary.csv"
        roi.to_csv(roi_path, index=False)
        logger.info(f"[SAVED] campaign_roi_summary.csv → {len(roi)} segments")
        results["campaign_roi"] = roi

    if regional_agg is not None:
        logger.info("\nStep 2/4: Regional Targeting...")
        results["regional_targets"] = compute_regional_targets(regional_agg)

    if customer_segments is not None:
        logger.info("\nStep 3/4: Discount Impact Analysis...")
        results["discount_impact"] = compute_discount_impact(customer_segments)

    if monthly_kpis is not None:
        logger.info("\nStep 4/4: Seasonal Trends...")
        results["seasonal_trends"] = compute_seasonal_trends(monthly_kpis)

    # ── Final summary ─────────────────────────────────────────────────────────
    logger.info("\n" + "=" * 60)
    logger.info("Module 5 Complete")
    logger.info("=" * 60)
    for name, df in results.items():
        logger.info(f"  {name}: {len(df):,} rows")

    logger.info("\nAll exports ready for Tableau dashboards.")
    logger.info("Ready for Module 6 (Anomaly Detection) and Module 7 (Tableau)")

    return results


# ── Run standalone ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    results = run_campaign_analysis_pipeline()

    if "campaign_roi" in results:
        print("\n── Campaign ROI Summary ─────────────────────────────────────")
        print(results["campaign_roi"][
            ["segment", "customer_count", "campaign_roi_pct",
             "recommended_budget_share", "roi_tier"]
        ].to_string(index=False))

    if "regional_targets" in results:
        print("\n── Top Regional Targets ─────────────────────────────────────")
        print(results["regional_targets"][
            ["customer_state", "opportunity_score", "campaign_priority",
             "unique_customers", "total_revenue"]
        ].head(10).to_string(index=False))

    if "seasonal_trends" in results and "is_peak_month" in results["seasonal_trends"].columns:
        print("\n── Peak Months for Campaign Launch ──────────────────────────")
        peaks = results["seasonal_trends"][results["seasonal_trends"]["is_peak_month"]]
        print(peaks[["purchase_year", "purchase_month", "total_revenue", "seasonal_index"]].to_string(index=False))
