import sys
import warnings
from pathlib import Path

import pandas as pd
import numpy as np
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore")
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import EXPORTS_DIR

# ══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title   = "ShopScope | Retail Intelligence",
    page_icon    = "🛍",
    layout       = "wide",
    initial_sidebar_state = "expanded",
)

# Custom CSS — clean, professional, minimal
st.markdown("""
<style>
    .metric-card {
        background: #f8f9fb; border-radius: 10px;
        padding: 16px 20px; border-left: 4px solid #4f46e5;
    }
    .metric-card h2 { font-size: 28px; font-weight: 700; color: #1a1a2e; margin: 0; }
    .metric-card p  { font-size: 13px; color: #6b7280; margin: 4px 0 0; }
    .alert-HIGH   { background:#fef2f2; border-left:4px solid #ef4444; border-radius:8px; padding:12px 16px; margin:6px 0; }
    .alert-MEDIUM { background:#fffbeb; border-left:4px solid #f59e0b; border-radius:8px; padding:12px 16px; margin:6px 0; }
    .alert-LOW    { background:#f0fdf4; border-left:4px solid #22c55e; border-radius:8px; padding:12px 16px; margin:6px 0; }
    .seg-badge { display:inline-block; padding:2px 10px; border-radius:12px; font-size:12px; font-weight:600; }
    div[data-testid="stMetricValue"] { font-size: 28px !important; }
    .stSelectbox label { font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# DATA LOADER — cached so it doesn't reload on every interaction
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)   # refresh every 5 minutes
def load_data() -> dict[str, pd.DataFrame | None]:
    """Load all export CSVs. Returns None for missing files (graceful degradation)."""
    files = {
        "monthly_kpis":       "monthly_kpis.csv",
        "customer_segments":  "customer_segments.csv",
        "segment_summary":    "segment_summary.csv",
        "regional_targets":   "regional_campaign_targets.csv",
        "campaign_roi":       "campaign_roi_summary.csv",
        "discount_impact":    "discount_impact_analysis.csv",
        "seasonal_trends":    "seasonal_trends.csv",
        "anomaly_alerts":     "anomaly_alerts.csv",
        "anomaly_summary":    "anomaly_summary.csv",
        "category_agg":       "category_aggregations.csv",
    }
    data = {}
    for key, fname in files.items():
        path = EXPORTS_DIR / fname
        data[key] = pd.read_csv(path) if path.exists() else None
    return data


def fmt_currency(val: float) -> str:
    if val >= 1_000_000: return f"${val/1_000_000:.1f}M"
    if val >= 1_000:     return f"${val/1_000:.1f}K"
    return f"${val:,.0f}"

def fmt_pct(val: float) -> str:
    return f"{val*100:.1f}%"

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════

def render_sidebar(data: dict) -> str:
    with st.sidebar:
        st.markdown("### 🛍 ShopScope")
        st.markdown("**Retail Campaign Intelligence**")
        st.markdown("---")

        page = st.radio(
            "Navigate",
            ["📈 Campaign Intelligence",
             "👤 Customer Intelligence",
             "🗺 Geographic Opportunity",
             "⚠️ Anomaly Monitor"],
            label_visibility="collapsed",
        )

        st.markdown("---")

        # Data freshness indicators
        st.markdown("**Data status**")
        status_map = {
            "monthly_kpis":      "Revenue trends",
            "customer_segments": "Customer scores",
            "regional_targets":  "Regional data",
            "anomaly_alerts":    "Anomaly feed",
        }
        for key, label in status_map.items():
            icon = "🟢" if data.get(key) is not None else "🔴"
            st.markdown(f"{icon} {label}")

        st.markdown("---")
        st.markdown("**ShopScope v1.0**")
        st.markdown("Olist Brazilian E-Commerce")
        st.caption("Built by Shreya Joshi · 2026")

    return page

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — CAMPAIGN INTELLIGENCE DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

def page_campaign_intelligence(data: dict):
    st.title("📈 Campaign Intelligence Dashboard")
    st.markdown("Revenue trends, campaign ROI, and seasonal opportunity analysis.")

    # ── KPI Row ───────────────────────────────────────────────────────────────
    monthly = data.get("monthly_kpis")
    roi     = data.get("campaign_roi")
    seg     = data.get("segment_summary")

    c1, c2, c3, c4 = st.columns(4)

    if monthly is not None:
        total_rev  = monthly["total_revenue"].sum()
        total_ord  = monthly["total_orders"].sum()
        latest_mom = monthly["mom_revenue_growth"].dropna().iloc[-1] if "mom_revenue_growth" in monthly.columns else 0
        c1.metric("Total Revenue",    fmt_currency(total_rev))
        c2.metric("Total Orders",     f"{int(total_ord):,}")
        c3.metric("MoM Growth (latest)", f"{latest_mom*100:+.1f}%" if latest_mom else "N/A")
    else:
        c1.metric("Total Revenue",  "Run pipeline")
        c2.metric("Total Orders",   "Run pipeline")
        c3.metric("MoM Growth",     "Run pipeline")

    if seg is not None:
        total_customers = seg["customer_count"].sum()
        c4.metric("Total Customers", f"{int(total_customers):,}")

    st.markdown("---")

    # ── Revenue trend chart ───────────────────────────────────────────────────
    if monthly is not None:
        st.subheader("Monthly Revenue Trend")
        monthly_sorted = monthly.sort_values(["purchase_year", "purchase_month"])
        monthly_sorted["period"] = (
            monthly_sorted["purchase_year"].astype(str) + "-"
            + monthly_sorted["purchase_month"].astype(str).str.zfill(2)
        )

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(x=monthly_sorted["period"], y=monthly_sorted["total_revenue"],
                   name="Revenue", marker_color="#4f46e5", opacity=0.85),
            secondary_y=False,
        )
        if "mom_revenue_growth" in monthly_sorted.columns:
            fig.add_trace(
                go.Scatter(x=monthly_sorted["period"],
                           y=monthly_sorted["mom_revenue_growth"] * 100,
                           name="MoM Growth %", mode="lines+markers",
                           line=dict(color="#f59e0b", width=2),
                           marker=dict(size=5)),
                secondary_y=True,
            )
        fig.update_layout(
            height=360, plot_bgcolor="white", paper_bgcolor="white",
            legend=dict(orientation="h", y=1.1),
            margin=dict(l=0, r=0, t=20, b=0),
        )
        fig.update_yaxes(title_text="Revenue ($)", secondary_y=False)
        fig.update_yaxes(title_text="MoM Growth (%)", secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Monthly KPI data not available. Run: python run_module3.py")

    col_left, col_right = st.columns(2)

    # ── Campaign ROI by segment ───────────────────────────────────────────────
    with col_left:
        st.subheader("Campaign ROI by Segment")
        if roi is not None and "campaign_roi_pct" in roi.columns:
            fig = px.bar(
                roi.sort_values("campaign_roi_pct"),
                x="campaign_roi_pct", y="segment",
                orientation="h",
                color="campaign_roi_pct",
                color_continuous_scale=["#ef4444", "#f59e0b", "#22c55e"],
                labels={"campaign_roi_pct": "ROI (%)", "segment": ""},
            )
            fig.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                              coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Run: python run_module5.py")

    # ── Seasonal index ────────────────────────────────────────────────────────
    with col_right:
        st.subheader("Seasonal Index by Month")
        seasonal = data.get("seasonal_trends")
        if seasonal is not None and "seasonal_index" in seasonal.columns:
            seasonal_sorted = seasonal.sort_values("purchase_month")
            month_avg = seasonal_sorted.groupby("purchase_month")["seasonal_index"].mean().reset_index()
            month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
                           7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}
            month_avg["month_name"] = month_avg["purchase_month"].map(month_names)
            colors = ["#ef4444" if v > 1.15 else "#f59e0b" if v > 1.0 else "#6b7280"
                      for v in month_avg["seasonal_index"]]
            fig = px.bar(month_avg, x="month_name", y="seasonal_index",
                         color_discrete_sequence=colors)
            fig.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="Avg")
            fig.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Run: python run_module5.py")

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — CUSTOMER INTELLIGENCE DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

def page_customer_intelligence(data: dict):
    st.title("👤 Customer Intelligence Dashboard")
    st.markdown("Health scores, RFM segments, and discount dependency analysis.")

    seg_summary = data.get("segment_summary")
    segments    = data.get("customer_segments")
    discount    = data.get("discount_impact")

    # ── KPI Row ───────────────────────────────────────────────────────────────
    if segments is not None:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Customers",      f"{len(segments):,}")
        c2.metric("Avg Health Score",     f"{segments['health_score'].mean():.1f}/100")
        hv_count = len(segments[segments["segment"] == "High Value"])
        c3.metric("High Value Customers", f"{hv_count:,}")
        disc_dep  = len(segments[segments["segment"] == "Discount Dependent"])
        c4.metric("Discount Dependent",   f"{disc_dep:,}")
        st.markdown("---")

    col_left, col_right = st.columns([1.2, 1])

    # ── Segment donut ─────────────────────────────────────────────────────────
    with col_left:
        st.subheader("Customer Segments")
        if seg_summary is not None:
            seg_colors = {
                "High Value":         "#4f46e5",
                "Loyal Customers":    "#06b6d4",
                "At-Risk Customers":  "#f59e0b",
                "Discount Dependent": "#ef4444",
                "Inactive Customers": "#9ca3af",
            }
            colors = [seg_colors.get(s, "#6b7280") for s in seg_summary["segment"]]
            fig = go.Figure(go.Pie(
                labels=seg_summary["segment"],
                values=seg_summary["customer_count"],
                hole=0.55,
                marker_colors=colors,
            ))
            fig.update_layout(
                height=320, margin=dict(l=0,r=0,t=10,b=0),
                legend=dict(orientation="h", y=-0.1, font_size=11),
            )
            st.plotly_chart(fig, use_container_width=True)

            # Segment table
            display_cols = ["segment","customer_count","total_revenue","avg_health_score"]
            avail = [c for c in display_cols if c in seg_summary.columns]
            if avail:
                st.dataframe(
                    seg_summary[avail].rename(columns={
                        "customer_count": "Customers",
                        "total_revenue": "Revenue ($)",
                        "avg_health_score": "Avg Health",
                    }),
                    use_container_width=True, hide_index=True,
                )
        else:
            st.info("Run: python run_module4.py")

    # ── Health score distribution ─────────────────────────────────────────────
    with col_right:
        st.subheader("Health Score Distribution")
        if segments is not None:
            fig = px.histogram(
                segments, x="health_score", nbins=30,
                color_discrete_sequence=["#4f46e5"],
            )
            fig.add_vline(x=segments["health_score"].mean(), line_dash="dash",
                          line_color="#f59e0b",
                          annotation_text=f"Avg: {segments['health_score'].mean():.0f}")
            fig.update_layout(height=200, margin=dict(l=0,r=0,t=10,b=0),
                              showlegend=False)
            st.plotly_chart(fig, use_container_width=True)

        # Discount impact
        st.subheader("Discount Impact on Margin")
        if discount is not None:
            fig = px.bar(
                discount, x="discount_band", y=["total_revenue","estimated_margin"],
                barmode="group",
                color_discrete_sequence=["#4f46e5","#22c55e"],
                labels={"value":"$","variable":"Metric"},
            )
            fig.update_layout(height=200, margin=dict(l=0,r=0,t=10,b=0),
                              legend=dict(orientation="h", y=1.15, font_size=10))
            fig.update_xaxes(tickangle=-15)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Run: python run_module5.py")

    # ── RFM scatter ───────────────────────────────────────────────────────────
    if segments is not None and all(c in segments.columns for c in ["r_score","m_score","health_score"]):
        st.subheader("RFM Score Distribution — Recency vs Monetary")
        sample = segments.sample(min(2000, len(segments)), random_state=42)
        seg_colors = {
            "High Value":"#4f46e5","Loyal Customers":"#06b6d4",
            "At-Risk Customers":"#f59e0b","Discount Dependent":"#ef4444",
            "Inactive Customers":"#9ca3af",
        }
        fig = px.scatter(
            sample, x="r_score", y="m_score",
            color="segment", size="health_score",
            color_discrete_map=seg_colors,
            labels={"r_score":"Recency Score","m_score":"Monetary Score"},
            opacity=0.7,
        )
        fig.update_layout(height=350, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — GEOGRAPHIC OPPORTUNITY DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════

def page_geographic(data: dict):
    st.title("🗺 Geographic Opportunity Dashboard")
    st.markdown("Regional campaign targeting scores and market opportunity analysis.")

    regional = data.get("regional_targets")
    if regional is None:
        st.info("No regional data found. Run: python run_module5.py")
        return

    # ── KPI Row ───────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Regions Analyzed", len(regional))
    high_prio = len(regional[regional["campaign_priority"] == "high_priority"]) if "campaign_priority" in regional.columns else "N/A"
    c2.metric("High Priority Regions", high_prio)
    c3.metric("Total Revenue",  fmt_currency(regional["total_revenue"].sum()))
    c4.metric("Total Customers", f"{int(regional['unique_customers'].sum()):,}")
    st.markdown("---")

    col_left, col_right = st.columns(2)

    # ── Opportunity score bar chart ───────────────────────────────────────────
    with col_left:
        st.subheader("Opportunity Score by State (Top 15)")
        top15 = regional.nlargest(15, "opportunity_score")
        priority_colors = {
            "high_priority":   "#4f46e5",
            "medium_priority": "#f59e0b",
            "low_priority":    "#9ca3af",
        }
        color_col = top15["campaign_priority"].map(priority_colors).fillna("#9ca3af") \
            if "campaign_priority" in top15.columns else "#4f46e5"

        fig = px.bar(
            top15.sort_values("opportunity_score"),
            x="opportunity_score", y="customer_state",
            orientation="h",
            color="campaign_priority" if "campaign_priority" in top15.columns else None,
            color_discrete_map=priority_colors,
            labels={"opportunity_score":"Opportunity Score","customer_state":"State"},
        )
        fig.update_layout(height=420, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

    # ── Revenue vs repeat rate scatter ────────────────────────────────────────
    with col_right:
        st.subheader("Revenue vs Repeat Purchase Rate")
        fig = px.scatter(
            regional, x="repeat_rate", y="total_revenue",
            size="unique_customers", color="opportunity_score",
            text="customer_state",
            color_continuous_scale="Viridis",
            labels={"repeat_rate":"Repeat Rate","total_revenue":"Total Revenue ($)"},
        )
        fig.update_traces(textposition="top center", textfont_size=9)
        fig.update_layout(height=420, margin=dict(l=0,r=0,t=10,b=0))
        st.plotly_chart(fig, use_container_width=True)

    # ── Full regional table ───────────────────────────────────────────────────
    st.subheader("Full Regional Summary")
    show_cols = [c for c in ["customer_state","unique_customers","total_revenue",
                              "repeat_rate","avg_order_value","opportunity_score",
                              "campaign_priority","campaign_recommendation"]
                 if c in regional.columns]
    display = regional[show_cols].sort_values("opportunity_score", ascending=False)
    display["total_revenue"] = display["total_revenue"].apply(lambda v: fmt_currency(v))
    st.dataframe(display, use_container_width=True, hide_index=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — ANOMALY MONITOR
# ══════════════════════════════════════════════════════════════════════════════

def page_anomaly_monitor(data: dict):
    st.title("⚠️ Anomaly Monitor")
    st.markdown("Real-time statistical anomaly detection across all KPI streams.")

    alerts  = data.get("anomaly_alerts")
    summary = data.get("anomaly_summary")

    # ── KPI badges ────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns(4)
    if alerts is not None and not alerts.empty:
        high   = len(alerts[alerts["severity"] == "HIGH"])
        medium = len(alerts[alerts["severity"] == "MEDIUM"])
        low    = len(alerts[alerts["severity"] == "LOW"])
        total  = len(alerts)
    else:
        high = medium = low = total = 0

    c1.metric("🔴 HIGH",    high,   delta="⚠ Immediate action" if high > 0 else None, delta_color="inverse")
    c2.metric("🟡 MEDIUM",  medium)
    c3.metric("🟢 LOW",     low)
    c4.metric("Total Alerts", total)
    st.markdown("---")

    if alerts is None or alerts.empty:
        st.success("✅ No anomalies detected. All KPI streams within normal range.")
        st.info("Run the full pipeline to populate anomaly detection:\n"
                "```python run_module6.py```")
        return

    # ── Filters ───────────────────────────────────────────────────────────────
    col_f1, col_f2 = st.columns(2)
    with col_f1:
        sev_filter = st.multiselect(
            "Filter by severity",
            options=["HIGH","MEDIUM","LOW"],
            default=["HIGH","MEDIUM","LOW"],
        )
    with col_f2:
        type_options = alerts["anomaly_type"].unique().tolist()
        type_filter = st.multiselect("Filter by type", options=type_options, default=type_options)

    filtered = alerts[
        alerts["severity"].isin(sev_filter) &
        alerts["anomaly_type"].isin(type_filter)
    ]

    # ── Alert cards ───────────────────────────────────────────────────────────
    st.subheader(f"Active Alerts ({len(filtered)})")

    severity_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
    filtered_sorted = filtered.sort_values(
        "severity", key=lambda s: s.map(severity_order)
    )

    for _, row in filtered_sorted.iterrows():
        icon = {"HIGH":"🔴","MEDIUM":"🟡","LOW":"🟢"}.get(row["severity"],"⚪")
        st.markdown(f"""
        <div class="alert-{row['severity']}">
            <strong>{icon} [{row['severity']}] {row['anomaly_type'].replace('_',' ').title()}</strong>
            &nbsp;·&nbsp; <code>{row['dimension']}</code> &nbsp;·&nbsp; {row['metric']}
            <br>
            Actual: <strong>{row['actual_value']:,.2f}</strong>
            &nbsp; Baseline: {row['baseline_mean']:,.2f}
            &nbsp; Deviation: <strong>{row['pct_deviation']:+.1f}%</strong>
            &nbsp; Z-score: {row['z_score']:.2f}
            &nbsp; Direction: {row['direction']}
        </div>
        """, unsafe_allow_html=True)

    # ── Summary chart ─────────────────────────────────────────────────────────
    if summary is not None and not summary.empty:
        st.markdown("---")
        st.subheader("Alert Distribution by Type")
        fig = px.bar(
            summary, x="anomaly_type", y="count", color="severity",
            color_discrete_map={"HIGH":"#ef4444","MEDIUM":"#f59e0b","LOW":"#22c55e"},
            barmode="stack",
        )
        fig.update_layout(height=300, margin=dict(l=0,r=0,t=10,b=0))
        fig.update_xaxes(tickangle=-20)
        st.plotly_chart(fig, use_container_width=True)

# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    data = load_data()
    page = render_sidebar(data)

    if   "Campaign"    in page: page_campaign_intelligence(data)
    elif "Customer"    in page: page_customer_intelligence(data)
    elif "Geographic"  in page: page_geographic(data)
    elif "Anomaly"     in page: page_anomaly_monitor(data)


if __name__ == "__main__":
    main()
