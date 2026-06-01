-- ═══════════════════════════════════════════════════════════════════════════
-- PURPOSE:
--   These are the SQL queries that power the Tableau dashboards.
--   In production (Azure SQL Database), Tableau connects directly and runs
--   these queries. Locally, they run against the SQLite/pandas layer.
--
--   Resume value: demonstrates SQL proficiency at an analytical level —
--   window functions, CTEs, conditional aggregation, cohort analysis.
-- ═══════════════════════════════════════════════════════════════════════════


-- ── QUERY 1: Customer Lifetime Value (CLV) by Segment ────────────────────────
-- Shows total, average, and potential revenue per segment.
-- Drives the "Customer Intelligence Dashboard" bar chart in Tableau.

WITH customer_metrics AS (
    SELECT
        cs.customer_unique_id,
        cs.segment,
        cs.health_score,
        cs.total_revenue,
        cs.total_orders,
        cs.discount_rate,
        cs.avg_review_score,
        cs.r_score,
        cs.f_score,
        cs.m_score
    FROM customer_segments cs
),
segment_clv AS (
    SELECT
        segment,
        COUNT(customer_unique_id)                       AS customer_count,
        ROUND(SUM(total_revenue), 2)                    AS total_revenue,
        ROUND(AVG(total_revenue), 2)                    AS avg_clv,
        ROUND(PERCENTILE_CONT(0.5)
              WITHIN GROUP (ORDER BY total_revenue), 2) AS median_clv,
        ROUND(MAX(total_revenue), 2)                    AS max_clv,
        ROUND(AVG(health_score), 1)                     AS avg_health_score,
        ROUND(AVG(discount_rate), 4)                    AS avg_discount_rate,
        ROUND(AVG(avg_review_score), 2)                 AS avg_review_score
    FROM customer_metrics
    GROUP BY segment
)
SELECT
    segment,
    customer_count,
    total_revenue,
    avg_clv,
    median_clv,
    max_clv,
    avg_health_score,
    avg_discount_rate,
    ROUND(total_revenue * 1.0 / SUM(total_revenue) OVER(), 4) AS revenue_share,
    ROUND(customer_count * 1.0 / SUM(customer_count) OVER(), 4) AS customer_share
FROM segment_clv
ORDER BY total_revenue DESC;


-- ── QUERY 2: Monthly Revenue Trend with YoY Comparison ───────────────────────
-- Window function: LAG to compare same month in prior year.
-- Drives the time-series line chart in the Campaign Intelligence Dashboard.

WITH monthly AS (
    SELECT
        purchase_year,
        purchase_month,
        SUM(total_order_value)           AS monthly_revenue,
        COUNT(DISTINCT order_id)         AS total_orders,
        COUNT(DISTINCT customer_unique_id) AS unique_customers,
        AVG(total_order_value)           AS avg_order_value,
        AVG(CAST(discount_applied AS FLOAT)) AS discount_rate
    FROM master_transactions
    GROUP BY purchase_year, purchase_month
),
with_yoy AS (
    SELECT
        *,
        LAG(monthly_revenue, 12) OVER (
            ORDER BY purchase_year, purchase_month
        ) AS same_month_prior_year,
        LAG(monthly_revenue, 1) OVER (
            ORDER BY purchase_year, purchase_month
        ) AS prior_month_revenue
    FROM monthly
)
SELECT
    purchase_year,
    purchase_month,
    monthly_revenue,
    total_orders,
    unique_customers,
    avg_order_value,
    discount_rate,
    prior_month_revenue,
    CASE
        WHEN prior_month_revenue IS NOT NULL AND prior_month_revenue > 0
        THEN ROUND((monthly_revenue - prior_month_revenue)
                   / prior_month_revenue * 100, 2)
        ELSE NULL
    END AS mom_growth_pct,
    same_month_prior_year,
    CASE
        WHEN same_month_prior_year IS NOT NULL AND same_month_prior_year > 0
        THEN ROUND((monthly_revenue - same_month_prior_year)
                   / same_month_prior_year * 100, 2)
        ELSE NULL
    END AS yoy_growth_pct
FROM with_yoy
ORDER BY purchase_year, purchase_month;


-- ── QUERY 3: Regional Revenue + Campaign Opportunity Score ────────────────────
-- Powers the Geographic Opportunity Dashboard map in Tableau.

SELECT
    rt.customer_state,
    rt.total_revenue,
    rt.total_orders,
    rt.unique_customers,
    rt.repeat_customers,
    ROUND(rt.repeat_rate, 4)             AS repeat_rate,
    ROUND(rt.avg_order_value, 2)         AS avg_order_value,
    ROUND(rt.avg_review_score, 2)        AS avg_review_score,
    ROUND(rt.discount_order_rate, 4)     AS discount_order_rate,
    ROUND(rt.revenue_per_customer, 2)    AS revenue_per_customer,
    ROUND(rt.opportunity_score, 4)       AS opportunity_score,
    rt.campaign_priority,
    rt.campaign_recommendation
FROM regional_campaign_targets rt
ORDER BY rt.opportunity_score DESC;


-- ── QUERY 4: Discount Dependency Profitability Analysis ──────────────────────
-- Shows margin erosion caused by discount-heavy customers.

WITH discount_bands AS (
    SELECT
        discount_dependency_level,
        COUNT(customer_unique_id)           AS customer_count,
        ROUND(SUM(total_revenue), 2)        AS total_revenue,
        ROUND(AVG(total_revenue), 2)        AS avg_revenue,
        ROUND(AVG(discount_rate), 4)        AS avg_discount_rate,
        ROUND(AVG(avg_review_score), 2)     AS avg_review_score,
        ROUND(AVG(health_score), 1)         AS avg_health_score,
        ROUND(SUM(discount_revenue_impact), 2) AS total_discount_impact
    FROM customer_segments
    GROUP BY discount_dependency_level
)
SELECT
    discount_dependency_level,
    customer_count,
    total_revenue,
    avg_revenue,
    avg_discount_rate,
    total_discount_impact,
    ROUND(total_discount_impact / NULLIF(total_revenue, 0), 4) AS impact_rate,
    avg_health_score,
    ROUND(customer_count * 1.0 / SUM(customer_count) OVER(), 4) AS customer_share,
    ROUND(total_revenue * 1.0 / SUM(total_revenue) OVER(), 4)   AS revenue_share
FROM discount_bands
ORDER BY avg_discount_rate;


-- ── QUERY 5: RFM Cohort Analysis ─────────────────────────────────────────────
-- Groups customers by R+F+M score combination.
-- Powers the customer health heatmap in Tableau.

SELECT
    r_score,
    f_score,
    m_score,
    COUNT(customer_unique_id)           AS customer_count,
    ROUND(AVG(health_score), 1)         AS avg_health,
    ROUND(AVG(total_revenue), 2)        AS avg_revenue,
    ROUND(AVG(total_orders), 1)         AS avg_orders
FROM customer_segments
GROUP BY r_score, f_score, m_score
HAVING COUNT(customer_unique_id) >= 5   -- ignore noise cohorts
ORDER BY avg_health DESC;


-- ── QUERY 6: Anomaly Alert Summary ───────────────────────────────────────────
-- Shows active anomalies for the monitoring section of the dashboard.

SELECT
    anomaly_type,
    dimension,
    metric,
    ROUND(actual_value, 2)              AS actual_value,
    ROUND(baseline_mean, 2)             AS baseline_mean,
    ROUND(pct_deviation, 1)             AS pct_deviation,
    ROUND(z_score, 2)                   AS z_score,
    direction,
    severity,
    detected_at,
    resolved
FROM anomaly_alerts
WHERE resolved = 0
ORDER BY
    CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END,
    ABS(pct_deviation) DESC;
