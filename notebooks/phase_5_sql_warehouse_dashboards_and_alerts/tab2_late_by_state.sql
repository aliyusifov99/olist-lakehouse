-- Per-state delivery performance.
-- Surfaces both lateness definitions: strict (vs. customer-facing promise)
-- and SLA (vs. operational threshold). Their gap quantifies estimate-padding.
-- Source: gold.delivery_performance (wide-format, ≥10 orders/state).
SELECT
  seller_state,
  total_delivered_orders                AS orders,
  ROUND(avg_delivery_days, 1)           AS avg_days_to_deliver,
  ROUND(avg_delay_days, 1)              AS avg_days_vs_estimate,
  strict_late_pct,
  sla_late_pct,
  CASE
    WHEN strict_late_pct < 10 THEN 'low'
    WHEN strict_late_pct < 20 THEN 'medium'
    ELSE 'high'
  END AS severity_bucket
FROM (
  SELECT
    seller_state,
    total_delivered_orders,
    avg_delivery_days,
    avg_delay_days,
    strict_late_rate_pct AS strict_late_pct,
    sla_late_rate_pct    AS sla_late_pct
  FROM olist_lakehouse_us.gold.delivery_performance
)
ORDER BY strict_late_pct DESC;