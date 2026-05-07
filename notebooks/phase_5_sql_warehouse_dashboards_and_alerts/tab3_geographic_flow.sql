-- Top 30 customer-state x seller-state revenue routes.
-- Surfaces the SP-as-hub finding: SP-to-SP and SP-to-other-states dominate.
-- Includes intra/inter flag for routing-pattern analysis.
-- Source: gold.geographic_metrics (Phase 3, ≥5 orders/route).
SELECT
  customer_state,
  seller_state,
  CASE WHEN is_intra_state THEN 'Intra-state' ELSE 'Cross-state' END AS route_type,
  order_count,
  ROUND(total_revenue, 0)             AS revenue_brl,
  ROUND(avg_freight_value, 2)         AS avg_freight_brl,
  ROUND(avg_delivery_days, 1)         AS avg_delivery_days,
  strict_late_rate_pct                AS strict_late_pct
FROM olist_lakehouse_us.gold.geographic_metrics
ORDER BY total_revenue DESC
LIMIT 30;