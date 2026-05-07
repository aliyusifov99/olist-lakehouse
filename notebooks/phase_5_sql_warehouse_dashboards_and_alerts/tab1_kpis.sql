-- Tab 1 KPI tiles. Single 1-row result, three measures.
-- Sourced from monthly_revenue because that's the canonical revenue table
SELECT
  ROUND(SUM(total_revenue), 0)                       AS total_revenue_brl,
  SUM(order_count)                                   AS total_orders,
  ROUND(SUM(total_revenue) / SUM(order_count), 2)    AS avg_order_value_brl
FROM olist_lakehouse_us.gold.monthly_revenue;