-- Monthly revenue across the dataset's full timespan.
-- One row per month_start; we sum across categories so the line is total revenue.
SELECT
  month_start,
  ROUND(SUM(total_revenue), 2) AS revenue_brl,
  SUM(order_count)             AS orders
FROM olist_lakehouse_us.gold.monthly_revenue
GROUP BY month_start
ORDER BY month_start;