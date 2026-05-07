-- Top 10 categories by total revenue, English names.
-- 'unknown' filtered out — it's the bucket for products that didn't translate
SELECT
  category_name_en,
  ROUND(SUM(total_revenue), 2) AS revenue_brl,
  SUM(order_count)             AS orders
FROM olist_lakehouse_us.gold.monthly_revenue
WHERE category_name_en IS NOT NULL
  AND category_name_en <> 'unknown'
GROUP BY category_name_en
ORDER BY revenue_brl DESC
LIMIT 10;