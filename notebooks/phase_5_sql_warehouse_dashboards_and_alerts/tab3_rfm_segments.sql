-- RFM segment distribution with is_repeat_customer overlay.
-- Surfaces the F-degeneracy finding: textbook RFM segments are ~97% single-purchase
-- customers, meaning frequency-based segmentation is structurally meaningless here.
-- Source: gold.customer_rfm.
SELECT
  customer_segment,
  COUNT(*)                                                                  AS total_customers,
  SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END)                       AS repeat_customers,
  SUM(CASE WHEN NOT is_repeat_customer THEN 1 ELSE 0 END)                   AS single_purchase_customers,
  ROUND(100.0 * SUM(CASE WHEN is_repeat_customer THEN 1 ELSE 0 END) / COUNT(*), 2) AS repeat_pct,
  ROUND(AVG(monetary), 2)                                                   AS avg_spend_brl,
  ROUND(AVG(recency_days), 0)                                               AS avg_recency_days
FROM olist_lakehouse_us.gold.customer_rfm
GROUP BY customer_segment
ORDER BY total_customers DESC;