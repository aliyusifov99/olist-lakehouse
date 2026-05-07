SELECT
  olist_lakehouse_us.silver.delivery_sla_status(delivery_delay_days) AS sla_bucket,
  COUNT(*) AS order_count,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
FROM olist_lakehouse_us.silver.orders
WHERE order_status = 'delivered'
  AND delivery_delay_days IS NOT NULL
GROUP BY 1
ORDER BY
  CASE sla_bucket
    WHEN 'early'         THEN 1
    WHEN 'on_time'       THEN 2
    WHEN 'slightly_late' THEN 3
    WHEN 'very_late'     THEN 4
    ELSE 5
  END;