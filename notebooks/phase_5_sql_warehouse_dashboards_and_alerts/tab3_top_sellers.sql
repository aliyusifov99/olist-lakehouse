-- Top 20 sellers ranked by composite_score:
-- 40% review + 30% delivery + 30% volume, all on 0-100 scale).
-- Includes component-score breakdown so the bar chart can be stacked,
-- showing what each seller is strong at.
SELECT
  seller_id,
  seller_state,
  seller_city,
  order_count,
  ROUND(total_revenue, 0)                AS revenue_brl,
  avg_review_score,
  ROUND(avg_delivery_days, 1)            AS avg_delivery_days,
  late_rate_pct,
  -- Composite + components
  ROUND(composite_score, 1)              AS composite_score,
  ROUND(review_score_component, 1)       AS pts_from_reviews,
  delivery_score_component               AS pts_from_delivery,
  ROUND(volume_score_component, 1)       AS pts_from_volume,
  performance_tier
FROM olist_lakehouse_us.gold.seller_scorecard
ORDER BY composite_score DESC
LIMIT 20;