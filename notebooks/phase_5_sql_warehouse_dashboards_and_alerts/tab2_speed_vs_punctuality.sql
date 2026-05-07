-- Monthly overlay: average review score vs. late delivery %.
-- The cornerstone visualization for the speed-vs-punctuality finding.
-- Both metrics computed over the same population (orders that received a review),
-- making the inverse correlation numerically interpretable.
-- Source: gold.review_trends.
SELECT
  review_month_start                              AS month_start,
  ROUND(avg_review_score, 2)                      AS avg_review_score,
  late_delivery_pct_for_reviewed_orders           AS late_delivery_pct,
  review_count
FROM olist_lakehouse_us.gold.review_trends
WHERE review_month_start IS NOT NULL
ORDER BY review_month_start;