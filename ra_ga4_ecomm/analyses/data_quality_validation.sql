-- Comprehensive Data Quality Validation
-- Run this to verify all models have logically consistent data

-- 1. Session metrics validation
WITH session_validation AS (
  SELECT
    'sessions' AS model_name,
    'row_count' AS metric,
    COUNT(*) AS value
  FROM {{ ref('sessions') }}

  UNION ALL

  SELECT
    'sessions' AS model_name,
    'sessions_with_negative_duration' AS metric,
    COUNT(*) AS value
  FROM {{ ref('sessions') }}
  WHERE session_duration_seconds < 0

  UNION ALL

  SELECT
    'sessions' AS model_name,
    'sessions_with_invalid_page_views' AS metric,
    COUNT(*) AS value
  FROM {{ ref('sessions') }}
  WHERE page_views_per_session < 0 OR events_per_session < page_views_per_session

  UNION ALL

  SELECT
    'sessions' AS model_name,
    'conversion_sessions' AS metric,
    COUNT(*) AS value
  FROM {{ ref('sessions') }}
  WHERE is_conversion_session = 1
),

-- 2. Pageviews validation
pageview_validation AS (
  SELECT
    'pageviews' AS model_name,
    'row_count' AS metric,
    COUNT(*) AS value
  FROM {{ ref('pageviews') }}

  UNION ALL

  SELECT
    'pageviews' AS model_name,
    'entrance_pages' AS metric,
    COUNT(*) AS value
  FROM {{ ref('pageviews') }}
  WHERE is_entrance = 1

  UNION ALL

  SELECT
    'pageviews' AS model_name,
    'exit_pages' AS metric,
    COUNT(*) AS value
  FROM {{ ref('pageviews') }}
  WHERE is_exit = 1

  UNION ALL

  SELECT
    'pageviews' AS model_name,
    'pages_with_negative_time_on_page' AS metric,
    COUNT(*) AS value
  FROM {{ ref('pageviews') }}
  WHERE time_on_page_seconds < 0
),

-- 3. User metrics validation
user_validation AS (
  SELECT
    'users' AS model_name,
    'total_users' AS metric,
    COUNT(*) AS value
  FROM {{ ref('users') }}

  UNION ALL

  SELECT
    'users' AS model_name,
    'users_with_purchases' AS metric,
    COUNT(*) AS value
  FROM {{ ref('users') }}
  WHERE has_ever_purchased = 1

  UNION ALL

  SELECT
    'users' AS model_name,
    'repeat_buyers' AS metric,
    COUNT(*) AS value
  FROM {{ ref('users') }}
  WHERE user_type = 'Repeat Buyer'

  UNION ALL

  SELECT
    'users' AS model_name,
    'users_with_negative_lifetime' AS metric,
    COUNT(*) AS value
  FROM {{ ref('users') }}
  WHERE user_lifetime_days < 0

  UNION ALL

  SELECT
    'users' AS model_name,
    'users_with_invalid_conversion_rate' AS metric,
    COUNT(*) AS value
  FROM {{ ref('users') }}
  WHERE user_conversion_rate < 0 OR user_conversion_rate > 1
),

-- 4. Ecommerce events validation
ecommerce_validation AS (
  SELECT
    'add_to_cart_events' AS model_name,
    'total_add_to_carts' AS metric,
    COUNT(*) AS value
  FROM {{ ref('add_to_cart_events') }}

  UNION ALL

  SELECT
    'product_views' AS model_name,
    'total_product_views' AS metric,
    COUNT(*) AS value
  FROM {{ ref('product_views') }}

  UNION ALL

  SELECT
    'purchase_events' AS model_name,
    'total_purchase_items' AS metric,
    COUNT(*) AS value
  FROM {{ ref('purchase_events') }}

  UNION ALL

  SELECT
    'purchase_events' AS model_name,
    'purchases_with_negative_revenue' AS metric,
    COUNT(*) AS value
  FROM {{ ref('purchase_events') }}
  WHERE item_revenue_usd < 0

  UNION ALL

  SELECT
    'purchase_events' AS model_name,
    'total_revenue_usd' AS metric,
    CAST(SUM(item_revenue_usd) AS INT64) AS value
  FROM {{ ref('purchase_events') }}
),

-- 5. Conversion funnel validation
funnel_validation AS (
  SELECT
    'conversion_funnel' AS model_name,
    'total_funnel_rows' AS metric,
    COUNT(*) AS value
  FROM {{ ref('conversion_funnel') }}

  UNION ALL

  SELECT
    'conversion_funnel' AS model_name,
    'invalid_conversion_rates' AS metric,
    COUNT(*) AS value
  FROM {{ ref('conversion_funnel') }}
  WHERE session_overall_conversion_rate < 0 OR session_overall_conversion_rate > 1

  UNION ALL

  SELECT
    'conversion_funnel' AS model_name,
    'funnel_logic_errors' AS metric,
    COUNT(*) AS value
  FROM {{ ref('conversion_funnel') }}
  WHERE sessions_with_purchase > sessions_with_begin_checkout
     OR sessions_with_begin_checkout > sessions_with_add_to_cart
     OR sessions_with_add_to_cart > sessions_with_product_view
),

-- 6. Cross-model consistency checks
consistency_checks AS (
  -- Check: Total sessions in sessions table matches conversion_funnel
  SELECT
    'consistency' AS model_name,
    'session_count_match' AS metric,
    ABS((SELECT COUNT(DISTINCT ga_session_id) FROM {{ ref('sessions') }}) -
        (SELECT SUM(total_sessions) FROM {{ ref('conversion_funnel') }})) AS value

  UNION ALL

  -- Check: Total users in users table matches unique users in sessions
  SELECT
    'consistency' AS model_name,
    'user_count_match' AS metric,
    ABS((SELECT COUNT(*) FROM {{ ref('users') }}) -
        (SELECT COUNT(DISTINCT user_pseudo_id) FROM {{ ref('sessions') }})) AS value

  UNION ALL

  -- Check: Purchase revenue in sessions matches purchase_events
  SELECT
    'consistency' AS model_name,
    'revenue_match_difference' AS metric,
    CAST(ABS(
      COALESCE((SELECT SUM(purchase_revenue_per_session) FROM {{ ref('sessions') }}), 0) -
      COALESCE((SELECT SUM(item_revenue_usd) FROM {{ ref('purchase_events') }}), 0)
    ) AS INT64) AS value
)

-- Combine all validation results
SELECT * FROM session_validation
UNION ALL
SELECT * FROM pageview_validation
UNION ALL
SELECT * FROM user_validation
UNION ALL
SELECT * FROM ecommerce_validation
UNION ALL
SELECT * FROM funnel_validation
UNION ALL
SELECT * FROM consistency_checks
ORDER BY model_name, metric
