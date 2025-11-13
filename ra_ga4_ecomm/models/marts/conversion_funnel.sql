{{
  config(
    materialized='incremental',
    unique_key=['event_date', 'traffic_source', 'traffic_medium', 'device_category', 'country'],
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

WITH daily_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,
    event_name,
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    device.category AS device_category,
    geo.country AS country
  FROM
    {{ source('ga4_obfuscated_sample_ecommerce', 'events_') }}
  WHERE
    _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
    AND event_name IN ('page_view', 'view_item', 'add_to_cart', 'begin_checkout', 'purchase')
    {% if is_incremental() %}
      AND PARSE_DATE('%Y%m%d', event_date) > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
),

funnel_by_session AS (
  SELECT
    event_date,
    user_pseudo_id,
    ga_session_id,
    traffic_source,
    traffic_medium,
    device_category,
    country,
    MAX(CASE WHEN event_name = 'page_view' THEN 1 ELSE 0 END) AS had_page_view,
    MAX(CASE WHEN event_name = 'view_item' THEN 1 ELSE 0 END) AS had_product_view,
    MAX(CASE WHEN event_name = 'add_to_cart' THEN 1 ELSE 0 END) AS had_add_to_cart,
    MAX(CASE WHEN event_name = 'begin_checkout' THEN 1 ELSE 0 END) AS had_begin_checkout,
    MAX(CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END) AS had_purchase
  FROM daily_events
  GROUP BY
    event_date,
    user_pseudo_id,
    ga_session_id,
    traffic_source,
    traffic_medium,
    device_category,
    country
),

funnel_aggregated AS (
  SELECT
    event_date,
    traffic_source,
    traffic_medium,
    device_category,
    country,

    -- Session counts at each funnel stage
    COUNT(DISTINCT ga_session_id) AS total_sessions,
    COUNT(DISTINCT CASE WHEN had_page_view = 1 THEN ga_session_id END) AS sessions_with_pageview,
    COUNT(DISTINCT CASE WHEN had_product_view = 1 THEN ga_session_id END) AS sessions_with_product_view,
    COUNT(DISTINCT CASE WHEN had_add_to_cart = 1 THEN ga_session_id END) AS sessions_with_add_to_cart,
    COUNT(DISTINCT CASE WHEN had_begin_checkout = 1 THEN ga_session_id END) AS sessions_with_begin_checkout,
    COUNT(DISTINCT CASE WHEN had_purchase = 1 THEN ga_session_id END) AS sessions_with_purchase,

    -- User counts at each funnel stage
    COUNT(DISTINCT user_pseudo_id) AS total_users,
    COUNT(DISTINCT CASE WHEN had_page_view = 1 THEN user_pseudo_id END) AS users_with_pageview,
    COUNT(DISTINCT CASE WHEN had_product_view = 1 THEN user_pseudo_id END) AS users_with_product_view,
    COUNT(DISTINCT CASE WHEN had_add_to_cart = 1 THEN user_pseudo_id END) AS users_with_add_to_cart,
    COUNT(DISTINCT CASE WHEN had_begin_checkout = 1 THEN user_pseudo_id END) AS users_with_begin_checkout,
    COUNT(DISTINCT CASE WHEN had_purchase = 1 THEN user_pseudo_id END) AS users_with_purchase

  FROM funnel_by_session
  GROUP BY
    event_date,
    traffic_source,
    traffic_medium,
    device_category,
    country
)

SELECT
  event_date,
  traffic_source,
  traffic_medium,
  CONCAT(
    COALESCE(traffic_source, '(direct)'),
    ' / ',
    COALESCE(traffic_medium, '(none)')
  ) AS traffic_source_full,
  device_category,
  country,

  -- Session metrics
  total_sessions,
  sessions_with_pageview,
  sessions_with_product_view,
  sessions_with_add_to_cart,
  sessions_with_begin_checkout,
  sessions_with_purchase,

  -- Session conversion rates (step-by-step)
  SAFE_DIVIDE(sessions_with_product_view, sessions_with_pageview) AS session_pv_to_product_view_rate,
  SAFE_DIVIDE(sessions_with_add_to_cart, sessions_with_product_view) AS session_product_view_to_cart_rate,
  SAFE_DIVIDE(sessions_with_begin_checkout, sessions_with_add_to_cart) AS session_cart_to_checkout_rate,
  SAFE_DIVIDE(sessions_with_purchase, sessions_with_begin_checkout) AS session_checkout_to_purchase_rate,

  -- Session overall conversion rate
  SAFE_DIVIDE(sessions_with_purchase, total_sessions) AS session_overall_conversion_rate,

  -- User metrics
  total_users,
  users_with_pageview,
  users_with_product_view,
  users_with_add_to_cart,
  users_with_begin_checkout,
  users_with_purchase,

  -- User conversion rates (step-by-step)
  SAFE_DIVIDE(users_with_product_view, users_with_pageview) AS user_pv_to_product_view_rate,
  SAFE_DIVIDE(users_with_add_to_cart, users_with_product_view) AS user_product_view_to_cart_rate,
  SAFE_DIVIDE(users_with_begin_checkout, users_with_add_to_cart) AS user_cart_to_checkout_rate,
  SAFE_DIVIDE(users_with_purchase, users_with_begin_checkout) AS user_checkout_to_purchase_rate,

  -- User overall conversion rate
  SAFE_DIVIDE(users_with_purchase, total_users) AS user_overall_conversion_rate,

  -- Drop-off metrics (sessions)
  sessions_with_pageview - sessions_with_product_view AS sessions_dropped_at_product_view,
  sessions_with_product_view - sessions_with_add_to_cart AS sessions_dropped_at_cart,
  sessions_with_add_to_cart - sessions_with_begin_checkout AS sessions_dropped_at_checkout,
  sessions_with_begin_checkout - sessions_with_purchase AS sessions_dropped_at_purchase,

  -- Drop-off rates (sessions)
  SAFE_DIVIDE(sessions_with_pageview - sessions_with_product_view, sessions_with_pageview) AS session_dropoff_rate_at_product_view,
  SAFE_DIVIDE(sessions_with_product_view - sessions_with_add_to_cart, sessions_with_product_view) AS session_dropoff_rate_at_cart,
  SAFE_DIVIDE(sessions_with_add_to_cart - sessions_with_begin_checkout, sessions_with_add_to_cart) AS session_dropoff_rate_at_checkout,
  SAFE_DIVIDE(sessions_with_begin_checkout - sessions_with_purchase, sessions_with_begin_checkout) AS session_dropoff_rate_at_purchase

FROM funnel_aggregated
