{{
  config(
    materialized='table'
  )
}}

WITH user_sessions AS (
  SELECT
    user_pseudo_id,
    COUNT(DISTINCT ga_session_id) AS total_sessions,
    MIN(event_date) AS first_seen_date,
    MAX(event_date) AS last_seen_date,
    SUM(page_views_per_session) AS total_page_views,
    SUM(events_per_session) AS total_events,
    SUM(session_duration_seconds) AS total_session_duration_seconds,
    SUM(session_engagement_time_msec) / 1000.0 AS total_engagement_time_seconds,
    AVG(session_duration_seconds) AS avg_session_duration_seconds,
    AVG(page_views_per_session) AS avg_page_views_per_session,
    SUM(purchase_events_per_session) AS total_purchases,
    SUM(purchase_revenue_per_session) AS total_revenue,
    MAX(is_conversion_session) AS has_ever_purchased,
    COUNT(DISTINCT CASE WHEN is_conversion_session = 1 THEN ga_session_id END) AS conversion_sessions,
    -- First and last touch attribution
    ARRAY_AGG(
      STRUCT(
        traffic_source_source,
        traffic_source_medium,
        traffic_source_name,
        event_date
      )
      ORDER BY event_date ASC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS first_touch,
    ARRAY_AGG(
      STRUCT(
        traffic_source_source,
        traffic_source_medium,
        traffic_source_name,
        event_date
      )
      ORDER BY event_date DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS last_touch,
    -- Device and geo from most recent session
    ARRAY_AGG(
      STRUCT(
        device_category,
        device_operating_system,
        geo_country,
        geo_region,
        geo_city
      )
      ORDER BY event_date DESC
      LIMIT 1
    )[SAFE_OFFSET(0)] AS latest_context
  FROM {{ ref('sessions') }}
  GROUP BY user_pseudo_id
),

user_metrics AS (
  SELECT
    user_pseudo_id,
    total_sessions,
    first_seen_date,
    last_seen_date,
    total_page_views,
    total_events,
    total_session_duration_seconds,
    total_engagement_time_seconds,
    avg_session_duration_seconds,
    avg_page_views_per_session,
    total_purchases,
    total_revenue,
    has_ever_purchased,
    conversion_sessions,
    first_touch,
    last_touch,
    latest_context,

    -- User lifecycle metrics
    DATE_DIFF(last_seen_date, first_seen_date, DAY) AS user_lifetime_days,
    DATE_DIFF(CURRENT_DATE(), last_seen_date, DAY) AS days_since_last_visit,

    -- Conversion metrics
    SAFE_DIVIDE(conversion_sessions, total_sessions) AS user_conversion_rate,
    SAFE_DIVIDE(total_revenue, total_sessions) AS revenue_per_session,
    SAFE_DIVIDE(total_revenue, total_purchases) AS average_order_value,

    -- Engagement metrics
    SAFE_DIVIDE(total_page_views, total_sessions) AS pages_per_session,
    SAFE_DIVIDE(total_engagement_time_seconds, total_sessions) AS avg_engagement_per_session
  FROM user_sessions
)

SELECT
  user_pseudo_id,

  -- Session metrics
  total_sessions,
  total_page_views,
  total_events,
  pages_per_session,
  avg_page_views_per_session,

  -- Time metrics
  total_session_duration_seconds,
  avg_session_duration_seconds,
  total_engagement_time_seconds,
  avg_engagement_per_session,

  -- Conversion metrics
  has_ever_purchased,
  total_purchases,
  conversion_sessions,
  user_conversion_rate,
  total_revenue,
  revenue_per_session,
  average_order_value,

  -- Lifecycle metrics
  first_seen_date,
  last_seen_date,
  user_lifetime_days,
  days_since_last_visit,

  -- RFM Segmentation
  CASE
    WHEN days_since_last_visit <= 7 THEN 'Active (Last 7 days)'
    WHEN days_since_last_visit <= 30 THEN 'Recent (Last 30 days)'
    WHEN days_since_last_visit <= 90 THEN 'Dormant (Last 90 days)'
    ELSE 'Inactive (90+ days)'
  END AS recency_segment,

  CASE
    WHEN total_sessions >= 10 THEN 'High Frequency (10+ sessions)'
    WHEN total_sessions >= 5 THEN 'Medium Frequency (5-9 sessions)'
    WHEN total_sessions >= 2 THEN 'Low Frequency (2-4 sessions)'
    ELSE 'Single Session'
  END AS frequency_segment,

  CASE
    WHEN total_revenue >= 500 THEN 'High Value ($500+)'
    WHEN total_revenue >= 100 THEN 'Medium Value ($100-$499)'
    WHEN total_revenue > 0 THEN 'Low Value ($1-$99)'
    ELSE 'No Revenue'
  END AS monetary_segment,

  -- User type classification
  CASE
    WHEN total_purchases > 1 THEN 'Repeat Buyer'
    WHEN total_purchases = 1 THEN 'One-Time Buyer'
    WHEN total_sessions > 1 THEN 'Engaged Non-Buyer'
    ELSE 'Single Visit'
  END AS user_type,

  -- Attribution
  first_touch.traffic_source_source AS first_touch_source,
  first_touch.traffic_source_medium AS first_touch_medium,
  first_touch.traffic_source_name AS first_touch_campaign,
  first_touch.event_date AS first_touch_date,

  last_touch.traffic_source_source AS last_touch_source,
  last_touch.traffic_source_medium AS last_touch_medium,
  last_touch.traffic_source_name AS last_touch_campaign,
  last_touch.event_date AS last_touch_date,

  -- Latest context
  latest_context.device_category AS latest_device_category,
  latest_context.device_operating_system AS latest_device_os,
  latest_context.geo_country AS latest_geo_country,
  latest_context.geo_region AS latest_geo_region,
  latest_context.geo_city AS latest_geo_city

FROM user_metrics
