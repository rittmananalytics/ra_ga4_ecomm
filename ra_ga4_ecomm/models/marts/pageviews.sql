{{
  config(
    materialized='incremental',
    unique_key=['event_timestamp', 'user_pseudo_id'],
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["user_pseudo_id", "ga_session_id"]
  )
}}

WITH page_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_location') AS page_location,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_title') AS page_title,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE KEY = 'page_referrer') AS page_referrer,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'entrances') AS entrances,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'engagement_time_msec') AS engagement_time_msec,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'session_engaged') AS session_engaged,
    traffic_source.name AS traffic_source_name,
    traffic_source.medium AS traffic_source_medium,
    traffic_source.source AS traffic_source_source,
    device.category AS device_category,
    device.operating_system AS device_operating_system,
    device.web_info.browser AS device_browser,
    device.language AS device_language,
    geo.continent AS geo_continent,
    geo.sub_continent AS geo_sub_continent,
    geo.country AS geo_country,
    geo.region AS geo_region,
    geo.city AS geo_city
  FROM
    {{ source('ga4_obfuscated_sample_ecommerce', 'events_') }}
  WHERE
    event_name = 'page_view'
    AND _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
    {% if is_incremental() %}
      -- Only process new data in incremental runs
      AND PARSE_DATE('%Y%m%d', event_date) > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
),

page_events_with_timing AS (
  SELECT
    *,
    LEAD(event_timestamp) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
    ) AS next_page_timestamp,
    LAG(page_location) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
    ) AS previous_page_location,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
    ) AS page_number_in_session,
    COUNT(*) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
    ) AS total_pages_in_session
  FROM page_events
)

SELECT
  event_date,
  event_timestamp,
  user_pseudo_id,
  ga_session_id,
  page_location,
  page_title,
  page_referrer,
  previous_page_location,

  -- Page path analysis
  REGEXP_EXTRACT(page_location, r'https?://[^/]+(/[^?#]*)') AS page_path,
  REGEXP_EXTRACT(page_location, r'https?://([^/]+)') AS page_hostname,
  REGEXP_EXTRACT(page_location, r'\?(.*)') AS query_string,

  -- Engagement metrics
  COALESCE(engagement_time_msec, 0) AS engagement_time_msec,
  COALESCE(engagement_time_msec, 0) / 1000.0 AS engagement_time_seconds,

  -- Time on page calculation (time until next page view or end of session)
  CASE
    WHEN next_page_timestamp IS NOT NULL
    THEN (next_page_timestamp - event_timestamp) / 1000000.0
    ELSE NULL
  END AS time_on_page_seconds,

  -- Session context
  page_number_in_session,
  total_pages_in_session,

  -- Entry/Exit flags
  CASE WHEN page_number_in_session = 1 THEN 1 ELSE 0 END AS is_entrance,
  CASE WHEN page_number_in_session = total_pages_in_session THEN 1 ELSE 0 END AS is_exit,
  COALESCE(entrances, 0) AS entrances,

  -- Engagement flag
  CASE WHEN session_engaged = 1 THEN 1 ELSE 0 END AS is_engaged_session,

  -- Traffic source
  traffic_source_name,
  traffic_source_medium,
  traffic_source_source,
  CONCAT(
    COALESCE(traffic_source_source, '(direct)'),
    ' / ',
    COALESCE(traffic_source_medium, '(none)')
  ) AS traffic_source_full,

  -- Device info
  device_category,
  device_operating_system,
  device_browser,
  device_language,

  -- Geographic info
  geo_continent,
  geo_sub_continent,
  geo_country,
  geo_region,
  geo_city

FROM page_events_with_timing
