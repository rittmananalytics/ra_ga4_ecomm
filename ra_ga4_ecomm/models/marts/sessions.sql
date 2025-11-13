{{
  config(
    materialized='incremental',
    unique_key=['event_date', 'user_pseudo_id', 'ga_session_id'],
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    }
  )
}}

SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  user_pseudo_id,
  (
    SELECT
      value.int_value
    FROM
      UNNEST(event_params)
    WHERE
      KEY = 'ga_session_id'
  ) AS ga_session_id,
  MIN(event_timestamp) AS session_start_timestamp,
  MAX(event_timestamp) AS session_end_timestamp,
  (MAX(event_timestamp) - MIN(event_timestamp)) / 1000000 AS session_duration_seconds,
  COUNT(CASE
      WHEN event_name = 'page_view' THEN 1
      ELSE NULL
    END
  ) AS page_views_per_session,
  COUNT(event_name) AS events_per_session,
  SUM(
    CASE
      WHEN (
        SELECT
          value.string_value
        FROM
          UNNEST(event_params)
        WHERE
          KEY = 'engagement_time_msec'
      ) IS NOT NULL THEN (
        SELECT
          CAST(value.string_value AS INT64)
        FROM
          UNNEST(event_params)
        WHERE
          KEY = 'engagement_time_msec'
      )
      ELSE 0
    END
  ) AS session_engagement_time_msec,
  traffic_source.name AS traffic_source_name,
  traffic_source.medium AS traffic_source_medium,
  traffic_source.source AS traffic_source_source,
  COUNT(CASE
      WHEN event_name = 'purchase' THEN 1
      ELSE NULL
    END
  ) AS purchase_events_per_session,
  SUM(
    CASE
      WHEN event_name = 'purchase' THEN ecommerce.purchase_revenue_in_usd
      ELSE 0
    END
  ) AS purchase_revenue_per_session,
  CASE
    WHEN COUNT(CASE
        WHEN event_name = 'purchase' THEN 1
        ELSE NULL
      END
    ) > 0 THEN 1
    ELSE 0
  END AS is_conversion_session,
  device.category AS device_category,
  device.operating_system AS device_operating_system,
  device.language AS device_browser,
  geo.continent AS geo_continent,
  geo.sub_continent AS geo_sub_continent,
  geo.country AS geo_country,
  geo.region AS geo_region,
  geo.city AS geo_city
FROM
  {{ source('ga4_obfuscated_sample_ecommerce', 'events_') }}
WHERE
  _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
  {% if is_incremental() %}
    -- Only process new data in incremental runs
    AND PARSE_DATE('%Y%m%d', event_date) > (SELECT MAX(event_date) FROM {{ this }})
  {% endif %}
GROUP BY
  event_date,
  user_pseudo_id,
  ga_session_id,
  traffic_source_name,
  traffic_source_medium,
  traffic_source_source,
  device_category,
  device_operating_system,
  device_browser,
  geo_continent,
  geo_sub_continent,
  geo_country,
  geo_region,
  geo_city
