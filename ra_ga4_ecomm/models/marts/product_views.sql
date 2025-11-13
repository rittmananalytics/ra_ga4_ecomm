{{
  config(
    materialized='incremental',
    unique_key=['event_timestamp', 'user_pseudo_id', 'item_id'],
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["user_pseudo_id", "ga_session_id"]
  )
}}

SELECT
  PARSE_DATE('%Y%m%d', event_date) AS event_date,
  event_timestamp,
  user_pseudo_id,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,

  -- Item details (unnested from items array)
  item.item_id,
  item.item_name,
  item.item_brand,
  item.item_variant,
  item.item_category,
  item.item_category2,
  item.item_category3,
  item.item_category4,
  item.item_category5,

  -- Price
  CAST(item.price AS FLOAT64) AS item_price,

  -- List and promotion info
  item.item_list_id,
  item.item_list_name,
  item.item_list_index,
  item.promotion_id,
  item.promotion_name,
  item.creative_name,
  item.creative_slot,

  -- Traffic source
  traffic_source.name AS traffic_source_name,
  traffic_source.medium AS traffic_source_medium,
  traffic_source.source AS traffic_source_source,
  CONCAT(
    COALESCE(traffic_source.source, '(direct)'),
    ' / ',
    COALESCE(traffic_source.medium, '(none)')
  ) AS traffic_source_full,

  -- Device info
  device.category AS device_category,
  device.operating_system AS device_operating_system,
  device.web_info.browser AS device_browser,

  -- Geographic info
  geo.continent AS geo_continent,
  geo.country AS geo_country,
  geo.region AS geo_region,
  geo.city AS geo_city,

  -- Product category hierarchy
  COALESCE(item.item_category, 'Uncategorized') AS category_l1,
  CASE
    WHEN item.item_category2 = '(not set)' THEN NULL
    ELSE item.item_category2
  END AS category_l2,
  CASE
    WHEN item.item_category3 = '(not set)' THEN NULL
    ELSE item.item_category3
  END AS category_l3

FROM
  {{ source('ga4_obfuscated_sample_ecommerce', 'events_') }},
  UNNEST(items) AS item
WHERE
  event_name = 'view_item'
  AND _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
  {% if is_incremental() %}
    AND PARSE_DATE('%Y%m%d', event_date) > (SELECT MAX(event_date) FROM {{ this }})
  {% endif %}
