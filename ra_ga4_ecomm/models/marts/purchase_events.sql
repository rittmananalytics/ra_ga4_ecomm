{{
  config(
    materialized='incremental',
    unique_key=['event_timestamp', 'user_pseudo_id', 'item_id'],
    partition_by={
      "field": "event_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["user_pseudo_id", "transaction_id"]
  )
}}

WITH purchases AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS event_date,
    event_timestamp,
    user_pseudo_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE KEY = 'ga_session_id') AS ga_session_id,

    -- Transaction details
    ecommerce.transaction_id,
    ecommerce.purchase_revenue_in_usd,
    ecommerce.purchase_revenue,
    ecommerce.tax_value_in_usd,
    ecommerce.shipping_value_in_usd,
    ecommerce.total_item_quantity,
    ecommerce.unique_items,

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

    -- Price and quantity
    CAST(item.price_in_usd AS FLOAT64) AS item_price_usd,
    CAST(item.price AS FLOAT64) AS item_price,
    CAST(item.quantity AS INT64) AS item_quantity,
    CAST(item.item_revenue_in_usd AS FLOAT64) AS item_revenue_usd,
    CAST(item.item_revenue AS FLOAT64) AS item_revenue,

    -- Promotional info
    item.coupon,
    item.affiliation,
    item.item_list_id,
    item.item_list_name,
    item.promotion_id,
    item.promotion_name,

    -- Traffic source
    traffic_source.name AS traffic_source_name,
    traffic_source.medium AS traffic_source_medium,
    traffic_source.source AS traffic_source_source,

    -- Device info
    device.category AS device_category,
    device.operating_system AS device_operating_system,
    device.web_info.browser AS device_browser,

    -- Geographic info
    geo.continent AS geo_continent,
    geo.country AS geo_country,
    geo.region AS geo_region,
    geo.city AS geo_city

  FROM
    {{ source('ga4_obfuscated_sample_ecommerce', 'events_') }},
    UNNEST(items) AS item
  WHERE
    event_name = 'purchase'
    AND _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
    {% if is_incremental() %}
      AND PARSE_DATE('%Y%m%d', event_date) > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
),

-- Get first session for each user to calculate time to purchase
user_first_session AS (
  SELECT
    user_pseudo_id,
    MIN(session_start_timestamp) AS first_session_timestamp
  FROM {{ ref('sessions') }}
  GROUP BY user_pseudo_id
)

SELECT
  p.*,

  -- Calculate time to purchase from first visit
  TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(p.event_timestamp),
    TIMESTAMP_MICROS(ufs.first_session_timestamp),
    HOUR
  ) AS hours_to_purchase_from_first_visit,

  TIMESTAMP_DIFF(
    TIMESTAMP_MICROS(p.event_timestamp),
    TIMESTAMP_MICROS(ufs.first_session_timestamp),
    DAY
  ) AS days_to_purchase_from_first_visit,

  -- Traffic source full
  CONCAT(
    COALESCE(p.traffic_source_source, '(direct)'),
    ' / ',
    COALESCE(p.traffic_source_medium, '(none)')
  ) AS traffic_source_full,

  -- Purchase timing
  EXTRACT(HOUR FROM TIMESTAMP_MICROS(p.event_timestamp)) AS purchase_hour_of_day,
  EXTRACT(DAYOFWEEK FROM TIMESTAMP_MICROS(p.event_timestamp)) AS purchase_day_of_week,

  -- Item-level metrics
  SAFE_DIVIDE(p.item_revenue_usd, p.item_quantity) AS unit_price_usd,

  -- Product category hierarchy
  COALESCE(p.item_category, 'Uncategorized') AS category_l1,
  CASE
    WHEN p.item_category2 = '(not set)' THEN NULL
    ELSE p.item_category2
  END AS category_l2,
  CASE
    WHEN p.item_category3 = '(not set)' THEN NULL
    ELSE p.item_category3
  END AS category_l3

FROM purchases p
LEFT JOIN user_first_session ufs ON p.user_pseudo_id = ufs.user_pseudo_id
