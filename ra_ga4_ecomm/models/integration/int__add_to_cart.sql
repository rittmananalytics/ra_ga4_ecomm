{{
    config(
        materialized = 'view'
    )
}}

with

{% if var('enable_ga4_source', true) %}
s_ga4_events as (

    select * from {{ ref('stg_ga4__event') }}
    where event_name = 'add_to_cart'

),

ga4_add_to_cart as (

    select
        -- source identifier
        'ga4' as source,

        -- primary key fields
        event_pk,
        event_date,
        event_ts,

        -- foreign keys
        user_pseudo_id as user_fk,
        cast(ga_session_id as string) as ga_session_id,

        -- ecommerce data
        ecommerce.transaction_id,
        ecommerce.total_item_quantity,
        ecommerce.unique_items,

        -- items array with explicit schema
        array(
            select as struct
                cast(item.item_id as string) as item_id,
                cast(item.item_name as string) as item_name,
                cast(item.item_brand as string) as item_brand,
                cast(item.item_variant as string) as item_variant,
                cast(item.item_category as string) as item_category,
                cast(item.item_category2 as string) as item_category2,
                cast(item.item_category3 as string) as item_category3,
                cast(item.item_category4 as string) as item_category4,
                cast(item.item_category5 as string) as item_category5,
                cast(item.price as float64) as price,
                cast(item.price_in_usd as float64) as price_in_usd,
                cast(item.quantity as int64) as quantity,
                cast(item.item_revenue as float64) as item_revenue,
                cast(item.item_revenue_in_usd as float64) as item_revenue_in_usd,
                cast(item.affiliation as string) as affiliation,
                cast(item.coupon as string) as coupon,
                cast(item.item_list_id as string) as item_list_id,
                cast(item.item_list_name as string) as item_list_name,
                cast(item.item_list_index as string) as item_list_index,
                cast(item.promotion_id as string) as promotion_id,
                cast(item.promotion_name as string) as promotion_name,
                cast(item.creative_name as string) as creative_name,
                cast(item.creative_slot as string) as creative_slot,
                cast(null as float64) as discount,  -- Snowplow only
                cast(null as string) as index  -- Snowplow only
            from unnest(items) as item
        ) as items,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        cast(null as string) as traffic_source_content,
        cast(null as string) as traffic_source_term,

        -- device
        device_category,
        device_operating_system,
        device_browser,
        device_language,

        -- device details (Snowplow-specific, null for GA4)
        cast(null as string) as browser_family,
        cast(null as string) as browser_name,
        cast(null as float64) as browser_version,
        cast(null as string) as device_type,
        cast(null as bool) as device_is_mobile,
        cast(null as int64) as device_screen_height,
        cast(null as int64) as device_screen_width,

        -- geography
        geo_continent,
        geo_sub_continent,
        geo_country,
        geo_region,
        cast(null as string) as geo_region_name,
        geo_city,
        cast(null as string) as geo_zipcode,
        cast(null as float64) as geo_latitude,
        cast(null as float64) as geo_longitude,
        cast(null as string) as geo_timezone,

        -- user properties (Snowplow-specific, null for GA4)
        cast(null as string) as user_customer_segment,
        cast(null as string) as user_loyalty_tier,
        cast(null as string) as user_subscription_status

    from s_ga4_events

),
{% endif %}

{% if var('enable_snowplow_source', false) %}
s_snowplow_events as (

    select * from {{ ref('stg_snowplow__event') }}
    where event_name = 'add_to_cart'

),

snowplow_add_to_cart as (

    select
        -- source identifier
        'snowplow' as source,

        -- primary key fields
        event_pk,
        event_date,
        event_ts,

        -- foreign keys
        user_pseudo_id as user_fk,
        cast(ga_session_id as string) as ga_session_id,

        -- ecommerce data (Snowplow doesn't have these GA4 aggregates)
        ecommerce.transaction_id,
        cast(null as int64) as total_item_quantity,
        cast(null as int64) as unique_items,

        -- items array with explicit schema matching GA4
        array(
            select as struct
                cast(item.item_id as string) as item_id,
                cast(item.item_name as string) as item_name,
                cast(item.item_brand as string) as item_brand,
                cast(item.item_variant as string) as item_variant,
                cast(item.item_category as string) as item_category,
                cast(item.item_category2 as string) as item_category2,
                cast(null as string) as item_category3,  -- GA4 only
                cast(null as string) as item_category4,  -- GA4 only
                cast(null as string) as item_category5,  -- GA4 only
                cast(item.price as float64) as price,
                cast(null as float64) as price_in_usd,  -- GA4 only
                cast(item.quantity as int64) as quantity,
                cast(null as float64) as item_revenue,  -- GA4 only
                cast(null as float64) as item_revenue_in_usd,  -- GA4 only
                cast(item.affiliation as string) as affiliation,
                cast(item.coupon as string) as coupon,
                cast(item.item_list_id as string) as item_list_id,
                cast(item.item_list_name as string) as item_list_name,
                cast(item.index as string) as item_list_index,  -- Snowplow uses 'index'
                cast(null as string) as promotion_id,  -- GA4 only
                cast(null as string) as promotion_name,  -- GA4 only
                cast(null as string) as creative_name,  -- GA4 only
                cast(null as string) as creative_slot,  -- GA4 only
                cast(item.discount as float64) as discount,  -- Snowplow specific
                cast(item.index as string) as index  -- Snowplow specific
            from unnest(items) as item
        ) as items,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        traffic_source_content,
        traffic_source_term,

        -- device
        device_category,
        device_operating_system,
        device_browser,
        device_language,

        -- device details (Snowplow-specific)
        browser_family,
        browser_name,
        browser_version,
        device_type,
        device_is_mobile,
        device_screen_height,
        device_screen_width,

        -- geography
        cast(null as string) as geo_continent,
        geo_sub_continent,
        geo_country,
        geo_region,
        geo_region_name,
        geo_city,
        geo_zipcode,
        geo_latitude,
        geo_longitude,
        geo_timezone,

        -- user properties (Snowplow-specific)
        user_customer_segment,
        user_loyalty_tier,
        user_subscription_status

    from s_snowplow_events

),
{% endif %}

final as (

    {% if var('enable_ga4_source', true) %}
    select * from ga4_add_to_cart
    {% endif %}

    {% if var('enable_ga4_source', true) and var('enable_snowplow_source', false) %}
    union all
    {% endif %}

    {% if var('enable_snowplow_source', false) %}
    select * from snowplow_add_to_cart
    {% endif %}

)

select * from final
