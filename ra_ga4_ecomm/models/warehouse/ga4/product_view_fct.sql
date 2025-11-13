{{
    config(
        materialized = 'incremental',
        unique_key = 'product_view_pk',
        partition_by = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["user_fk", "ga_session_id"]
    )
}}

with

s_events as (

    select * from {{ ref('int__product_view') }}
    {% if is_incremental() %}
        where event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

product_view_events_with_items as (

    select
        -- generate unique key for each item view
        {{ dbt_utils.generate_surrogate_key([
            'source',
            'event_pk',
            'item.item_id',
            'item_offset'
        ]) }} as product_view_pk,

        -- source identifier
        source,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        event_ts as product_view_ts,

        -- item details
        item.item_id,
        item.item_name,
        item.item_brand,
        item.item_variant,
        item.item_category,
        item.item_category2,
        item.item_category3,
        item.item_category4,
        item.item_category5,

        -- price
        cast(item.price as float64) as item_price,

        -- list and promotion info
        item.item_list_id,
        item.item_list_name,
        item.item_list_index,
        item.promotion_id,
        item.promotion_name,
        item.creative_name,
        item.creative_slot,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        traffic_source_content,
        traffic_source_term,

        -- device info
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

        -- geographic info
        geo_continent,
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

    from s_events,
    unnest(items) as item with offset as item_offset

),

final as (

    select
        -- primary key
        product_view_pk,

        -- source identifier
        source,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        product_view_ts,

        -- item details
        item_id,
        item_name,
        item_brand,
        item_variant,

        -- product category hierarchy
        coalesce(item_category, 'Uncategorized') as category_l1,
        case
            when item_category2 = '(not set)' then null
            else item_category2
        end as category_l2,
        case
            when item_category3 = '(not set)' then null
            else item_category3
        end as category_l3,
        case
            when item_category4 = '(not set)' then null
            else item_category4
        end as category_l4,
        case
            when item_category5 = '(not set)' then null
            else item_category5
        end as category_l5,

        -- price
        item_price,

        -- list and promotion info
        item_list_id,
        item_list_name,
        item_list_index,
        promotion_id,
        promotion_name,
        creative_name,
        creative_slot,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        traffic_source_content,
        traffic_source_term,
        concat(
            coalesce(traffic_source_source, '(direct)'),
            ' / ',
            coalesce(traffic_source_medium, '(none)')
        ) as traffic_source_full,

        -- device info
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

        -- geographic info
        geo_continent,
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

    from product_view_events_with_items

)

select * from final
