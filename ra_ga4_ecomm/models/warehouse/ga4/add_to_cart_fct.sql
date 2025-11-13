{{
    config(
        materialized = 'incremental',
        unique_key = 'add_to_cart_pk',
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

    select * from {{ ref('stg_ga4__event') }}
    where event_name = 'add_to_cart'
    {% if is_incremental() %}
        and event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

cart_events_with_items as (

    select
        -- generate unique key for each item in cart event
        {{ dbt_utils.generate_surrogate_key([
            'event_pk',
            'item.item_id'
        ]) }} as add_to_cart_pk,

        -- foreign keys
        user_pseudo_id as user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        event_ts as add_to_cart_ts,

        -- ecommerce metrics
        ecommerce.transaction_id,
        ecommerce.total_item_quantity,
        ecommerce.unique_items,

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

        -- price and quantity
        cast(item.price as float64) as item_price,
        cast(item.quantity as int64) as item_quantity,
        cast(item.price as float64)
            * cast(item.quantity as int64) as item_value,

        -- promotional info
        item.coupon,
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
        concat(
            coalesce(traffic_source_source, '(direct)'),
            ' / ',
            coalesce(traffic_source_medium, '(none)')
        ) as traffic_source_full,

        -- device info
        device_category,
        device_operating_system,
        device_browser,

        -- geographic info
        geo_continent,
        geo_country,
        geo_region,
        geo_city

    from s_events,
    unnest(items) as item

),

final as (

    select
        -- primary key
        add_to_cart_pk,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        add_to_cart_ts,

        -- ecommerce metrics
        transaction_id,
        total_item_quantity,
        unique_items,

        -- item details
        item_id,
        item_name,
        item_brand,
        item_variant,
        item_category,
        item_category2,
        item_category3,
        item_category4,
        item_category5,

        -- price and quantity
        item_price,
        item_quantity,
        item_value,

        -- promotional info
        coupon,
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
        traffic_source_full,

        -- device info
        device_category,
        device_operating_system,
        device_browser,

        -- geographic info
        geo_continent,
        geo_country,
        geo_region,
        geo_city

    from cart_events_with_items

)

select * from final
