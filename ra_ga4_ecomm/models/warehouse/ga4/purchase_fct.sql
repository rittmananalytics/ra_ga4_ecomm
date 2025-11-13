{{
    config(
        materialized = 'incremental',
        unique_key = 'purchase_pk',
        partition_by = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["user_fk", "transaction_id"]
    )
}}

with

s_events as (

    select * from {{ ref('stg_ga4__event') }}
    where event_name = 'purchase'
    {% if is_incremental() %}
        and event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

s_sessions as (

    select
        user_fk,
        min(session_start_ts) as first_session_ts
    from {{ ref('session_fct') }}
    group by user_fk

),

purchase_events_with_items as (

    select
        -- generate unique key for each item in purchase
        {{ dbt_utils.generate_surrogate_key([
            'event_pk',
            'item.item_id'
        ]) }} as purchase_pk,

        -- foreign keys
        user_pseudo_id as user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        event_ts as purchase_ts,

        -- transaction details
        ecommerce.transaction_id,
        ecommerce.purchase_revenue_in_usd,
        ecommerce.purchase_revenue,
        ecommerce.tax_value_in_usd,
        ecommerce.shipping_value_in_usd,
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
        cast(item.price_in_usd as float64) as item_price_usd,
        cast(item.price as float64) as item_price,
        cast(item.quantity as int64) as item_quantity,
        cast(item.item_revenue_in_usd as float64) as item_revenue_usd,
        cast(item.item_revenue as float64) as item_revenue,

        -- promotional info
        item.coupon,
        item.affiliation,
        item.item_list_id,
        item.item_list_name,
        item.promotion_id,
        item.promotion_name,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,

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
        purchase_pk,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,
        transaction_id,

        -- timestamps
        purchase_ts,

        -- time to purchase metrics
        timestamp_diff(
            purchase_ts,
            s_sessions.first_session_ts,
            hour
        ) as hours_to_purchase_from_first_visit,
        timestamp_diff(
            purchase_ts,
            s_sessions.first_session_ts,
            day
        ) as days_to_purchase_from_first_visit,

        -- purchase timing
        extract(hour from purchase_ts) as purchase_hour_of_day,
        extract(dayofweek from purchase_ts) as purchase_day_of_week,

        -- transaction-level metrics
        purchase_revenue_in_usd,
        purchase_revenue,
        tax_value_in_usd,
        shipping_value_in_usd,
        total_item_quantity,
        unique_items,

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

        -- price and quantity
        item_price_usd,
        item_price,
        item_quantity,
        item_revenue_usd,
        item_revenue,

        -- item-level metrics
        safe_divide(item_revenue_usd, item_quantity) as unit_price_usd,

        -- promotional info
        coupon,
        affiliation,
        item_list_id,
        item_list_name,
        promotion_id,
        promotion_name,

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

    from purchase_events_with_items
    left join s_sessions
        on purchase_events_with_items.user_fk = s_sessions.user_fk

)

select * from final
