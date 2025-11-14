{{
    config(
        materialized = 'incremental',
        unique_key = 'funnel_pk',
        partition_by = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        }
    )
}}

with

s_events as (

    select * from {{ ref('stg_ga4__event') }}
    where event_name in (
        'page_view',
        'view_item',
        'add_to_cart',
        'begin_checkout',
        'purchase'
    )
    {% if is_incremental() %}
        and event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

funnel_by_session as (

    select
        event_date,
        user_pseudo_id,
        ga_session_id,
        traffic_source_source,
        traffic_source_medium,
        device_category,
        geo_country,

        -- funnel stage flags
        max(
            case when event_name = 'page_view' then 1 else 0 end
        ) as had_page_view,
        max(
            case when event_name = 'view_item' then 1 else 0 end
        ) as had_product_view,
        max(
            case when event_name = 'add_to_cart' then 1 else 0 end
        ) as had_add_to_cart,
        max(
            case when event_name = 'begin_checkout' then 1 else 0 end
        ) as had_begin_checkout,
        max(
            case when event_name = 'purchase' then 1 else 0 end
        ) as had_purchase

    from s_events
    group by
        event_date,
        user_pseudo_id,
        ga_session_id,
        traffic_source_source,
        traffic_source_medium,
        device_category,
        geo_country

),

funnel_aggregated as (

    select
        event_date,
        traffic_source_source,
        traffic_source_medium,
        device_category,
        geo_country,

        -- session counts at each funnel stage
        count(distinct ga_session_id) as total_sessions,
        count(distinct
            case when had_page_view = 1 then ga_session_id end
        ) as sessions_with_pageview,
        count(distinct
            case when had_product_view = 1 then ga_session_id end
        ) as sessions_with_product_view,
        count(distinct
            case when had_add_to_cart = 1 then ga_session_id end
        ) as sessions_with_add_to_cart,
        count(distinct
            case when had_begin_checkout = 1 then ga_session_id end
        ) as sessions_with_begin_checkout,
        count(distinct
            case when had_purchase = 1 then ga_session_id end
        ) as sessions_with_purchase,

        -- user counts at each funnel stage
        count(distinct user_pseudo_id) as total_users,
        count(distinct
            case when had_page_view = 1 then user_pseudo_id end
        ) as users_with_pageview,
        count(distinct
            case when had_product_view = 1 then user_pseudo_id end
        ) as users_with_product_view,
        count(distinct
            case when had_add_to_cart = 1 then user_pseudo_id end
        ) as users_with_add_to_cart,
        count(distinct
            case when had_begin_checkout = 1 then user_pseudo_id end
        ) as users_with_begin_checkout,
        count(distinct
            case when had_purchase = 1 then user_pseudo_id end
        ) as users_with_purchase

    from funnel_by_session
    group by
        event_date,
        traffic_source_source,
        traffic_source_medium,
        device_category,
        geo_country

),

final as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'traffic_source_source',
            'traffic_source_medium',
            'device_category',
            'geo_country'
        ]) }} as funnel_pk,

        -- dates
        event_date,

        -- dimensions
        traffic_source_source,
        traffic_source_medium,
        concat(
            coalesce(traffic_source_source, '(direct)'),
            ' / ',
            coalesce(traffic_source_medium, '(none)')
        ) as traffic_source_full,
        device_category,
        geo_country,

        -- session metrics
        total_sessions,
        sessions_with_pageview,
        sessions_with_product_view,
        sessions_with_add_to_cart,
        sessions_with_begin_checkout,
        sessions_with_purchase,

        -- session conversion rates (step-by-step)
        safe_divide(
            sessions_with_product_view,
            sessions_with_pageview
        ) as session_pv_to_product_view_rate,
        safe_divide(
            sessions_with_add_to_cart,
            sessions_with_product_view
        ) as session_product_view_to_cart_rate,
        safe_divide(
            sessions_with_begin_checkout,
            sessions_with_add_to_cart
        ) as session_cart_to_checkout_rate,
        safe_divide(
            sessions_with_purchase,
            sessions_with_begin_checkout
        ) as session_checkout_to_purchase_rate,

        -- session overall conversion rate
        safe_divide(
            sessions_with_purchase,
            total_sessions
        ) as session_overall_conversion_rate,

        -- user metrics
        total_users,
        users_with_pageview,
        users_with_product_view,
        users_with_add_to_cart,
        users_with_begin_checkout,
        users_with_purchase,

        -- user conversion rates (step-by-step)
        safe_divide(
            users_with_product_view,
            users_with_pageview
        ) as user_pv_to_product_view_rate,
        safe_divide(
            users_with_add_to_cart,
            users_with_product_view
        ) as user_product_view_to_cart_rate,
        safe_divide(
            users_with_begin_checkout,
            users_with_add_to_cart
        ) as user_cart_to_checkout_rate,
        safe_divide(
            users_with_purchase,
            users_with_begin_checkout
        ) as user_checkout_to_purchase_rate,

        -- user overall conversion rate
        safe_divide(
            users_with_purchase,
            total_users
        ) as user_overall_conversion_rate,

        -- drop-off metrics (sessions)
        sessions_with_pageview - sessions_with_product_view
            as sessions_dropped_at_product_view,
        sessions_with_product_view - sessions_with_add_to_cart
            as sessions_dropped_at_cart,
        sessions_with_add_to_cart - sessions_with_begin_checkout
            as sessions_dropped_at_checkout,
        sessions_with_begin_checkout - sessions_with_purchase
            as sessions_dropped_at_purchase,

        -- drop-off rates (sessions)
        safe_divide(
            sessions_with_pageview - sessions_with_product_view,
            sessions_with_pageview
        ) as session_dropoff_rate_at_product_view,
        safe_divide(
            sessions_with_product_view - sessions_with_add_to_cart,
            sessions_with_product_view
        ) as session_dropoff_rate_at_cart,
        safe_divide(
            sessions_with_add_to_cart - sessions_with_begin_checkout,
            sessions_with_add_to_cart
        ) as session_dropoff_rate_at_checkout,
        safe_divide(
            sessions_with_begin_checkout - sessions_with_purchase,
            sessions_with_begin_checkout
        ) as session_dropoff_rate_at_purchase

    from funnel_aggregated

)

select * from final
