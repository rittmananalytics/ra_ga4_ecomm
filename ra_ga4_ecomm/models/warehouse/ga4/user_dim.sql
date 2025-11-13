{{
    config(
        materialized = 'table'
    )
}}

with

s_sessions as (

    select * from {{ ref('session_fct') }}

),

user_sessions as (

    select
        user_fk,
        count(distinct ga_session_id) as total_sessions,
        min(event_date) as first_seen_date,
        max(event_date) as last_seen_date,
        sum(page_views_per_session) as total_page_views,
        sum(events_per_session) as total_events,
        sum(session_duration_seconds) as total_session_duration_seconds,
        sum(session_engagement_time_msec) / 1000.0
            as total_engagement_time_seconds,
        avg(session_duration_seconds) as avg_session_duration_seconds,
        avg(page_views_per_session) as avg_page_views_per_session,
        sum(purchase_events_per_session) as total_purchases,
        sum(purchase_revenue_per_session) as total_revenue,
        max(is_conversion_session) as has_ever_purchased,
        count(distinct
            case
                when is_conversion_session = 1 then ga_session_id
            end
        ) as conversion_sessions,

        -- first and last touch attribution
        array_agg(
            struct(
                traffic_source_source,
                traffic_source_medium,
                traffic_source_name,
                event_date
            )
            order by event_date asc
            limit 1
        )[safe_offset(0)] as first_touch,
        array_agg(
            struct(
                traffic_source_source,
                traffic_source_medium,
                traffic_source_name,
                event_date
            )
            order by event_date desc
            limit 1
        )[safe_offset(0)] as last_touch,

        -- device and geo from most recent session
        array_agg(
            struct(
                device_category,
                device_operating_system,
                geo_country,
                geo_region,
                geo_city
            )
            order by event_date desc
            limit 1
        )[safe_offset(0)] as latest_context

    from s_sessions
    group by user_fk

),

user_metrics as (

    select
        -- primary key
        user_fk as user_pk,

        -- session metrics
        total_sessions,
        total_page_views,
        total_events,
        avg_page_views_per_session,

        -- time metrics
        total_session_duration_seconds,
        avg_session_duration_seconds,
        total_engagement_time_seconds,
        safe_divide(
            total_engagement_time_seconds,
            total_sessions
        ) as avg_engagement_per_session,

        -- conversion metrics
        has_ever_purchased,
        total_purchases,
        conversion_sessions,
        safe_divide(
            conversion_sessions,
            total_sessions
        ) as user_conversion_rate,
        total_revenue,
        safe_divide(
            total_revenue,
            total_sessions
        ) as revenue_per_session,
        safe_divide(
            total_revenue,
            total_purchases
        ) as average_order_value,

        -- lifecycle metrics
        first_seen_date,
        last_seen_date,
        date_diff(
            last_seen_date,
            first_seen_date,
            day
        ) as user_lifetime_days,
        date_diff(
            current_date(),
            last_seen_date,
            day
        ) as days_since_last_visit,

        -- engagement metrics
        safe_divide(
            total_page_views,
            total_sessions
        ) as pages_per_session,

        -- attribution
        first_touch,
        last_touch,
        latest_context

    from user_sessions

),

final as (

    select
        -- primary key
        user_pk,

        -- session metrics
        total_sessions,
        total_page_views,
        total_events,
        pages_per_session,
        avg_page_views_per_session,

        -- time metrics
        total_session_duration_seconds,
        avg_session_duration_seconds,
        total_engagement_time_seconds,
        avg_engagement_per_session,

        -- conversion metrics
        has_ever_purchased,
        total_purchases,
        conversion_sessions,
        user_conversion_rate,
        total_revenue,
        revenue_per_session,
        average_order_value,

        -- lifecycle metrics
        first_seen_date,
        last_seen_date,
        user_lifetime_days,
        days_since_last_visit,

        -- rfm segmentation
        case
            when days_since_last_visit <= 7
            then 'Active (Last 7 days)'
            when days_since_last_visit <= 30
            then 'Recent (Last 30 days)'
            when days_since_last_visit <= 90
            then 'Dormant (Last 90 days)'
            else 'Inactive (90+ days)'
        end as recency_segment,
        case
            when total_sessions >= 10
            then 'High Frequency (10+ sessions)'
            when total_sessions >= 5
            then 'Medium Frequency (5-9 sessions)'
            when total_sessions >= 2
            then 'Low Frequency (2-4 sessions)'
            else 'Single Session'
        end as frequency_segment,
        case
            when total_revenue >= 500 then 'High Value ($500+)'
            when total_revenue >= 100 then 'Medium Value ($100-$499)'
            when total_revenue > 0 then 'Low Value ($1-$99)'
            else 'No Revenue'
        end as monetary_segment,

        -- user type classification
        case
            when total_purchases > 1 then 'Repeat Buyer'
            when total_purchases = 1 then 'One-Time Buyer'
            when total_sessions > 1 then 'Engaged Non-Buyer'
            else 'Single Visit'
        end as user_type,

        -- attribution
        first_touch.traffic_source_source as first_touch_source,
        first_touch.traffic_source_medium as first_touch_medium,
        first_touch.traffic_source_name as first_touch_campaign,
        first_touch.event_date as first_touch_date,
        last_touch.traffic_source_source as last_touch_source,
        last_touch.traffic_source_medium as last_touch_medium,
        last_touch.traffic_source_name as last_touch_campaign,
        last_touch.event_date as last_touch_date,

        -- latest context
        latest_context.device_category as latest_device_category,
        latest_context.device_operating_system as latest_device_os,
        latest_context.geo_country as latest_geo_country,
        latest_context.geo_region as latest_geo_region,
        latest_context.geo_city as latest_geo_city

    from user_metrics

)

select * from final
