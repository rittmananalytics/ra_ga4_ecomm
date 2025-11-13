{{
    config(
        materialized = 'incremental',
        unique_key = ['event_date', 'user_pseudo_id', 'ga_session_id'],
        partition_by = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["user_pseudo_id", "ga_session_id"]
    )
}}

with

s_events as (

    select * from {{ ref('stg_ga4__event') }}
    {% if is_incremental() %}
        where event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

session_aggregations as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'event_date',
            'user_pseudo_id',
            'ga_session_id'
        ]) }} as session_pk,

        -- foreign keys
        user_pseudo_id as user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        min(event_ts) as session_start_ts,
        max(event_ts) as session_end_ts,

        -- session metrics
        timestamp_diff(max(event_ts), min(event_ts), second)
            as session_duration_seconds,
        count(
            case when event_name = 'page_view' then 1 end
        ) as page_views_per_session,
        count(*) as events_per_session,
        sum(
            case
                when (
                    select value.string_value
                    from unnest(event_params)
                    where key = 'engagement_time_msec'
                ) is not null
                then cast((
                    select value.string_value
                    from unnest(event_params)
                    where key = 'engagement_time_msec'
                ) as int64)
                else 0
            end
        ) as session_engagement_time_msec,

        -- conversion metrics
        count(
            case when event_name = 'purchase' then 1 end
        ) as purchase_events_per_session,
        sum(
            case
                when event_name = 'purchase'
                then ecommerce.purchase_revenue_in_usd
                else 0
            end
        ) as purchase_revenue_per_session,
        case
            when count(
                case when event_name = 'purchase' then 1 end
            ) > 0
            then 1
            else 0
        end as is_conversion_session,

        -- traffic source
        any_value(traffic_source_name) as traffic_source_name,
        any_value(traffic_source_medium) as traffic_source_medium,
        any_value(traffic_source_source) as traffic_source_source,

        -- device
        any_value(device_category) as device_category,
        any_value(device_operating_system) as device_operating_system,
        any_value(device_browser) as device_browser,

        -- geography
        any_value(geo_continent) as geo_continent,
        any_value(geo_sub_continent) as geo_sub_continent,
        any_value(geo_country) as geo_country,
        any_value(geo_region) as geo_region,
        any_value(geo_city) as geo_city

    from s_events
    where ga_session_id is not null
    group by
        event_date,
        user_pseudo_id,
        ga_session_id

),

final as (

    select
        -- primary key
        session_pk,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        session_start_ts,
        session_end_ts,

        -- session metrics
        session_duration_seconds,
        page_views_per_session,
        events_per_session,
        session_engagement_time_msec,

        -- conversion metrics
        purchase_events_per_session,
        purchase_revenue_per_session,
        is_conversion_session,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,

        -- device
        device_category,
        device_operating_system,
        device_browser,

        -- geography
        geo_continent,
        geo_sub_continent,
        geo_country,
        geo_region,
        geo_city

    from session_aggregations

)

select * from final
