{{
    config(
        materialized = 'incremental',
        unique_key = 'pageview_pk',
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
    where event_name = 'page_view'
    {% if is_incremental() %}
        and event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

page_events_with_params as (

    select
        -- primary key fields
        event_pk,
        event_date,
        event_ts,

        -- foreign keys
        user_pseudo_id as user_fk,
        ga_session_id,

        -- page parameters from event_params
        (
            select value.string_value
            from unnest(event_params)
            where key = 'page_location'
        ) as page_location,
        (
            select value.string_value
            from unnest(event_params)
            where key = 'page_title'
        ) as page_title,
        (
            select value.string_value
            from unnest(event_params)
            where key = 'page_referrer'
        ) as page_referrer,
        (
            select value.int_value
            from unnest(event_params)
            where key = 'entrances'
        ) as entrances,
        (
            select value.string_value
            from unnest(event_params)
            where key = 'engagement_time_msec'
        ) as engagement_time_msec,
        (
            select value.string_value
            from unnest(event_params)
            where key = 'session_engaged'
        ) as session_engaged,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,

        -- device
        device_category,
        device_operating_system,
        device_browser,
        device_language,

        -- geography
        geo_continent,
        geo_sub_continent,
        geo_country,
        geo_region,
        geo_city

    from s_events

),

page_events_with_timing as (

    select
        *,

        -- Navigation timing
        lead(event_ts) over (
            partition by user_fk, ga_session_id
            order by event_ts
        ) as next_page_ts,
        lag(page_location) over (
            partition by user_fk, ga_session_id
            order by event_ts
        ) as previous_page_location,

        -- Page sequencing
        row_number() over (
            partition by user_fk, ga_session_id
            order by event_ts
        ) as page_number_in_session,
        count(*) over (
            partition by user_fk, ga_session_id
        ) as total_pages_in_session

    from page_events_with_params

),

final as (

    select
        -- primary key (use event_pk as pageview_pk)
        event_pk as pageview_pk,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        event_ts as pageview_ts,

        -- page details
        page_location,
        page_title,
        page_referrer,
        previous_page_location,

        -- page path analysis
        regexp_extract(
            page_location,
            r'https?://[^/]+(/[^?#]*)'
        ) as page_path,
        regexp_extract(
            page_location,
            r'https?://([^/]+)'
        ) as page_hostname,
        regexp_extract(
            page_location,
            r'\?(.*)'
        ) as query_string,

        -- engagement metrics
        coalesce(
            safe_cast(engagement_time_msec as int64),
            0
        ) as engagement_time_msec,
        coalesce(
            safe_cast(engagement_time_msec as int64),
            0
        ) / 1000.0 as engagement_time_seconds,

        -- time on page calculation
        case
            when next_page_ts is not null
            then timestamp_diff(next_page_ts, event_ts, second)
            else null
        end as time_on_page_seconds,

        -- session context
        page_number_in_session,
        total_pages_in_session,

        -- entry/exit flags
        case
            when page_number_in_session = 1 then 1 else 0
        end as is_entrance,
        case
            when page_number_in_session = total_pages_in_session
            then 1
            else 0
        end as is_exit,
        coalesce(entrances, 0) as entrances,

        -- engagement flag
        case
            when safe_cast(session_engaged as int64) = 1 then 1 else 0
        end as is_engaged_session,

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
        device_language,

        -- geographic info
        geo_continent,
        geo_sub_continent,
        geo_country,
        geo_region,
        geo_city

    from page_events_with_timing

)

select * from final
