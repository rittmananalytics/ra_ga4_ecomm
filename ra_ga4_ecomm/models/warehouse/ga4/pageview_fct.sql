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

s_pageviews as (

    select * from {{ ref('int__pageview') }}
    {% if is_incremental() %}
        where event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

pageviews_with_timing as (

    select
        *,

        -- Navigation timing
        lead(pageview_ts) over (
            partition by source, user_fk, ga_session_id
            order by pageview_ts
        ) as next_page_ts,
        lag(page_location) over (
            partition by source, user_fk, ga_session_id
            order by pageview_ts
        ) as previous_page_location,

        -- Page sequencing
        row_number() over (
            partition by source, user_fk, ga_session_id
            order by pageview_ts
        ) as page_number_in_session,
        count(*) over (
            partition by source, user_fk, ga_session_id
        ) as total_pages_in_session

    from s_pageviews

),

pageviews_with_keys as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'source',
            'event_pk'
        ]) }} as pageview_pk,

        -- all fields from integration
        *

    from pageviews_with_timing

),

final as (

    select
        -- primary key
        pageview_pk,

        -- source identifier
        source,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,

        -- timestamps
        pageview_ts,

        -- page details
        page_location,
        page_title,
        page_referrer,
        previous_page_location,

        -- Snowplow-specific page details
        page_url,
        page_urlhost,
        page_urlpath,
        page_urlquery,
        page_urlfragment,

        -- page path analysis
        regexp_extract(
            coalesce(page_location, page_url),
            r'https?://[^/]+(/[^?#]*)'
        ) as page_path,
        regexp_extract(
            coalesce(page_location, page_url),
            r'https?://([^/]+)'
        ) as page_hostname,
        regexp_extract(
            coalesce(page_location, page_url),
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
            then timestamp_diff(next_page_ts, pageview_ts, second)
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
        browser_viewheight,
        browser_viewwidth,

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
        geo_timezone

    from pageviews_with_keys

)

select * from final
