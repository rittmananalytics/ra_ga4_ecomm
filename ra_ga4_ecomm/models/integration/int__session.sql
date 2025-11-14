{{
    config(
        materialized = 'view'
    )
}}

with

{% if var('enable_ga4_source', true) %}
s_ga4_events as (

    select * from {{ ref('stg_ga4__event') }}
    where ga_session_id is not null

),

ga4_sessions as (

    select
        -- source identifier
        'ga4' as source,

        -- dates
        event_date,

        -- identifiers
        user_pseudo_id as user_fk,
        cast(ga_session_id as string) as ga_session_id,
        cast(null as int64) as session_index,

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
        cast(null as string) as traffic_source_content,
        cast(null as string) as traffic_source_term,

        -- device
        any_value(device_category) as device_category,
        any_value(device_operating_system) as device_operating_system,
        any_value(device_browser) as device_browser,
        any_value(device_language) as device_language,

        -- device details (Snowplow-specific, null for GA4)
        cast(null as string) as browser_family,
        cast(null as string) as browser_name,
        cast(null as float64) as browser_version,
        cast(null as string) as device_type,
        cast(null as bool) as device_is_mobile,
        cast(null as int64) as device_screen_height,
        cast(null as int64) as device_screen_width,

        -- geography
        any_value(geo_continent) as geo_continent,
        any_value(geo_sub_continent) as geo_sub_continent,
        any_value(geo_country) as geo_country,
        any_value(geo_region) as geo_region,
        cast(null as string) as geo_region_name,
        any_value(geo_city) as geo_city,
        cast(null as string) as geo_zipcode,
        cast(null as float64) as geo_latitude,
        cast(null as float64) as geo_longitude,
        cast(null as string) as geo_timezone,

        -- user properties (Snowplow-specific, null for GA4)
        cast(null as string) as user_customer_segment,
        cast(null as string) as user_loyalty_tier,
        cast(null as string) as user_subscription_status,

        -- ContentSquare-specific session metrics (null for GA4)
        cast(null as int64) as frustration_score,
        cast(null as int64) as looping_index,
        cast(null as int64) as page_consumption

    from s_ga4_events
    group by
        event_date,
        user_pseudo_id,
        cast(ga_session_id as string)

),
{% endif %}

{% if var('enable_snowplow_source', false) %}
s_snowplow_events as (

    select * from {{ ref('stg_snowplow__event') }}
    where ga_session_id is not null

),

snowplow_sessions as (

    select
        -- source identifier
        'snowplow' as source,

        -- dates
        event_date,

        -- identifiers
        user_pseudo_id as user_fk,
        cast(ga_session_id as string) as ga_session_id,
        any_value(session_index) as session_index,

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
            coalesce(engagement_time_msec, 0)
        ) as session_engagement_time_msec,

        -- conversion metrics
        count(
            case when event_name = 'purchase' then 1 end
        ) as purchase_events_per_session,
        sum(
            case
                when event_name = 'purchase'
                then ecommerce.value
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
        any_value(traffic_source_content) as traffic_source_content,
        any_value(traffic_source_term) as traffic_source_term,

        -- device
        any_value(device_category) as device_category,
        any_value(device_operating_system) as device_operating_system,
        any_value(device_browser) as device_browser,
        any_value(device_language) as device_language,

        -- device details (Snowplow-specific)
        any_value(browser_family) as browser_family,
        any_value(browser_name) as browser_name,
        any_value(browser_version) as browser_version,
        any_value(device_type) as device_type,
        any_value(device_is_mobile) as device_is_mobile,
        any_value(device_screen_height) as device_screen_height,
        any_value(device_screen_width) as device_screen_width,

        -- geography
        cast(null as string) as geo_continent,
        any_value(geo_sub_continent) as geo_sub_continent,
        any_value(geo_country) as geo_country,
        any_value(geo_region) as geo_region,
        any_value(geo_region_name) as geo_region_name,
        any_value(geo_city) as geo_city,
        any_value(geo_zipcode) as geo_zipcode,
        any_value(geo_latitude) as geo_latitude,
        any_value(geo_longitude) as geo_longitude,
        any_value(geo_timezone) as geo_timezone,

        -- user properties (Snowplow-specific)
        any_value(user_customer_segment) as user_customer_segment,
        any_value(user_loyalty_tier) as user_loyalty_tier,
        any_value(user_subscription_status) as user_subscription_status,

        -- ContentSquare-specific session metrics (null for Snowplow)
        cast(null as int64) as frustration_score,
        cast(null as int64) as looping_index,
        cast(null as int64) as page_consumption

    from s_snowplow_events
    group by
        event_date,
        user_pseudo_id,
        cast(ga_session_id as string)

),
{% endif %}

{% if var('enable_contentsquare_source', false) %}
s_contentsquare_sessions as (

    select * from {{ source('contentsquare', 'sessions') }}

),

contentsquare_sessions as (

    select
        -- source identifier
        'contentsquare' as source,

        -- dates
        cast(time as date) as event_date,

        -- identifiers
        cast(user_id as string) as user_fk,
        cast(session_id as string) as ga_session_id,
        cast(null as int64) as session_index,

        -- timestamps (ContentSquare provides session start time)
        time as session_start_ts,
        timestamp_add(
            time,
            interval safe_cast(session_duration as int64) second
        ) as session_end_ts,

        -- session metrics (ContentSquare pre-aggregates these)
        safe_cast(session_duration as int64) as session_duration_seconds,
        safe_cast(session_number_of_views as int64) as page_views_per_session,
        safe_cast(session_number_of_views as int64) as events_per_session,  -- approximate
        cast(null as int64) as session_engagement_time_msec,

        -- conversion metrics (ContentSquare doesn't track purchases in sessions table)
        cast(null as int64) as purchase_events_per_session,
        cast(null as float64) as purchase_revenue_per_session,
        0 as is_conversion_session,

        -- traffic source
        cast(null as string) as traffic_source_name,
        utm_medium as traffic_source_medium,
        utm_source as traffic_source_source,
        utm_content as traffic_source_content,
        utm_term as traffic_source_term,

        -- device
        cast(null as string) as device_category,
        platform as device_operating_system,
        browser as device_browser,
        session_language as device_language,

        -- device details (ContentSquare-specific)
        browser as browser_family,
        browser as browser_name,
        cast(null as float64) as browser_version,
        device_type,
        case
            when lower(device_type) = 'mobile' then true
            when lower(device_type) = 'desktop' then false
            else null
        end as device_is_mobile,
        cast(null as int64) as device_screen_height,
        cast(null as int64) as device_screen_width,

        -- geography
        cast(null as string) as geo_continent,
        cast(null as string) as geo_sub_continent,
        country as geo_country,
        region as geo_region,
        region as geo_region_name,
        city as geo_city,
        cast(null as string) as geo_zipcode,
        cast(null as float64) as geo_latitude,
        cast(null as float64) as geo_longitude,
        cast(null as string) as geo_timezone,

        -- user properties (null for ContentSquare)
        cast(null as string) as user_customer_segment,
        cast(null as string) as user_loyalty_tier,
        cast(null as string) as user_subscription_status,

        -- ContentSquare-specific session metrics (not in GA4/Snowplow)
        safe_cast(frustration_score as int64) as frustration_score,
        safe_cast(looping_index as int64) as looping_index,
        safe_cast(page_consumption as int64) as page_consumption

    from s_contentsquare_sessions

),
{% endif %}

final as (

    {% if var('enable_ga4_source', true) %}
    select * from ga4_sessions
    {% endif %}

    {% if var('enable_ga4_source', true) and var('enable_snowplow_source', false) %}
    union all
    {% endif %}

    {% if var('enable_snowplow_source', false) %}
    select * from snowplow_sessions
    {% endif %}

    {% if (var('enable_ga4_source', true) or var('enable_snowplow_source', false)) and var('enable_contentsquare_source', false) %}
    union all
    {% endif %}

    {% if var('enable_contentsquare_source', false) %}
    select * from contentsquare_sessions
    {% endif %}

)

select * from final
