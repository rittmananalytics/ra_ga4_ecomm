{{
    config(
        materialized = 'incremental',
        unique_key = ['event_date', 'user_fk', 'ga_session_id'],
        partition_by = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["user_fk", "ga_session_id"]
    )
}}

with

s_sessions as (

    select * from {{ ref('int__session') }}
    {% if is_incremental() %}
        where event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

session_with_keys as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'source',
            'event_date',
            'user_fk',
            'ga_session_id'
        ]) }} as session_pk,

        -- all fields from integration
        *

    from s_sessions

),

final as (

    select
        -- primary key
        session_pk,

        -- source identifier
        source,

        -- foreign keys
        user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,
        session_index,

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
        user_subscription_status,

        -- ContentSquare-specific session metrics
        frustration_score,
        looping_index,
        page_consumption

    from session_with_keys

)

select * from final
