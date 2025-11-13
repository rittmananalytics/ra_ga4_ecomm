{{
    config(
        materialized = 'view',
        enabled = var('enable_snowplow_source', false)
    )
}}

with

s_events as (

    select * from {{ source('snowplow_ecommerce_sample', 'events') }}

),

renamed as (

    select
        -- primary key
        event_id as event_pk,

        -- timestamps
        cast(collector_tstamp as date) as event_date,
        collector_tstamp as event_ts,

        -- identifiers
        user_pseudo_id,
        cast(session_id as string) as ga_session_id,
        user_id,
        domain_userid,
        network_userid,

        -- event attributes
        event_name,
        event_id,
        app_id,
        platform,
        debug_mode,

        -- ecommerce
        ecommerce,

        -- traffic source
        traffic_source.source as traffic_source_source,
        traffic_source.medium as traffic_source_medium,
        traffic_source.campaign as traffic_source_name,
        traffic_source.content as traffic_source_content,
        traffic_source.term as traffic_source_term,

        -- device
        device.category as device_category,
        device.os as device_operating_system,
        device.browser as device_browser,
        br_lang as device_language,
        br_family as browser_family,
        br_name as browser_name,
        br_version as browser_version,
        br_type as browser_type,
        br_renderengine as browser_renderengine,
        br_colordepth as browser_colordepth,
        br_viewheight as browser_viewheight,
        br_viewwidth as browser_viewwidth,
        br_cookies as browser_cookies_enabled,
        br_features_pdf as browser_supports_pdf,
        br_features_quicktime as browser_supports_quicktime,

        -- device details (Snowplow-specific)
        dvce_type as device_type,
        dvce_ismobile as device_is_mobile,
        dvce_screenheight as device_screen_height,
        dvce_screenwidth as device_screen_width,
        dvce_created_tstamp as device_created_ts,

        -- operating system
        os_name,
        os_family,
        os_manufacturer,
        os_timezone,

        -- geography
        cast(null as string) as geo_continent,
        cast(null as string) as geo_sub_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.region_name as geo_region_name,
        geo.city as geo_city,
        geo.zipcode as geo_zipcode,
        geo.latitude as geo_latitude,
        geo.longitude as geo_longitude,
        geo.timezone as geo_timezone,

        -- page details
        page_location,
        page_url,
        page_title,
        page_referrer,
        page_urlhost,
        page_urlpath,
        page_urlquery,
        page_urlfragment,

        -- engagement
        engagement_time_msec,

        -- user properties (Snowplow-specific)
        user_properties.customer_segment as user_customer_segment,
        user_properties.loyalty_tier as user_loyalty_tier,
        user_properties.subscription_status as user_subscription_status,

        -- session details (Snowplow-specific)
        domain_sessionidx as session_index,

        -- network
        user_ipaddress,

        -- ga4 adapter metadata
        ga4_adapter.source as ga4_adapter_source,
        ga4_adapter.original_event_name as ga4_adapter_original_event_name,
        ga4_adapter.data_layer_version as ga4_adapter_data_layer_version,

        -- etl metadata
        etl_tstamp,
        v_tracker as tracker_version,
        v_collector as collector_version,
        v_etl as etl_version

    from s_events

)

select * from renamed
