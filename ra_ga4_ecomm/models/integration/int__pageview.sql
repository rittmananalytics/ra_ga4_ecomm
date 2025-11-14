{{
    config(
        materialized = 'view'
    )
}}

with

{% if var('enable_ga4_source', true) %}
s_ga4_events as (

    select * from {{ ref('stg_ga4__event') }}
    where event_name = 'page_view'

),

ga4_pageviews as (

    select
        -- source identifier
        'ga4' as source,

        -- primary key fields
        event_pk,
        event_date,
        event_ts as pageview_ts,

        -- foreign keys
        user_pseudo_id as user_fk,
        cast(ga_session_id as string) as ga_session_id,

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

        -- Snowplow-specific page fields (null for GA4)
        cast(null as string) as page_url,
        cast(null as string) as page_urlhost,
        cast(null as string) as page_urlpath,
        cast(null as string) as page_urlquery,
        cast(null as string) as page_urlfragment,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        cast(null as string) as traffic_source_content,
        cast(null as string) as traffic_source_term,

        -- device
        device_category,
        device_operating_system,
        device_browser,
        device_language,

        -- device details (Snowplow-specific, null for GA4)
        cast(null as string) as browser_family,
        cast(null as string) as browser_name,
        cast(null as float64) as browser_version,
        cast(null as int64) as browser_viewheight,
        cast(null as int64) as browser_viewwidth,

        -- geography
        geo_continent,
        geo_sub_continent,
        geo_country,
        geo_region,
        cast(null as string) as geo_region_name,
        geo_city,
        cast(null as string) as geo_zipcode,
        cast(null as float64) as geo_latitude,
        cast(null as float64) as geo_longitude,
        cast(null as string) as geo_timezone,

        -- ContentSquare-specific Web Vitals (null for GA4)
        cast(null as float64) as first_input_delay,
        cast(null as float64) as interaction_to_next_paint,
        cast(null as float64) as time_to_first_byte,
        cast(null as float64) as largest_contentful_paint,
        cast(null as float64) as cumulative_layout_shift,
        cast(null as int64) as dom_interactive_after_msec,
        cast(null as int64) as fully_loaded,
        cast(null as float64) as first_contentful_paint,
        cast(null as float64) as start_render,
        cast(null as int64) as window_height,
        cast(null as int64) as window_width,
        cast(null as float64) as scroll_rate,
        cast(null as int64) as view_duration_msec

    from s_ga4_events

),
{% endif %}

{% if var('enable_snowplow_source', false) %}
s_snowplow_events as (

    select * from {{ ref('stg_snowplow__event') }}
    where event_name = 'page_view'

),

snowplow_pageviews as (

    select
        -- source identifier
        'snowplow' as source,

        -- primary key fields
        event_pk,
        event_date,
        event_ts as pageview_ts,

        -- foreign keys
        user_pseudo_id as user_fk,
        cast(ga_session_id as string) as ga_session_id,

        -- page parameters
        page_location,
        page_title,
        page_referrer,
        cast(null as int64) as entrances,
        cast(engagement_time_msec as string) as engagement_time_msec,
        cast(null as string) as session_engaged,

        -- Snowplow-specific page fields
        page_url,
        page_urlhost,
        page_urlpath,
        page_urlquery,
        page_urlfragment,

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
        browser_viewheight,
        browser_viewwidth,

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

        -- ContentSquare-specific Web Vitals (null for Snowplow)
        cast(null as float64) as first_input_delay,
        cast(null as float64) as interaction_to_next_paint,
        cast(null as float64) as time_to_first_byte,
        cast(null as float64) as largest_contentful_paint,
        cast(null as float64) as cumulative_layout_shift,
        cast(null as int64) as dom_interactive_after_msec,
        cast(null as int64) as fully_loaded,
        cast(null as float64) as first_contentful_paint,
        cast(null as float64) as start_render,
        cast(null as int64) as window_height,
        cast(null as int64) as window_width,
        cast(null as float64) as scroll_rate,
        cast(null as int64) as view_duration_msec

    from s_snowplow_events

),
{% endif %}

{% if var('enable_contentsquare_source', false) %}
s_contentsquare_pageviews as (

    select * from {{ ref('stg_contentsquare__pageview') }}

),

contentsquare_pageviews as (

    select
        -- source identifier
        'contentsquare' as source,

        -- primary key fields
        event_pk,
        event_date,
        event_ts as pageview_ts,

        -- foreign keys
        user_pseudo_id as user_fk,
        ga_session_id,

        -- page parameters (ContentSquare uses different field structure)
        concat('https://', coalesce(page_path, '/')) as page_location,
        page_title,
        referrer as page_referrer,
        case when is_first_view then 1 else 0 end as entrances,
        cast(view_duration_msec as string) as engagement_time_msec,
        cast(null as string) as session_engaged,

        -- Snowplow-specific page fields (partial mapping for ContentSquare)
        concat('https://', coalesce(page_path, '/')) as page_url,
        cast(null as string) as page_urlhost,
        page_path as page_urlpath,
        page_query as page_urlquery,
        page_hash as page_urlfragment,

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

        -- device details (ContentSquare-specific)
        browser_family,
        browser_name,
        browser_version,
        window_height as browser_viewheight,
        window_width as browser_viewwidth,

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

        -- ContentSquare-specific Web Vitals
        first_input_delay,
        interaction_to_next_paint,
        time_to_first_byte,
        largest_contentful_paint,
        cumulative_layout_shift,
        dom_interactive_after_msec,
        fully_loaded,
        first_contentful_paint,
        start_render,
        window_height,
        window_width,
        scroll_rate,
        view_duration_msec

    from s_contentsquare_pageviews

),
{% endif %}

final as (

    {% if var('enable_ga4_source', true) %}
    select * from ga4_pageviews
    {% endif %}

    {% if var('enable_ga4_source', true) and var('enable_snowplow_source', false) %}
    union all
    {% endif %}

    {% if var('enable_snowplow_source', false) %}
    select * from snowplow_pageviews
    {% endif %}

    {% if (var('enable_ga4_source', true) or var('enable_snowplow_source', false)) and var('enable_contentsquare_source', false) %}
    union all
    {% endif %}

    {% if var('enable_contentsquare_source', false) %}
    select * from contentsquare_pageviews
    {% endif %}

)

select * from final
