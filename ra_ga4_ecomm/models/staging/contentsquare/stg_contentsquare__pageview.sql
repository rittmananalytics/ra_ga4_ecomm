{{
    config(
        materialized = 'view',
        enabled = var('enable_contentsquare_source', false)
    )
}}

with

s_pageviews as (

    select * from {{ source('contentsquare', 'pageviews') }}

),

renamed as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'event_id'
        ]) }} as event_pk,

        -- timestamps
        cast(time as date) as event_date,
        time as event_ts,

        -- identifiers
        cast(user_id as string) as user_pseudo_id,
        cast(session_id as string) as ga_session_id,
        cast(event_id as string) as event_id,

        -- event attributes (ContentSquare doesn't have event names, using 'page_view')
        'page_view' as event_name,

        -- page details
        path as page_path,
        `query` as page_query,
        `hash` as page_hash,
        title as page_title,

        -- pageview metrics (ContentSquare-specific)
        cast(view_number as int64) as view_number,
        case when is_first = '1' then true else false end as is_first_view,
        case when is_last = '1' then true else false end as is_last_view,
        safe_cast(scroll_rate as float64) as scroll_rate,
        safe_cast(view_duration_msec as int64) as view_duration_msec,

        -- web vitals (ContentSquare-specific)
        safe_cast(first_input_delay as float64) as first_input_delay,
        safe_cast(interaction_to_next_paint as float64) as interaction_to_next_paint,
        safe_cast(time_to_first_byte as float64) as time_to_first_byte,
        safe_cast(largest_contentful_paint as float64) as largest_contentful_paint,
        safe_cast(cumulative_layout_shift as float64) as cumulative_layout_shift,
        safe_cast(dom_interactive_after_msec as int64) as dom_interactive_after_msec,
        safe_cast(fully_loaded as int64) as fully_loaded,
        safe_cast(first_contentful_paint as float64) as first_contentful_paint,
        safe_cast(start_render as float64) as start_render,

        -- window dimensions
        safe_cast(window_height as int64) as window_height,
        safe_cast(window_width as int64) as window_width,

        -- traffic source
        cast(null as string) as traffic_source_name,
        utm_medium as traffic_source_medium,
        utm_source as traffic_source_source,
        utm_content as traffic_source_content,
        utm_term as traffic_source_term,
        utm_campaign,

        -- device
        cast(null as string) as device_category,
        platform as device_operating_system,
        browser as device_browser,
        cast(null as string) as device_language,

        -- device details (ContentSquare-specific)
        browser as browser_name,
        cast(null as string) as browser_family,
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

        -- ContentSquare-specific
        referrer,
        landing_page,
        session_replay_link

    from s_pageviews

)

select * from renamed
