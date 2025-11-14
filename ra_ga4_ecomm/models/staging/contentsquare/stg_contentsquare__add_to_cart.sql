{{
    config(
        materialized = 'view',
        enabled = var('enable_contentsquare_source', false)
    )
}}

with

s_add_to_cart as (

    select * from {{ source('contentsquare', 'add_to_cart') }}

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
        cast(pageview_id as string) as pageview_id,

        -- event attributes
        'add_to_cart' as event_name,

        -- page details
        path as page_path,
        query as page_query,
        hash as page_hash,
        title as page_title,
        domain,
        href,
        target_text,

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
        type as event_type,
        referrer,
        landing_page,
        landing_page_query,
        landing_page_hash

    from s_add_to_cart

)

select * from renamed
