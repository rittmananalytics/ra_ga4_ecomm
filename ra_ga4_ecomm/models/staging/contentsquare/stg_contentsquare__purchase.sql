{{
    config(
        materialized = 'view',
        enabled = var('enable_contentsquare_source', false)
    )
}}

with

s_transactions as (

    select * from {{ source('contentsquare', 'ecommerce_transactions') }}

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
        cast(event_id as string) as transaction_id,  -- using event_id as transaction_id

        -- event attributes
        'purchase' as event_name,

        -- transaction details
        transaction_amount,
        transaction_currency,

        -- traffic source
        cast(null as string) as traffic_source_name,
        cast(null as string) as traffic_source_medium,
        cast(null as string) as traffic_source_source,
        cast(null as string) as traffic_source_content,
        cast(null as string) as traffic_source_term,

        -- device
        cast(null as string) as device_category,
        platform as device_operating_system,
        cast(null as string) as device_browser,
        cast(null as string) as device_language,

        -- device details (ContentSquare-specific)
        cast(null as string) as browser_name,
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
        referrer

    from s_transactions

)

select * from renamed
