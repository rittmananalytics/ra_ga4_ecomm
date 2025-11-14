{{
    config(
        materialized = 'view',
        enabled = var('enable_contentsquare_source', false)
    )
}}

with

s_rage_clicks as (

    select * from {{ source('contentsquare', 'rage_click') }}

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
        'rage_click' as event_name,
        type as event_type,

        -- page details
        domain,
        path as page_path,
        `query` as page_query,
        `hash` as page_hash,
        landing_page,

        -- rage click details
        target_text,
        target_path,
        frustration_score,
        relative_time,
        value as click_count,

        -- traffic source
        cast(null as string) as traffic_source_source,
        cast(null as string) as traffic_source_medium,

        -- device
        platform as device_operating_system,
        device_type,
        case
            when lower(device_type) = 'mobile' then true
            when lower(device_type) = 'desktop' then false
            else null
        end as device_is_mobile,

        -- geography
        country as geo_country,
        region as geo_region,
        city as geo_city,

        -- ContentSquare-specific
        referrer,
        library

    from s_rage_clicks

)

select * from renamed
