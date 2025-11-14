{{
    config(
        materialized = 'view',
        enabled = var('enable_contentsquare_source', false)
    )
}}

with

s_js_errors as (

    select * from {{ source('contentsquare', 'js_errors') }}

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
        'js_error' as event_name,
        type as event_type,

        -- error details
        error_message,
        error_line_number,
        js_error_file_name as error_file_name,
        js_error_column_number as error_column_number,
        errors_after_clicks,
        error_group_id,
        error_source,

        -- page details
        landing_page,

        -- traffic source
        referrer,

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
        library

    from s_js_errors

)

select * from renamed
