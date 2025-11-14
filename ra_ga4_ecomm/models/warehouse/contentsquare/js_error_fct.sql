{{
    config(
        materialized = 'incremental',
        unique_key = 'js_error_pk',
        partition_by = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["user_fk", "ga_session_id"],
        enabled = var('enable_contentsquare_source', false)
    )
}}

with

s_js_errors as (

    select * from {{ ref('stg_contentsquare__js_error') }}
    {% if is_incremental() %}
        where event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

js_errors_with_keys as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'event_pk'
        ]) }} as js_error_pk,

        -- all fields from staging
        *

    from s_js_errors

),

final as (

    select
        -- primary key
        js_error_pk,

        -- foreign keys
        user_pseudo_id as user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,
        pageview_id,
        event_id,

        -- timestamps
        event_ts as error_ts,

        -- event details
        event_name,
        event_type,

        -- error details
        error_message,
        error_line_number,
        error_file_name,
        error_column_number,
        errors_after_clicks,
        error_group_id,
        error_source,

        -- page details
        landing_page,

        -- traffic source
        referrer,

        -- device
        device_operating_system,
        device_type,
        device_is_mobile,

        -- geography
        geo_country,
        geo_region,
        geo_city,

        -- ContentSquare-specific
        library

    from js_errors_with_keys

)

select * from final
