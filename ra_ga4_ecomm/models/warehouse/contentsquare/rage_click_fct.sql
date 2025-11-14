{{
    config(
        materialized = 'incremental',
        unique_key = 'rage_click_pk',
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

s_rage_clicks as (

    select * from {{ ref('stg_contentsquare__rage_click') }}
    {% if is_incremental() %}
        where event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

rage_clicks_with_keys as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'event_pk'
        ]) }} as rage_click_pk,

        -- all fields from staging
        *

    from s_rage_clicks

),

final as (

    select
        -- primary key
        rage_click_pk,

        -- foreign keys
        user_pseudo_id as user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,
        pageview_id,
        event_id,

        -- timestamps
        event_ts as rage_click_ts,

        -- event details
        event_name,
        event_type,

        -- page details
        domain,
        page_path,
        page_query,
        page_hash,
        landing_page,

        -- rage click details
        target_text,
        target_path,
        frustration_score,
        relative_time,
        click_count,

        -- traffic source
        traffic_source_source,
        traffic_source_medium,

        -- device
        device_operating_system,
        device_type,
        device_is_mobile,

        -- geography
        geo_country,
        geo_region,
        geo_city,

        -- ContentSquare-specific
        referrer,
        library

    from rage_clicks_with_keys

)

select * from final
