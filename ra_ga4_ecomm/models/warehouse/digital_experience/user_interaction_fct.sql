{{
    config(
        materialized = 'incremental',
        unique_key = 'interaction_pk',
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

s_add_to_cart as (

    select * from {{ ref('stg_contentsquare__add_to_cart') }}
    {% if is_incremental() %}
        where event_date > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

interactions_with_keys as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'event_pk'
        ]) }} as interaction_pk,

        -- all fields from staging
        *

    from s_add_to_cart

),

final as (

    select
        -- primary key
        interaction_pk,

        -- foreign keys
        user_pseudo_id as user_fk,

        -- dates
        event_date,

        -- identifiers
        ga_session_id,
        pageview_id,
        event_id,

        -- timestamps
        event_ts as interaction_ts,

        -- event details
        event_name,
        event_type,

        -- page details
        page_path,
        page_query,
        page_hash,
        page_title,
        domain,
        href,
        target_text,

        -- traffic source
        traffic_source_name,
        traffic_source_medium,
        traffic_source_source,
        traffic_source_content,
        traffic_source_term,
        utm_campaign,

        -- device
        device_category,
        device_operating_system,
        device_browser,
        device_language,

        -- device details
        browser_name,
        browser_family,
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

        -- ContentSquare-specific
        referrer,
        landing_page,
        landing_page_query,
        landing_page_hash

    from interactions_with_keys

)

select * from final
