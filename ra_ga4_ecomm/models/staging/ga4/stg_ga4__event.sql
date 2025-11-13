{{
    config(
        materialized = 'incremental',
        unique_key = ['event_ts', 'user_pseudo_id'],
        partition_by = {
            "field": "event_date",
            "data_type": "date",
            "granularity": "day"
        },
        cluster_by = ["user_pseudo_id"],
        enabled = var('enable_ga4_source', true)
    )
}}

with

s_events as (

    select * from {{ source('ga4_obfuscated_sample_ecommerce', 'events_') }}
    where _TABLE_SUFFIX between '20201101' and '20210131'
    {% if is_incremental() %}
        and parse_date('%Y%m%d', event_date) > (
            select max(event_date) from {{ this }}
        )
    {% endif %}

),

renamed as (

    select
        -- primary key
        {{ dbt_utils.generate_surrogate_key([
            'event_timestamp',
            'user_pseudo_id',
            'event_name',
            'event_bundle_sequence_id'
        ]) }} as event_pk,

        -- timestamps
        parse_date('%Y%m%d', event_date) as event_date,
        timestamp_micros(event_timestamp) as event_ts,

        -- identifiers
        user_pseudo_id,
        (
            select value.int_value
            from unnest(event_params)
            where key = 'ga_session_id'
        ) as ga_session_id,

        -- event attributes
        event_name,
        event_bundle_sequence_id,
        event_params,

        -- ecommerce
        ecommerce,
        items,

        -- traffic source
        traffic_source.name as traffic_source_name,
        traffic_source.medium as traffic_source_medium,
        traffic_source.source as traffic_source_source,

        -- device
        device.category as device_category,
        device.operating_system as device_operating_system,
        device.web_info.browser as device_browser,
        device.language as device_language,

        -- geography
        geo.continent as geo_continent,
        geo.sub_continent as geo_sub_continent,
        geo.country as geo_country,
        geo.region as geo_region,
        geo.city as geo_city

    from s_events

)

select * from renamed
