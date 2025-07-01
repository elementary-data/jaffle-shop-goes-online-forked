{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

with combined_ads as (
    select
        ad_id,
        date,
        campaign_name,
        ad_name,
        spend,
        clicks,
        impressions,
        'google' as utm_source
    from {{ source("ads", "stg_google_ads") }}

    union all

    select
        ad_id,
        date,
        campaign_name,
        ad_name,
        spend,
        clicks,
        impressions,
        'facebook' as utm_source
    from {{ source("ads", "stg_facebook_ads") }}

    union all

    select
        ad_id,
        date,
        campaign_name,
        ad_name,
        spend,
        clicks,
        impressions,
        'instagram' as utm_source
    from {{ source("ads", "stg_instagram_ads") }}
)

select
    ad_id,
    date,
    campaign_name,
    ad_name,
    spend,
    clicks,
    impressions,
    utm_source
from combined_ads
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}