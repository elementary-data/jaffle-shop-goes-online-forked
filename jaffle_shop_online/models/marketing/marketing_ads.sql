{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

with google_ads as (
    select
        ad_id,
        date,
        campaign_name,
        ad_group_name,
        spend,
        impressions,
        clicks,
        'google' as utm_source
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select
        ad_id,
        date,
        campaign_name,
        ad_group_name,
        spend,
        impressions,
        clicks,
        'facebook' as utm_source
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select
        ad_id,
        date,
        campaign_name,
        ad_group_name,
        spend,
        impressions,
        clicks,
        'instagram' as utm_source
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
)

select 
    ad_id,
    date,
    campaign_name,
    ad_group_name,
    spend,
    impressions,
    clicks,
    utm_source,
    current_timestamp() as loaded_at
from google_ads

union all

select 
    ad_id,
    date,
    campaign_name,
    ad_group_name,
    spend,
    impressions,
    clicks,
    utm_source,
    current_timestamp() as loaded_at
from facebook_ads

union all

select 
    ad_id,
    date,
    campaign_name,
    ad_group_name,
    spend,
    impressions,
    clicks,
    utm_source,
    current_timestamp() as loaded_at
from instagram_ads