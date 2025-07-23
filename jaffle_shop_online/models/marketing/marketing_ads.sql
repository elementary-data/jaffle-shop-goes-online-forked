{{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}}

with google_ads as (
    select 
        ad_id,
        date,
        cost,
        utm_medium,
        utm_campaign,
        clicks,
        impressions
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select 
        ad_id,
        date,
        cost,
        utm_medium,
        utm_campaign,
        clicks,
        impressions
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select 
        ad_id,
        date,
        cost,
        utm_medium,
        utm_campaign,
        clicks,
        impressions
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
)

select 
    ad_id,
    date,
    cost,
    utm_medium,
    utm_campaign,
    clicks,
    impressions,
    'google' as utm_source
from google_ads

union all

select 
    ad_id,
    date,
    cost,
    utm_medium,
    utm_campaign,
    clicks,
    impressions,
    'facebook' as utm_source
from facebook_ads

union all

select 
    ad_id,
    date,
    cost,
    utm_medium,
    utm_campaign,
    clicks,
    impressions,
    'instagram' as utm_source
from instagram_ads
