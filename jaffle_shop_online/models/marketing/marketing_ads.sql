{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

-- This model combines ad data from Google, Facebook, and Instagram
-- It's incrementally updated based on the 'date' column

with google_ads as (
    select
        ad_id,
        campaign_name,
        ad_group,
        headline,
        date,
        impressions,
        clicks,
        cost
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select
        ad_id,
        campaign_name,
        ad_set_name as ad_group,
        ad_name as headline,
        date,
        impressions,
        clicks,
        spend as cost
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select
        ad_id,
        campaign_name,
        ad_set_name as ad_group,
        ad_name as headline,
        date,
        impressions,
        clicks,
        spend as cost
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
)

select *, 'google' as utm_source
from google_ads
union all
select *, 'facebook' as utm_source
from facebook_ads
union all
select *, 'instagram' as utm_source
from instagram_ads