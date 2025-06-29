{{
    config(
        materialized = "incremental",
        unique_key = "ad_id",
    )
}}

with google_ads as (
    select
        ad_id,
        campaign_id,
        ad_group_id,
        headline,
        description,
        final_url,
        ad_type,
        status,
        clicks,
        impressions,
        cost,
        created_at,
        updated_at
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select
        ad_id,
        campaign_id,
        ad_group_id,
        headline,
        description,
        final_url,
        ad_type,
        status,
        clicks,
        impressions,
        cost,
        created_at,
        updated_at
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select
        ad_id,
        campaign_id,
        ad_group_id,
        headline,
        description,
        final_url,
        ad_type,
        status,
        clicks,
        impressions,
        cost,
        created_at,
        updated_at
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
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