{{ config(
    materialized='table',
    tags=['marketing']
) }}

-- Consider changing materialization to 'incremental' if this model is updated frequently
-- and queried often. This can significantly improve performance for large datasets.

with google_ads as (
    select
        date,
        campaign_id,
        campaign_name,
        ad_id,
        ad_name,
        clicks,
        impressions,
        spend
    from {{ ref('stg_google_ads') }}
),

facebook_ads as (
    select
        date,
        campaign_id,
        campaign_name,
        ad_id,
        ad_name,
        clicks,
        impressions,
        spend
    from {{ ref('stg_facebook_ads') }}
),

instagram_ads as (
    select
        date,
        campaign_id,
        campaign_name,
        ad_id,
        ad_name,
        clicks,
        impressions,
        spend
    from {{ ref('stg_instagram_ads') }}
)

select
    date,
    campaign_id,
    campaign_name,
    ad_id,
    ad_name,
    clicks,
    impressions,
    spend,
    'google' as utm_source
from google_ads
union all
select
    date,
    campaign_id,
    campaign_name,
    ad_id,
    ad_name,
    clicks,
    impressions,
    spend,
    'facebook' as utm_source
from facebook_ads
union all
select
    date,
    campaign_id,
    campaign_name,
    ad_id,
    ad_name,
    clicks,
    impressions,
    spend,
    'instagram' as utm_source
from instagram_ads