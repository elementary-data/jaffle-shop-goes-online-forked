{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

with google_ads as (
    select *
    from {{ source("ads", "stg_google_ads") }}
),

facebook_ads as (
    select *
    from {{ source("ads", "stg_facebook_ads") }}
),

instagram_ads as (
    select *
    from {{ source("ads", "stg_instagram_ads") }}
)

-- Normalize cost to a single currency unit (USD) across sources.
-- Google Ads reports cost in micros (millionths of the account currency),
-- while Facebook and Instagram report cost in whole dollars. Unioning the
-- raw `cost` values inflates total_spend and collapses ROAS downstream, so
-- Google's cost is divided by 1,000,000 here.
-- NOTE: pending validation against raw source values before merge.
select
    ad_id,
    cost / 1000000.0 as cost,
    date,
    utm_campain,
    utm_medium,
    'google' as utm_source
from google_ads
union all
select
    ad_id,
    cost,
    date,
    utm_campain,
    utm_medium,
    'facebook' as utm_source
from facebook_ads
union all
select
    ad_id,
    cost,
    date,
    utm_campain,
    utm_medium,
    'instagram' as utm_source
from instagram_ads
