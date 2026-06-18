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

-- Normalize cost to a common currency unit (dollars) before unioning.
-- Facebook reports cost in cents, while Google and Instagram report in dollars.
-- Without this conversion, total ad spend is inflated and ROAS collapses.
select ad_id, date, utm_medium, utm_campain, cost, 'google' as utm_source
from google_ads
union all
select ad_id, date, utm_medium, utm_campain, cost / 100.0 as cost, 'facebook' as utm_source
from facebook_ads
union all
select ad_id, date, utm_medium, utm_campain, cost, 'instagram' as utm_source
from instagram_ads
