{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

-- Normalize each source's `cost` to a common currency unit (dollars) before
-- unioning. Each platform feed can report cost in a different unit; if a source
-- silently switches units (e.g. cents/micros instead of dollars) an unnormalized
-- `select *` union inflates total_spend and corrupts downstream CPA / ROAS.
-- The per-source multipliers below convert each feed into dollars. Set the
-- multiplier for a source to its unit factor (e.g. 0.01 for cents, 1e-6 for
-- micros); default 1 keeps existing behavior until the offending feed is confirmed.
{% set google_cost_to_usd = var("google_ads_cost_to_usd", 1) %}
{% set facebook_cost_to_usd = var("facebook_ads_cost_to_usd", 1) %}
{% set instagram_cost_to_usd = var("instagram_ads_cost_to_usd", 1) %}

with google_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost * {{ google_cost_to_usd }} as cost,
        'google' as utm_source
    from {{ source("ads", "stg_google_ads") }}
),

facebook_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost * {{ facebook_cost_to_usd }} as cost,
        'facebook' as utm_source
    from {{ source("ads", "stg_facebook_ads") }}
),

instagram_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost * {{ instagram_cost_to_usd }} as cost,
        'instagram' as utm_source
    from {{ source("ads", "stg_instagram_ads") }}
)

select ad_id, date, utm_medium, utm_campain, cost, utm_source
from google_ads
union all
select ad_id, date, utm_medium, utm_campain, cost, utm_source
from facebook_ads
union all
select ad_id, date, utm_medium, utm_campain, cost, utm_source
from instagram_ads
