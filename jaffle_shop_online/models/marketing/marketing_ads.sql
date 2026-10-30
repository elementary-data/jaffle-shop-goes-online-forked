{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

-- NOTE: `cost` from every ad platform MUST be expressed in USD (dollars)
-- before it is unioned here. Downstream `ads_spend.spend` and the ROAS /
-- CPA metrics in `cpa_and_roas` assume a single, consistent unit. If a
-- platform's feed starts reporting cost in a different unit (e.g. cents),
-- convert it to dollars in that source's CTE below. This `unit_divisor`
-- pattern keeps the conversion explicit and per-source so a future unit
-- mismatch is visible and correctable in one place.
{% set cost_unit_divisor = {
    "google": 1,
    "facebook": 1,
    "instagram": 1,
} %}

with google_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost / {{ cost_unit_divisor["google"] }} as cost,
        'google' as utm_source
    from {{ source("ads", "stg_google_ads") }}
),

facebook_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost / {{ cost_unit_divisor["facebook"] }} as cost,
        'facebook' as utm_source
    from {{ source("ads", "stg_facebook_ads") }}
),

instagram_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost / {{ cost_unit_divisor["instagram"] }} as cost,
        'instagram' as utm_source
    from {{ source("ads", "stg_instagram_ads") }}
)

select * from google_ads
union all
select * from facebook_ads
union all
select * from instagram_ads
