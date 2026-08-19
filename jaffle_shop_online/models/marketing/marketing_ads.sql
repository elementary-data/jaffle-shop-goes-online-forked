{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

-- Each ad platform reports `cost` in its own native unit. Unioning the raw
-- `cost` columns together (via `select *`) produced inconsistent spend and
-- corrupted downstream ROAS (cpa_and_roas.return_on_advertising_spend).
-- Normalize every source to a common currency unit (whole units of currency)
-- BEFORE the union so total_spend is comparable across sources.
--
-- ACTION REQUIRED (reviewer): confirm each platform's native cost unit and set
-- the per-source conversion factor below. Factor = raw_cost -> whole currency.
--   * 1        if the source already reports whole currency units
--   * / 1000000 if the source reports micro-currency (e.g. Google Ads API micros)
--   * / 100    if the source reports minor units (cents)

with google_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        -- Google Ads API commonly reports cost in micros (1e-6 of a currency unit)
        cost / 1000000 as cost
    from {{ source("ads", "stg_google_ads") }}
),

facebook_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        -- Facebook Ads reports spend in whole currency units
        cost as cost
    from {{ source("ads", "stg_facebook_ads") }}
),

instagram_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        -- Instagram Ads reports spend in whole currency units
        cost as cost
    from {{ source("ads", "stg_instagram_ads") }}
)

select ad_id, date, utm_medium, utm_campain, cost, 'google' as utm_source
from google_ads
union all
select ad_id, date, utm_medium, utm_campain, cost, 'facebook' as utm_source
from facebook_ads
union all
select ad_id, date, utm_medium, utm_campain, cost, 'instagram' as utm_source
from instagram_ads
