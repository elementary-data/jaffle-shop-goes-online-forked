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

-- Select columns explicitly per source so a schema change or extra column
-- in one feed cannot silently propagate via `select *`. All three sources
-- must expose `cost` in the same unit (dollars); they are unioned here and
-- summed into `total_spend` downstream in `ads_spend`, which feeds the ROAS
-- denominator in `cpa_and_roas`.
select
    ad_id,
    date,
    utm_medium,
    utm_campain,
    cost,
    'google' as utm_source
from google_ads
union all
select
    ad_id,
    date,
    utm_medium,
    utm_campain,
    cost,
    'facebook' as utm_source
from facebook_ads
union all
select
    ad_id,
    date,
    utm_medium,
    utm_campain,
    cost,
    'instagram' as utm_source
from instagram_ads
