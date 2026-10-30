{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

-- NOTE: cost must be in the same currency unit (dollars) across every ad
-- source before it is unioned, otherwise total_spend downstream in ads_spend
-- and the ROAS calculation in cpa_and_roas are silently corrupted. One source
-- began reporting cost in cents, which inflated its spend ~100x. We normalize
-- that source to dollars with the project's cents_to_dollars macro so all three
-- feeds are consistent. Select columns explicitly (instead of *) so the cost
-- unit is enforced at the union boundary.
with google_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost
    from {{ source("ads", "stg_google_ads") }}
),

facebook_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost
    from {{ source("ads", "stg_facebook_ads") }}
),

instagram_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        -- this feed reports cost in cents; convert to dollars to match the others
        {{ cents_to_dollars('cost') }} as cost
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
