{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

with google_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost,
        'google' as utm_source
    from {{ source("ads", "stg_google_ads") }}
),

facebook_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost,
        'facebook' as utm_source
    from {{ source("ads", "stg_facebook_ads") }}
),

instagram_ads as (
    select
        ad_id,
        date,
        utm_medium,
        utm_campain,
        cost,
        'instagram' as utm_source
    from {{ source("ads", "stg_instagram_ads") }}
)

select * from google_ads
union all
select * from facebook_ads
union all
select * from instagram_ads
