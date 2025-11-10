{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
        on_schema_change = "fail"
    )
}}

with google_ads as (
    select 
        ad_id,
        cost,
        date,
        utm_campain,
        utm_medium,
        'google' as utm_source
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select 
        ad_id,
        cost,
        date,
        utm_campain,
        utm_medium,
        'facebook' as utm_source
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select 
        ad_id,
        cost,
        date,
        utm_campain,
        utm_medium,
        'instagram' as utm_source
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
)

select * from google_ads
union all
select * from facebook_ads
union all
select * from instagram_ads
