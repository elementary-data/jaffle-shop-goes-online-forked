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
        utm_campain,
        date,
        cost,
        utm_medium
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select 
        ad_id,
        utm_campain,
        date,
        cost,
        utm_medium
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select 
        ad_id,
        utm_campain,
        date,
        cost,
        utm_medium
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
        where date > (select max(date) from {{ this }})
    {% endif %}
)

select 
    ad_id,
    utm_campain,
    date,
    'google' as utm_source,
    cost,
    utm_medium
from google_ads
union all
select 
    ad_id,
    utm_campain,
    date,
    'facebook' as utm_source,
    cost,
    utm_medium
from facebook_ads
union all
select 
    ad_id,
    utm_campain,
    date,
    'instagram' as utm_source,
    cost,
    utm_medium
from instagram_ads
