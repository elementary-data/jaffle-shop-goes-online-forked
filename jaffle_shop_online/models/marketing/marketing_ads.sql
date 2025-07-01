{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

{% set columns = [
    "ad_id",
    "date",
    "campaign_name",
    "ad_group_name",
    "spend",
    "impressions",
    "clicks",
    "conversions"
] %}

with google_ads as (
    select
        {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select
        {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select
        {% for column in columns %}
        {{ column }}{% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
)

select 
    {% for column in columns %}
    {{ column }},
    {% endfor %}
    'google' as utm_source
from google_ads
union all
select 
    {% for column in columns %}
    {{ column }},
    {% endfor %}
    'facebook' as utm_source
from facebook_ads
union all
select 
    {% for column in columns %}
    {{ column }},
    {% endfor %}
    'instagram' as utm_source
from instagram_ads