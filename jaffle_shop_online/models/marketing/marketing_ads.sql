{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

{% set columns_to_select = [
    "ad_id",
    "date",
    "campaign_name",
    "ad_group_name",
    "spend",
    "impressions",
    "clicks"
] %}

with google_ads as (
    select 
        {{ columns_to_select | join(', ') }},
        'google' as utm_source
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select 
        {{ columns_to_select | join(', ') }},
        'facebook' as utm_source
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select 
        {{ columns_to_select | join(', ') }},
        'instagram' as utm_source
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
)

select 
    *,
    current_timestamp() as created_at
from google_ads

union all

select 
    *,
    current_timestamp() as created_at
from facebook_ads

union all

select 
    *,
    current_timestamp() as created_at
from instagram_ads
