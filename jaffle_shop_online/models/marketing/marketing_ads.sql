{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

{% set common_columns = [
    "ad_id",
    "date",
    "campaign_id",
    "campaign_name",
    "ad_group_id",
    "ad_group_name",
    "impressions",
    "clicks",
    "cost"
] %}

with google_ads as (
    select
        {{ common_columns | join(', ') }}
    from {{ source("ads", "stg_google_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

facebook_ads as (
    select
        {{ common_columns | join(', ') }}
    from {{ source("ads", "stg_facebook_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
),

instagram_ads as (
    select
        {{ common_columns | join(', ') }}
    from {{ source("ads", "stg_instagram_ads") }}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
)

select *, 'google' as utm_source
from google_ads
union all
select *, 'facebook' as utm_source
from facebook_ads
union all
select *, 'instagram' as utm_source
from instagram_ads