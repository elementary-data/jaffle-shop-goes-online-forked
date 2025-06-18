{{
    config(
        materialized = "view",
    )
}}

with marketing_ads as (
    select *
    from {{ ref("marketing_ads") }}
)

select date as date_day, utm_source, utm_medium, utm_campaign, sum(cost) as spend
from marketing_ads
group by date, utm_source, utm_medium, utm_campaign
