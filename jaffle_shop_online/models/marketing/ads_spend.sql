{{
    config(
        materialized = "table",
    )
}}

select 
    date(date) as day, 
    utm_source, 
    utm_medium, 
    utm_campain, 
    sum(cost) as spend
from {{ ref("marketing_ads") }}
group by date(date), utm_source, utm_medium, utm_campain
