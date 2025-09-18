{{
    config(
        materialized = "incremental",
        unique_key = ['day', 'utm_source', 'utm_medium', 'utm_campain'],
        on_schema_change = "fail"
    )
}}

with marketing_ads as (
    select 
        date,
        utm_source,
        utm_medium,
        utm_campain,
        cost
    from {{ ref("marketing_ads") }}
    
    {% if is_incremental() %}
        -- Only process new/updated data in incremental runs
        where date > (select max(day) from {{ this }})
    {% endif %}
)

select 
    date(date) as day, 
    utm_source, 
    utm_medium, 
    utm_campain, 
    sum(cost) as spend
from marketing_ads
group by date(date), utm_source, utm_medium, utm_campain
