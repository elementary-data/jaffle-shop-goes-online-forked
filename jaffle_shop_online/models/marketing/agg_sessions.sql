{{
    config(
        materialized = "incremental",
        unique_key = "session_id",
    )
}}

with app_sessions as (
    select
        session_id,
        customer_id,
        ad_id,
        utm_source,
        started_at,
        ended_at
    from {{ source("sessions", "stg_app_sessions") }}
    {% if is_incremental() %}
        where started_at > (select max(started_at) from {{ this }})
    {% endif %}
),

website_sessions as (
    select
        session_id,
        customer_id,
        ad_id,
        utm_source,
        started_at,
        ended_at
    from {{ source("sessions", "stg_website_sessions") }}
    {% if is_incremental() %}
        where started_at > (select max(started_at) from {{ this }})
    {% endif %}
)

select *, 'app' as platform
from app_sessions
union all
select *, 'website' as platform
from website_sessions
