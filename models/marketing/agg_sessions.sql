select 
    session_id,
    user_id,
    started_at,
    ended_at,
    duration_seconds,
    'app' as platform
from {{ ref('stg_app_sessions') }}

union all

select 
    session_id,
    user_id,
    started_at,
    ended_at,
    duration_seconds,
    'website' as platform
from {{ ref('stg_website_sessions') }}