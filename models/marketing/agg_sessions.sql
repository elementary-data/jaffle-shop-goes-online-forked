with combined_sessions as (
    select
        session_id,
        user_id,
        start_time,
        end_time,
        duration_seconds,
        'app' as platform
    from {{ ref('stg_app_sessions') }}
    
    union all
    
    select
        session_id,
        user_id,
        start_time,
        end_time,
        duration_seconds,
        'website' as platform
    from {{ ref('stg_website_sessions') }}
)

select
    session_id,
    user_id,
    start_time,
    end_time,
    duration_seconds,
    platform
from combined_sessions