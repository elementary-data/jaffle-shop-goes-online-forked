select
    id,
    user_id,
    session_start,
    session_end,
    page_views,
    cart_adds,
    purchases,
    'app' as platform
from {{ ref('stg_app_sessions') }}

union all

select
    id,
    user_id,
    session_start,
    session_end,
    page_views,
    cart_adds,
    purchases,
    'website' as platform
from {{ ref('stg_website_sessions') }}