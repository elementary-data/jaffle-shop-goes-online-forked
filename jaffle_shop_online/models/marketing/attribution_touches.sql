{{
    config(
        materialized = "incremental",
        unique_key = "customer_id || '_' || converted_at || '_' || session_id",
        on_schema_change = "fail",
        cluster_by = ["customer_id", "converted_at"]
    )
}}

with customer_conversions as (
    select * from {{ ref('customer_conversions') }}
    {% if is_incremental() %}
        -- Only process conversions from the last few days to capture any late-arriving data
        where converted_at >= (
            select dateadd('day', -3, max(converted_at))
            from {{ this }}
        )
    {% endif %}
),

-- Pre-filter sessions to a reasonable time window to reduce join volume
sessions_filtered as (
    select 
        customer_id,
        session_id,
        started_at,
        ended_at,
        utm_source,
        utm_medium
    from {{ ref('sessions') }}
    where started_at >= (
        select dateadd('day', -{{ var('conversion_window_days', 7) }} - 1, min(converted_at))
        from customer_conversions
    )
),

-- Optimized join with pre-calculated attribution window
attribution_eligible_sessions as (
    select 
        conversions.customer_id,
        conversions.converted_at,
        conversions.revenue,
        conversions.order_id,
        sessions.session_id,
        sessions.started_at,
        sessions.ended_at,
        sessions.utm_source,
        sessions.utm_medium,
        datediff('day', sessions.started_at, conversions.converted_at) as days_before_conversion,
        datediff('hour', sessions.started_at, conversions.converted_at) as hours_before_conversion
    from customer_conversions as conversions
    inner join sessions_filtered as sessions 
        on conversions.customer_id = sessions.customer_id 
        and sessions.started_at <= conversions.converted_at
        and sessions.started_at >= dateadd('day', -{{ var('conversion_window_days', 7) }}, conversions.converted_at)
),

-- Consolidated window functions and attribution calculations
final_attribution as (
    select
        customer_id,
        converted_at,
        revenue,
        order_id,
        session_id,
        started_at,
        ended_at,
        utm_source,
        utm_medium,
        days_before_conversion,
        hours_before_conversion,
        
        -- Calculate all window functions in one pass
        row_number() over (
            partition by customer_id, converted_at 
            order by started_at
        ) as session_index,
        
        count(*) over (
            partition by customer_id, converted_at
        ) as total_sessions,
        
        -- Calculate attribution weights directly
        1.0 / count(*) over (partition by customer_id, converted_at) as linear_weight,
        
        case 
            when row_number() over (partition by customer_id, converted_at order by started_at) = 1 
            then 1.0 
            else 0.0 
        end as first_touch_weight,
        
        case 
            when row_number() over (partition by customer_id, converted_at order by started_at) = 
                 count(*) over (partition by customer_id, converted_at)
            then 1.0 
            else 0.0 
        end as last_touch_weight,
        
        -- Time-decay weight calculation
        power(0.7, count(*) over (partition by customer_id, converted_at) - 
                  row_number() over (partition by customer_id, converted_at order by started_at)) / 
        sum(power(0.7, count(*) over (partition by customer_id, converted_at) - 
                     row_number() over (partition by customer_id, converted_at order by started_at))) 
        over (partition by customer_id, converted_at) as time_decay_weight
        
    from attribution_eligible_sessions
),

-- Add final calculations
with_all_weights as (
    select
        *,
        -- 40/20/40 model calculation
        case 
            when total_sessions = 1 then 1.0
            when session_index = 1 then 0.4
            when session_index = total_sessions then 0.4
            else 0.2 / greatest(1, total_sessions - 2)
        end as forty_twenty_forty_weight
    from final_attribution
)

select
    customer_id,
    converted_at,
    revenue,
    order_id,
    session_id,
    started_at,
    ended_at,
    utm_source,
    utm_medium,
    days_before_conversion,
    hours_before_conversion,
    session_index,
    total_sessions,
    
    -- Attribution weights
    linear_weight,
    first_touch_weight,
    last_touch_weight,
    forty_twenty_forty_weight,
    time_decay_weight,
    
    -- Revenue attribution
    revenue * first_touch_weight as first_touch_revenue,
    revenue * last_touch_weight as last_touch_revenue,
    revenue * forty_twenty_forty_weight as forty_twenty_forty_revenue,
    revenue * linear_weight as linear_revenue,
    revenue * time_decay_weight as time_decay_revenue,
    
    -- Points attribution
    linear_weight as linear_points,
    first_touch_weight as first_touch_points,
    last_touch_weight as last_touch_points,
    forty_twenty_forty_weight as forty_twenty_forty_points,
    time_decay_weight as time_decay_points
    
from with_all_weights