{{
    config(
        materialized = "incremental",
        unique_key = ["customer_id", "order_id", "session_id"],
        on_schema_change = "sync_all_columns"
    )
}}

with customer_conversions as (
    select
        customer_id,
        converted_at,
        revenue,
        order_id
    from {{ ref('customer_conversions') }}
    {% if is_incremental() %}
    where converted_at > (select max(converted_at) from {{ this }})
    {% endif %}
),

sessions as (
    select
        customer_id,
        session_id,
        started_at,
        ended_at,
        utm_source,
        utm_medium
    from {{ ref('sessions') }}
),

-- Find all sessions that could have contributed to each conversion
-- (all sessions before the conversion within a reasonable attribution window)
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
        datediff('day', sessions.started_at, conversions.converted_at)  as days_before_conversion,
        datediff('hour', sessions.started_at, conversions.converted_at) as hours_before_conversion
    from customer_conversions as conversions
    inner join sessions 
        on conversions.customer_id = sessions.customer_id 
        and sessions.started_at <= conversions.converted_at  -- Session must be before conversion
        and datediff('day', sessions.started_at, conversions.converted_at) <= {{ var('conversion_window_days', 7) }}  -- attribution window matches conversion window
),

-- Calculate session sequence and total sessions per conversion
sessions_with_sequence as (
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
        row_number() over (
            partition by customer_id, converted_at 
            order by started_at
        ) as session_index,
        count(*) over (
            partition by customer_id, converted_at
        ) as total_sessions
    from attribution_eligible_sessions
),

-- Calculate attribution weights for different models
with_attribution_weights as (
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
        -- Linear attribution: equal weight to all sessions
        1.0 / total_sessions as linear_weight,
        
        -- First-touch: 100% to first session
        case when session_index = 1 then 1.0 else 0.0 end as first_touch_weight,
        
        -- Last-touch: 100% to last session  
        case when session_index = total_sessions then 1.0 else 0.0 end as last_touch_weight,
        
        -- 40/20/40 model: 40% first, 40% last, 20% split among middle sessions
        case 
            when total_sessions = 1 then 1.0
            when session_index = 1 then 0.4
            when session_index = total_sessions then 0.4
            else 0.2 / greatest(1, total_sessions - 2)  -- Split 20% among middle sessions
        end as forty_twenty_forty_weight,
        
        -- Time-decay: more recent sessions get more weight
        power(0.7, total_sessions - session_index) / 
        sum(power(0.7, total_sessions - session_index)) over (
            partition by customer_id, converted_at
        ) as time_decay_weight
        
    from sessions_with_sequence
),

-- Calculate revenue attribution for each model
with_points as (
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
        linear_weight,
        first_touch_weight,
        last_touch_weight,
        forty_twenty_forty_weight,
        time_decay_weight,
        -- Revenue attribution based on different models
        revenue * first_touch_weight as first_touch_revenue,
        revenue * last_touch_weight as last_touch_revenue,
        revenue * forty_twenty_forty_weight as forty_twenty_forty_revenue,
        revenue * linear_weight as linear_revenue,
        revenue * time_decay_weight as time_decay_revenue,
        
        -- Points (conversions) attribution - typically use linear for points
        linear_weight as linear_points,
        first_touch_weight as first_touch_points,
        last_touch_weight as last_touch_points,
        forty_twenty_forty_weight as forty_twenty_forty_points,
        time_decay_weight as time_decay_points

    from with_attribution_weights
)

select * from with_points