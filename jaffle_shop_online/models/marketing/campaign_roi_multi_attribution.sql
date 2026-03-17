{{
    config(
        materialized = "table",
    )
}}

with ad_spend as (

    select * from {{ ref('ads_spend') }}

),

attribution as (

    select * from {{ ref('attribution_touches') }}

),

sessions as (

    select * from {{ ref('sessions') }}

),

-- Aggregate daily spend by source
ad_spend_aggregated as (

    select
        day,
        utm_source,
        sum(spend) as total_spend

    from ad_spend
    group by 1, 2

),

-- Aggregate attribution metrics across all models
attribution_aggregated as (

    select
        sess.started_at::date as day,
        sess.utm_source,

        -- Linear attribution
        sum(attr.linear_points) as linear_points,
        sum(attr.linear_revenue) as linear_revenue,

        -- First-touch attribution
        sum(attr.first_touch_points) as first_touch_points,
        sum(attr.first_touch_revenue) as first_touch_revenue,

        -- Last-touch attribution
        sum(attr.last_touch_points) as last_touch_points,
        sum(attr.last_touch_revenue) as last_touch_revenue,

        -- 40/20/40 attribution
        sum(attr.forty_twenty_forty_points) as forty_twenty_forty_points,
        sum(attr.forty_twenty_forty_revenue) as forty_twenty_forty_revenue,

        -- Time-decay attribution
        sum(attr.time_decay_points) as time_decay_points,
        sum(attr.time_decay_revenue) as time_decay_revenue

    from attribution attr
    inner join sessions sess
        on sess.session_id = attr.session_id
        and sess.customer_id = attr.customer_id

    group by 1, 2

),

joined as (

    select
        coalesce(attr_agg.day, spend_agg.day) as day,
        coalesce(attr_agg.utm_source, spend_agg.utm_source) as utm_source,
        coalesce(spend_agg.total_spend, 0) as total_spend,

        -- Raw attribution metrics
        coalesce(attr_agg.linear_points, 0) as linear_points,
        coalesce(attr_agg.linear_revenue, 0) as linear_revenue,
        coalesce(attr_agg.first_touch_points, 0) as first_touch_points,
        coalesce(attr_agg.first_touch_revenue, 0) as first_touch_revenue,
        coalesce(attr_agg.last_touch_points, 0) as last_touch_points,
        coalesce(attr_agg.last_touch_revenue, 0) as last_touch_revenue,
        coalesce(attr_agg.forty_twenty_forty_points, 0) as forty_twenty_forty_points,
        coalesce(attr_agg.forty_twenty_forty_revenue, 0) as forty_twenty_forty_revenue,
        coalesce(attr_agg.time_decay_points, 0) as time_decay_points,
        coalesce(attr_agg.time_decay_revenue, 0) as time_decay_revenue,

        -- ROAS by attribution model
        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.linear_revenue, 0) > 0
            then 1.0 * attr_agg.linear_revenue / spend_agg.total_spend end as linear_roas,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.first_touch_revenue, 0) > 0
            then 1.0 * attr_agg.first_touch_revenue / spend_agg.total_spend end as first_touch_roas,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.last_touch_revenue, 0) > 0
            then 1.0 * attr_agg.last_touch_revenue / spend_agg.total_spend end as last_touch_roas,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.forty_twenty_forty_revenue, 0) > 0
            then 1.0 * attr_agg.forty_twenty_forty_revenue / spend_agg.total_spend end as forty_twenty_forty_roas,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.time_decay_revenue, 0) > 0
            then 1.0 * attr_agg.time_decay_revenue / spend_agg.total_spend end as time_decay_roas,

        -- CPA by attribution model
        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.linear_points, 0) > 0
            then 1.0 * spend_agg.total_spend / attr_agg.linear_points end as linear_cpa,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.first_touch_points, 0) > 0
            then 1.0 * spend_agg.total_spend / attr_agg.first_touch_points end as first_touch_cpa,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.last_touch_points, 0) > 0
            then 1.0 * spend_agg.total_spend / attr_agg.last_touch_points end as last_touch_cpa,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.forty_twenty_forty_points, 0) > 0
            then 1.0 * spend_agg.total_spend / attr_agg.forty_twenty_forty_points end as forty_twenty_forty_cpa,

        case when coalesce(spend_agg.total_spend, 0) > 0 and coalesce(attr_agg.time_decay_points, 0) > 0
            then 1.0 * spend_agg.total_spend / attr_agg.time_decay_points end as time_decay_cpa

    from attribution_aggregated attr_agg
    full outer join ad_spend_aggregated spend_agg
        on attr_agg.day = spend_agg.day
        and attr_agg.utm_source = spend_agg.utm_source

)

select * from joined
order by day, utm_source
