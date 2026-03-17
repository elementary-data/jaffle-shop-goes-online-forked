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

-- Aggregate spend by day, source, medium, and campaign
ad_spend_by_campaign as (

    select
        day,
        utm_source,
        utm_medium,
        utm_campain,

        sum(spend) as total_spend

    from ad_spend
    group by 1, 2, 3, 4

),

-- Aggregate attribution by day, source, medium, and campaign
attribution_by_campaign as (

    select
        sess.started_at::date as day,
        sess.utm_source,
        sess.utm_medium,
        sess.utm_campain,

        count(distinct attr.customer_id) as unique_customers,
        count(distinct attr.order_id) as total_conversions,
        sum(attr.linear_points) as attribution_points,
        sum(attr.linear_revenue) as attribution_revenue,
        sum(attr.first_touch_revenue) as first_touch_revenue,
        sum(attr.last_touch_revenue) as last_touch_revenue,
        sum(attr.time_decay_revenue) as time_decay_revenue

    from attribution attr
    inner join sessions sess
        on sess.session_id = attr.session_id
        and sess.customer_id = attr.customer_id
    group by 1, 2, 3, 4

),

joined as (

    select
        coalesce(attr.day, spend.day) as day,
        coalesce(attr.utm_source, spend.utm_source) as utm_source,
        coalesce(attr.utm_medium, spend.utm_medium) as utm_medium,
        coalesce(attr.utm_campain, spend.utm_campain) as utm_campain,

        -- Spend
        coalesce(spend.total_spend, 0) as total_spend,

        -- Attribution metrics
        coalesce(attr.unique_customers, 0) as unique_customers,
        coalesce(attr.total_conversions, 0) as total_conversions,
        coalesce(attr.attribution_points, 0) as attribution_points,
        coalesce(attr.attribution_revenue, 0) as attribution_revenue,

        -- ROI = (Revenue - Cost) / Cost
        case
            when coalesce(spend.total_spend, 0) > 0
            then (coalesce(attr.attribution_revenue, 0) - spend.total_spend) / spend.total_spend
            else null
        end as roi,

        -- ROAS = Revenue / Cost
        case
            when coalesce(spend.total_spend, 0) > 0
            then 1.0 * coalesce(attr.attribution_revenue, 0) / spend.total_spend
            else null
        end as roas,

        -- CPA = Cost / Conversions
        case
            when coalesce(attr.attribution_points, 0) > 0
            then 1.0 * spend.total_spend / attr.attribution_points
            else null
        end as cost_per_acquisition,

        -- Revenue per conversion
        case
            when coalesce(attr.total_conversions, 0) > 0
            then 1.0 * coalesce(attr.attribution_revenue, 0) / attr.total_conversions
            else null
        end as revenue_per_conversion,

        -- Multi-model revenue for comparison
        coalesce(attr.first_touch_revenue, 0) as first_touch_revenue,
        coalesce(attr.last_touch_revenue, 0) as last_touch_revenue,
        coalesce(attr.time_decay_revenue, 0) as time_decay_revenue

    from attribution_by_campaign attr
    full outer join ad_spend_by_campaign spend
        on attr.day = spend.day
        and attr.utm_source = spend.utm_source
        and attr.utm_medium = spend.utm_medium
        and attr.utm_campain = spend.utm_campain

)

select * from joined
order by day desc, utm_source, utm_medium, utm_campain