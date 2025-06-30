-- tests/assert_historical_realtime_orders_consistent.sql
with historical_orders as (
    select * from {{ ref('historical_orders') }}
    where date(order_date) = (
        select date(max(order_date)) - interval 1 day
        from {{ ref('historical_orders') }}
    )
),

real_time_orders as (
    select * from {{ ref('real_time_orders') }}
    where date(order_date) = (
        select date(max(order_date)) - interval 1 day
        from {{ ref('real_time_orders') }}
    )
),

compare_amounts as (
    select
        'historical' as source,
        avg(amount) as avg_amount,
        count(*) as order_count
    from historical_orders

    union all

    select
        'real_time' as source,
        avg(amount) as avg_amount,
        count(*) as order_count
    from real_time_orders
)

select *
from compare_amounts
having count(distinct avg_amount) > 1 or count(distinct order_count) > 1