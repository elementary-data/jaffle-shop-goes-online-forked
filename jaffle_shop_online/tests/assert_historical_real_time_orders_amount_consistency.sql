-- Compare the average amount between historical and real-time orders
-- to ensure they are within a reasonable range of each other
with historical_avg as (
    select avg(amount) as avg_amount
    from {{ ref('historical_orders') }}
    where date(order_date) = (
        select date(max(order_date)) - interval 1 day
        from {{ ref('historical_orders') }}
    )
),
real_time_avg as (
    select avg(amount) as avg_amount
    from {{ ref('real_time_orders') }}
)
select
    case
        when abs(historical_avg.avg_amount - real_time_avg.avg_amount) / greatest(historical_avg.avg_amount, real_time_avg.avg_amount) > 0.1
        then 'Average amounts differ by more than 10%'
        else 'OK'
    end as amount_consistency_check
from historical_avg, real_time_avg
having amount_consistency_check != 'OK'
