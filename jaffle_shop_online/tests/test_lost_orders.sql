-- Test for not null columns
select *
from {{ ref('lost_orders') }}
where order_id is null
   or customer_id is null
   or amount is null

union all

-- Test for unique order_id
select order_id
from {{ ref('lost_orders') }}
group by order_id
having count(*) > 1

union all

-- Test for positive amount
select *
from {{ ref('lost_orders') }}
where amount <= 0

union all

-- Test for date range (assuming order_date and lost_date columns exist)
select *
from {{ ref('lost_orders') }}
where order_date > current_date
   or lost_date > current_date
   or order_date < '2020-01-01'  -- Adjust this date as needed

union all

-- Test for referential integrity with customers table
select lo.customer_id
from {{ ref('lost_orders') }} lo
left join {{ ref('customers') }} c on lo.customer_id = c.customer_id
where c.customer_id is null

union all

-- Test for status transition (assuming status column exists)
select *
from {{ ref('lost_orders') }}
where status = 'shipped' and lost_date < order_date

-- Note: Volume anomaly and freshness tests are typically handled by Elementary's automated tests
-- and don't need to be explicitly defined here.