-- Validates that the amount in real_time_orders does not exceed
-- the maximum price found in raw_products.
select *
from {{ ref('real_time_orders') }}
where amount > (select max(price) from {{ ref('raw_products') }})
