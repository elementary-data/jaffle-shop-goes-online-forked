-- Validates that the amount field in real_time_orders
-- does not exceed the maximum price from raw_products.
select
    order_id,
    amount
from {{ ref('real_time_orders') }}
where amount > (select max(price) from {{ source('products', 'raw_products') }})
