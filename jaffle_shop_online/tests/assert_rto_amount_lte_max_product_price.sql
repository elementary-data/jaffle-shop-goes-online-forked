-- Validates that no real-time order amount exceeds the max price from raw_products.
-- This guards the ROAS calculation in cpa_and_roas from inflated order values.

select
    order_id,
    amount
from {{ ref('real_time_orders') }}
where amount > (
    select max(price) from {{ source('products', 'raw_products') }}
)
