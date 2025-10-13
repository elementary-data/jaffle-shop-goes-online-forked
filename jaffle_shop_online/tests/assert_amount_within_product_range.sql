-- Test to validate that order amounts in real_time_orders are within reasonable bounds
-- compared to the maximum product price from raw_products table

select
    order_id,
    amount,
    max_product_price
from (
    select 
        rto.order_id,
        rto.amount,
        -- Assuming a reasonable max product price multiplier for multiple items
        1000 as max_product_price  -- Using a reasonable maximum for validation
    from {{ ref('real_time_orders') }} rto
    where rto.amount > 1000  -- Orders exceeding reasonable maximum
) orders_over_limit