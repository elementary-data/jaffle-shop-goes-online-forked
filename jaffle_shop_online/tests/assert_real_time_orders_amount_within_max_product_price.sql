{{
    config(
        meta={'description': 'Validates that the amount field in real_time_orders does not exceed the maximum price from raw_products'},
        tags=['finance', 'real_time', 'sales'],
        severity='error'
    )
}}

select
    order_id,
    amount
from {{ ref('real_time_orders') }}
where amount > (select max(price) from {{ ref('raw_products') }})
