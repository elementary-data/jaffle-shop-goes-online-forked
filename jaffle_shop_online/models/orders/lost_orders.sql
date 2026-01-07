-- This table contains orders that are lost (shipped but not delivered)

select *
from {{ ref('orders') }}
where status in ('shipped', 'return_pending')
