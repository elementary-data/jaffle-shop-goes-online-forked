-- This table contains all orders that have been returned

select *
from {{ ref('orders') }}
where status in ('return_pending', 'returned')
