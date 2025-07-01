
-- Union of historical and real-time datasets
select 
    *,
    amount_cents / 100 as amount
from ELEMENTARY_TESTS.mika_jaffle_shop_online.historical_orders

union all

select 
    *,
    amount_cents / 100 as amount
from ELEMENTARY_TESTS.mika_jaffle_shop_online.real_time_orders