-- All monetary amounts in this model are in dollars

with orders as (
    select * from ELEMENTARY_TESTS.mika_jaffle_shop_online.stg_orders
),

payments as (
    select * from ELEMENTARY_TESTS.mika_jaffle_shop_online.stg_payments
),

order_payments as (
    select
        order_id,
        sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,
        sum(amount) as total_amount
    from payments
    group by order_id
),

final as (
    select
        o.order_id,
        o.customer_id,
        o.order_date,
        o.status,
        op.credit_card_amount,
        op.coupon_amount,
        op.bank_transfer_amount,
        op.gift_card_amount,
        op.total_amount    as amount_cents
    from orders o
    left join order_payments op on o.order_id = op.order_id
)

select 
    order_id,
    customer_id,
    order_date,
    status,
    (amount_cents / 100)::numeric(16, 2) as amount,
    (bank_transfer_amount / 100)::numeric(16, 2) as bank_transfer_amount,
    (coupon_amount / 100)::numeric(16, 2) as coupon_amount,
    (credit_card_amount / 100)::numeric(16, 2) as credit_card_amount,
    (gift_card_amount / 100)::numeric(16, 2) as gift_card_amount
from final
where date(order_date) = (
    select date(max(order_date)) from final
)