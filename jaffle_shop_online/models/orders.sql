
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
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        order_payments.credit_card_amount,
        order_payments.coupon_amount,
        order_payments.bank_transfer_amount,
        order_payments.gift_card_amount,
        order_payments.total_amount as amount
    from orders
    left join order_payments
        on orders.order_id = order_payments.order_id
),

past_orders as (
    select
        order_id,
        customer_id,
        order_date,
        status,
        amount,
        bank_transfer_amount,
        coupon_amount,
        credit_card_amount,
        gift_card_amount
    from final
    where date(order_date) < (select date(max(order_date)) from final)
),

real_time_orders as (
    select 
        order_id,
        customer_id,
        order_date,
        status,
        amount,  -- Removed division by 100
        bank_transfer_amount,
        coupon_amount,
        credit_card_amount,
        gift_card_amount
    from final
    where date(order_date) = (select date(max(order_date)) from final)
)

select * from past_orders
union all
select * from real_time_orders
