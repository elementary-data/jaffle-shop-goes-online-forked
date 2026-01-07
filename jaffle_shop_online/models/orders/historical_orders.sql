-- Historical orders from batch processing

with source_orders as (
    select *
    from {{ ref('stg_orders') }}
    where order_date < current_date
),

source_payments as (
    select *
    from {{ ref('stg_payments') }}
),

order_payments as (
    select
        order_id,
        sum(case when payment_method = 'credit_card' then amount else 0 end) as credit_card_amount,
        sum(case when payment_method = 'coupon' then amount else 0 end) as coupon_amount,
        sum(case when payment_method = 'bank_transfer' then amount else 0 end) as bank_transfer_amount,
        sum(case when payment_method = 'gift_card' then amount else 0 end) as gift_card_amount,
        sum(amount) as total_amount
    from source_payments
    group by 1
),

final as (
    select
        source_orders.order_id,
        source_orders.customer_id,
        source_orders.order_date,
        source_orders.status,
        -- Convert to cents for historical data
        (order_payments.total_amount * 100)::integer as amount,
        (order_payments.credit_card_amount * 100)::integer as credit_card_amount,
        (order_payments.coupon_amount * 100)::integer as coupon_amount,
        (order_payments.bank_transfer_amount * 100)::integer as bank_transfer_amount,
        (order_payments.gift_card_amount * 100)::integer as gift_card_amount
    from source_orders
    left join order_payments using (order_id)
)

select * from final