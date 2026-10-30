{{
  config(
    materialized = 'view',
    )
}}

with orders as (
    select *
    from {{ ref('orders') }}
)

select 
  order_id,
  customer_id,
  order_date,
  status,
  amount,
  credit_card_amount,
  coupon_amount,
  bank_transfer_amount,
  gift_card_amount
from orders
where status = 'return_pending' or status = 'shipped'
