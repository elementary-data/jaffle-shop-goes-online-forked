-- All monetary amounts in this model are normalized to dollars
{% raw %}{{{% endraw %}
  config(materialized='view')
{% raw %}}}{% endraw %}

{% raw %}{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}{% endraw %}

with orders as (
    select * from {% raw %}{{ ref('stg_orders') }}{% endraw %}
),

payments as (
    select * from {% raw %}{{ ref('stg_payments') }}{% endraw %}
),

order_payments as (
    select
        order_id,
        {% raw %}{% for payment_method in payment_methods -%}{% endraw %}
        sum(case when payment_method = '{% raw %}{{ payment_method }}{% endraw %}' then amount else 0 end) as {% raw %}{{ payment_method }}{% endraw %}_amount,
        {% raw %}{% endfor -%}{% endraw %}
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
        {% raw %}{% for payment_method in payment_methods -%}{% endraw %}
        op.{% raw %}{{ payment_method }}{% endraw %}_amount,
        {% raw %}{% endfor -%}{% endraw %}
        op.total_amount    as amount_cents
    from orders o
    left join order_payments op on o.order_id = op.order_id
)

select 
    order_id,
    customer_id,
    order_date,
    status,
    {% raw %}{{ cents_to_dollars('amount_cents') }}{% endraw %} as amount,
    {% raw %}{{ cents_to_dollars('bank_transfer_amount') }}{% endraw %} as bank_transfer_amount,
    {% raw %}{{ cents_to_dollars('coupon_amount') }}{% endraw %} as coupon_amount,
    {% raw %}{{ cents_to_dollars('credit_card_amount') }}{% endraw %} as credit_card_amount,
    {% raw %}{{ cents_to_dollars('gift_card_amount') }}{% endraw %} as gift_card_amount
from final
where date(order_date) = (
    select date(max(order_date)) from final
) 