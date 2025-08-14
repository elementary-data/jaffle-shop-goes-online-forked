{{  config(materialized='view') }}

{% raw %}
-- All monetary amounts in this model are converted from cents to dollars
{% endraw %}

{% raw %}
{% set payment_methods = ['credit_card', 'coupon', 'bank_transfer', 'gift_card'] %}
{% endraw %}

with orders as (
    select * from {% raw %}{{ ref('stg_orders') }}{% endraw %}
),

payments as (
    select * from {% raw %}{{ ref('stg_payments') }}{% endraw %}
),

order_payments as (
    select
        order_id,
        {% raw %}
        {% for payment_method in payment_methods -%}
        sum(case when payment_method = '{{ payment_method }}' then amount else 0 end) as {{ payment_method }}_amount,
        {% endfor -%}
        {% endraw %}
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
        {% raw %}
        {% for payment_method in payment_methods -%}
        {{ cents_to_dollars('op.' + payment_method + '_amount') }} as {{ payment_method }}_amount,
        {% endfor -%}
        {% endraw %}
        {% raw %}{{ cents_to_dollars('op.total_amount') }}{% endraw %} as amount  -- Amount is now in dollars
    from orders o
    left join order_payments op on o.order_id = op.order_id
)

select *
from final
where date(order_date) < (
    select date(max(order_date))
    from final
) 