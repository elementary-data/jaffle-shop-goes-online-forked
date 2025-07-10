{{
    config(
        severity='warn'
    )
}}

select * from {{ ref('orders') }}
join {{ ref('customers') }} using (customer_id)
where status = 'shipped'
and amount = 0
