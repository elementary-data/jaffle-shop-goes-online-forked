{{
    config(
        meta={'quality_dimension': 'accuracy'},
        severity='warn'
    )
}}

select * from {{ ref('orders') }}
join {{ ref('customers') }} using (customer_id)
where status = 'shipped'
and customer_lifetime_value = 0
