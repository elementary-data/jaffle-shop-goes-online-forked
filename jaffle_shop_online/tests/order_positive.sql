{{
    config(
        severity='error'
    )
}}

select * from {{ ref('customers') }}
where number_of_orders > 0
