{{
    config(
        meta={'description': 'To check customer_is is less than 5000', 'quality_dimension': 'accuracy'},
        severity='warn'
    )
}}

select * from {{ ref('customers') }} 
where customer_id > 5000
