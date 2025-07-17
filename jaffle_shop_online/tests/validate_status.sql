{{
    config(
        tags=['elementary-tests'],
        meta={'owner': ['Or'], 'description': 'Validating the status of the orders table', 'quality_dimension': 'accuracy'},
        severity='error'
    )
}}

select * from {{ ref('orders') }} where status is not null
