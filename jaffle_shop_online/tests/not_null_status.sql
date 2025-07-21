{{
    config(
        severity='error',
        meta={'owner': ['Or'], 'description': "Status can't be empty"},
        override_primary_test_model_id='model.jaffle_shop_online.orders'
    )
}}

select * from {{ ref('orders') }} where status is null
