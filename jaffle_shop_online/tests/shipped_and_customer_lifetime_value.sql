{{
    config(
        meta={'owner': ['@finance-team'], 'description': "This test validates that orders with status = 'shipped' don't have a customer_lifetime_value of 0", 'quality_dimension': 'accuracy'},
        severity='warn',
        override_primary_test_model_id='model.jaffle_shop_online.orders',
        tags=['finance']
    )
}}

select * from {{ ref('orders') }}
join {{ ref('customers') }} using (customer_id)
where status = 'shipped'
and customer_lifetime_value = 0
