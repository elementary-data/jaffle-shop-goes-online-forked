{{
    config(
        meta={'owner': ['elon@jaffle.com'], 'description': 'test', 'quality_dimension': 'accuracy'},
        severity='warn',
        tags=['line_items'],
        override_primary_test_model_id='model.jaffle_shop_online.cpa_and_roas'
    )
}}

select * from {{ ref('cpa_and_roas') }}
