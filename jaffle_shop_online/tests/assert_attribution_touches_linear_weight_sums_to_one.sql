{{
    config(
        meta={'description': 'Validates that linear attribution weights sum to 1.0 per conversion (customer_id, converted_at) in attribution_touches. quality_dimension: consistency'},
        tags=['marketing', 'consistency'],
        severity='warn'
    )
}}

select customer_id, converted_at, sum(linear_weight) as total_weight
from {{ ref('attribution_touches') }}
group by 1, 2
having abs(sum(linear_weight) - 1.0) > 0.001
