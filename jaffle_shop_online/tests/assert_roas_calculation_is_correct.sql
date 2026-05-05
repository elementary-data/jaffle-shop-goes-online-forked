{{
    config(
        severity='error',
        meta={'description': 'Validates ROAS = (attribution_revenue / total_spend) * 100 when both inputs are > 0.'},
        tags=['finance', 'data_quality']
    )
}}

-- Validates that return_on_advertising_spend equals (attribution_revenue / total_spend) * 100
-- when both attribution_revenue > 0 and total_spend > 0.
-- A small tolerance avoids floating-point false positives.

with validation as (

    select
        day,
        utm_source,
        total_spend,
        attribution_revenue,
        return_on_advertising_spend as actual_roas,
        (100.0 * attribution_revenue / total_spend) as expected_roas
    from {{ ref('cpa_and_roas') }}
    where total_spend > 0
      and attribution_revenue > 0

)

select *
from validation
where return_on_advertising_spend is null
   or abs(actual_roas - expected_roas) > 0.0001

