{{
    config(
        severity='error',
        meta={'description': 'Validates that cost_per_acquisition and return_on_advertising_spend stored in cpa_and_roas match their derived formulas (total_spend / attribution_points and attribution_revenue / total_spend) within a 0.01 tolerance.'},
        tags=['business-rule']
    )
}}

select
    day,
    utm_source,
    cost_per_acquisition,
    return_on_advertising_spend,
    total_spend,
    attribution_points,
    attribution_revenue
from {{ ref('cpa_and_roas') }}
where
    (
        attribution_points > 0
        and abs(cost_per_acquisition - (total_spend / attribution_points)) > 0.01
    )
    or (
        total_spend > 0
        and abs(return_on_advertising_spend - (attribution_revenue / total_spend)) > 0.01
    )
