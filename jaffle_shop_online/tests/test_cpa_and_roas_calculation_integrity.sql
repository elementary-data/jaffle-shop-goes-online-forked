{{
    config(
        severity='error',
        meta={'description': 'Validates that CPA and ROAS calculations in cpa_and_roas are mathematically consistent with their input columns.'},
        tags=['finance', 'finance-data-product']
    )
}}

-- Test: Verify CPA and ROAS calculations in cpa_and_roas
-- Returns rows where the calculated metrics don't match the expected formulas

select
    day,
    utm_source,
    total_spend,
    attribution_points,
    attribution_revenue,
    cost_per_acquisition,
    return_on_advertising_spend

from {{ ref('cpa_and_roas') }}

where
    -- Case 1: CPA should be total_spend / attribution_points when both > 0
    (
        total_spend > 0 and attribution_points > 0
        and abs(cost_per_acquisition - (1.0 * total_spend / attribution_points)) > 0.01
    )
    or
    -- Case 2: CPA should be null when spend or points are 0
    (
        (total_spend = 0 or attribution_points = 0)
        and cost_per_acquisition is not null
    )
    or
    -- Case 3: ROAS should be null when spend or revenue are 0
    (
        (total_spend = 0 or attribution_revenue = 0)
        and return_on_advertising_spend is not null
    )
