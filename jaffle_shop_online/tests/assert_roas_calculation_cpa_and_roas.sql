{{
    config(
        tags=['marketing', 'finance', 'data_quality'],
        meta={'description': 'Verifies that the return_on_advertising_spend column equals attribution_revenue / total_spend for rows where both values are greater than 0. Limited to the last 24 hours.'},
        severity='warn'
    )
}}

-- Validates that ROAS = attribution_revenue / total_spend when both > 0
select
    day,
    utm_source,
    total_spend,
    attribution_revenue,
    return_on_advertising_spend,
    (1.0 * attribution_revenue / total_spend) as expected_roas
from {{ ref('cpa_and_roas') }}
where total_spend > 0
  and attribution_revenue > 0
  and day >= dateadd('hour', -24, current_timestamp)
  and abs(return_on_advertising_spend - (1.0 * attribution_revenue / total_spend)) > 0.0001
