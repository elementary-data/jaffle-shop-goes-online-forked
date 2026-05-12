{{
    config(
        severity='error',
        tags=['finance', 'data_quality'],
        meta={'description': 'Validates ROAS = 100 * attribution_revenue / total_spend (matches model logic) when both values are positive. Partitioned on day for efficiency, scoped to last 24 hours.'}
    )
}}

{{ config(severity = 'error') }}

select
    day,
    utm_source,
    attribution_revenue,
    total_spend,
    return_on_advertising_spend,
    (100.0 * attribution_revenue / total_spend) as expected_roas
from {{ ref('cpa_and_roas') }}
where day >= dateadd(hour, -24, current_timestamp)
  and attribution_revenue > 0
  and total_spend > 0
  and abs(return_on_advertising_spend - (100.0 * attribution_revenue / total_spend)) > 0.0001
