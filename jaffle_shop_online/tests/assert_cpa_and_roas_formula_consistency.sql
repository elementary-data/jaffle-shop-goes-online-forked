-- Validates that the CPA and ROAS metrics in cpa_and_roas match their
-- documented formula definitions, and that they are null exactly when the
-- inputs are not both positive.
--
-- Data quality dimension: accuracy
-- Scoped to the last 24 hours via the `day` partition for efficient,
-- partition-pruned execution.

with validation as (

    select
        day,
        utm_source,
        total_spend,
        attribution_points,
        attribution_revenue,
        cost_per_acquisition,
        return_on_advertising_spend
    from {{ ref('cpa_and_roas') }}
    where day >= dateadd(day, -1, current_date)

)

select *
from validation
where
    -- CPA must equal total_spend / attribution_points when both are positive
    ( total_spend > 0 and attribution_points > 0
      and abs(cost_per_acquisition - (1.0 * total_spend / attribution_points)) > 0.01 )

    -- ROAS must equal 100 * attribution_revenue / total_spend (percentage units)
    or ( total_spend > 0 and attribution_revenue > 0
         and abs(return_on_advertising_spend - (100.0 * attribution_revenue / total_spend)) > 0.01 )

    -- CPA must be null when inputs are not both positive
    or ( not (total_spend > 0 and attribution_points > 0) and cost_per_acquisition is not null )

    -- ROAS must be null when inputs are not both positive
    or ( not (total_spend > 0 and attribution_revenue > 0) and return_on_advertising_spend is not null )
