{{
    config(
        severity='error',
        meta={'description': 'Validates that total_spend in cpa_and_roas matches the aggregated spend from ads_spend for each day and utm_source combination'}
    )
}}

-- Validates that total_spend in cpa_and_roas matches the aggregated spend
-- from ads_spend for each (day, utm_source) combination.

WITH
spend_by_source_day AS (
    SELECT
        day,
        utm_source,
        sum(spend) AS total_spend_from_ads
    FROM
        {{ ref('ads_spend') }}
    GROUP BY
        day,
        utm_source
),
validation AS (
    SELECT
        cr.day,
        cr.utm_source,
        cr.total_spend,
        s.total_spend_from_ads,
        cr.total_spend - coalesce(s.total_spend_from_ads, 0) AS spend_difference
    FROM
        {{ ref('cpa_and_roas') }} cr
        LEFT JOIN spend_by_source_day s ON cr.day = s.day
        AND cr.utm_source = s.utm_source
)
SELECT
    *
FROM
    validation
WHERE
    spend_difference != 0
