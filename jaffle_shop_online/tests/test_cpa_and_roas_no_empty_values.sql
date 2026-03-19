{{
    config(
        meta={'description': 'Validates that attribution_revenue and utm_source columns in cpa_and_roas never contain NULL values'},
        tags=['finance', 'marketing'],
        severity='error'
    )
}}

-- This test validates that attribution_revenue and utm_source in cpa_and_roas contain no empty (NULL) values.

SELECT *
FROM {{ ref('cpa_and_roas') }}
WHERE
    attribution_revenue IS NULL
    OR utm_source IS NULL
