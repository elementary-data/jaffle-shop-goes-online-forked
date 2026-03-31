{{
    config(
        severity='error',
        tags=['finance', 'marketing'],
        meta={'description': 'Validates that when attribution_revenue is greater than 0, the utm_source column contains the letter A.'}
    )
}}

SELECT *
FROM {{ ref('cpa_and_roas') }}
WHERE attribution_revenue > 0
  AND utm_source NOT LIKE '%A%'
