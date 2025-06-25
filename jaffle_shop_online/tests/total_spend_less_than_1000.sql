{{
    config(
        severity='warn'
    )
}}

select * from {{ ref('cpa_and_roas') }}
where total_spend > 1000
