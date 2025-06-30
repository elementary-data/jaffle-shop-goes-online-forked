{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

{% set sources = [
    ("ads", "stg_google_ads", "google"),
    ("ads", "stg_facebook_ads", "facebook"),
    ("ads", "stg_instagram_ads", "instagram")
] %}

{% for source_name, table, utm_source in sources %}
    select
        ad_id,
        date,
        campaign_name,
        ad_group_name,
        clicks,
        impressions,
        spend,
        '{{ utm_source }}' as utm_source
    from {{ source(source_name, table) }}
    {% if not loop.last %}union all{% endif %}
    {% if is_incremental() %}
    where date > (select max(date) from {{ this }})
    {% endif %}
{% endfor %}