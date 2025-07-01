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

{% set columns = [
    "ad_id",
    "date",
    "campaign_name",
    "ad_group_name",
    "ad_name",
    "clicks",
    "impressions",
    "spend"
] %}

{% if is_incremental() %}
    {% set max_date_query %}
        select max(date) as max_date from {{ this }}
    {% endset %}
    {% set max_date = run_query(max_date_query).columns[0][0] %}
{% endif %}

{% for source_name, table, utm_source in sources %}
    select
        {{ columns | join(', ') }},
        '{{ utm_source }}' as utm_source
    from {{ source(source_name, table) }}
    {% if is_incremental() %}
    where date > '{{ max_date }}'
    {% endif %}
    {% if not loop.last %}union all{% endif %}
{% endfor %}