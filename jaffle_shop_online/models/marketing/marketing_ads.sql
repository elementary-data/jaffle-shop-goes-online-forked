{{
    config(
        materialized = "incremental",
        unique_key = ["ad_id", "utm_source", "date"],
    )
}}

{% set sources = [
    ('google', 'stg_google_ads'),
    ('facebook', 'stg_facebook_ads'),
    ('instagram', 'stg_instagram_ads')
] %}

{% set common_columns = [
    'ad_id',
    'date',
    'campaign_name',
    'ad_group_name',
    'ad_name',
    'clicks',
    'impressions',
    'spend'
] %}

{% if is_incremental() %}
    {% set max_date = "select max(date) from " ~ this %}
{% endif %}

{% for utm_source, source_name in sources %}
    select
        {% for column in common_columns %}
        {{ column }},
        {% endfor %}
        '{{ utm_source }}' as utm_source
    from {{ source("ads", source_name) }}
    {% if is_incremental() %}
    where date > ({{ max_date }})
    {% endif %}

    {% if not loop.last %}
    union all
    {% endif %}
{% endfor %}