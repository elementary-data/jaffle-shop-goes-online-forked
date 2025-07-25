{% macro cents_to_dollars(column_name) -%}
    {{ return(adapter.dispatch('cents_to_dollars')(column_name)) }}
{%- endmacro %}

{% macro default__cents_to_dollars(column_name) -%}
    ({{ column_name }} / 100)::numeric(16, 2)
{%- endmacro %}

{% macro postgres__cents_to_dollars(column_name) -%}
    ({{ column_name }}::numeric(16, 2) / 100)
{%- endmacro %}

{% macro bigquery__cents_to_dollars(column_name) %}
    round(cast(({{ column_name }} / 100) as numeric), 2)
{% endmacro %}

{% macro fabric__cents_to_dollars(column_name) %}
    cast({{ column_name }} / 10 as numeric(16,2))
{% endmacro %}
