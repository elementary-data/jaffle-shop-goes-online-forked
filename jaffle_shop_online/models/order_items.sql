-- depends_on: {{ ref('orders') }}

{% if 'fromm' in sql %}
  {{ exceptions.raise_compiler_error("Detected 'fromm' in SQL, potential syntax error.") }}
{% endif %}

select * from {{ ref('orders') }}