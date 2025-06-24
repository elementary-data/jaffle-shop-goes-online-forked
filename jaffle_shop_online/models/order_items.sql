-- depends_on: {{ ref('orders') }}

select * from {{ ref('orders') }}
