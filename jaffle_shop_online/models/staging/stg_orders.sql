-- depends_on: {{ ref('raw_orders_validation') }}

{% if elementary.get_config_var('validation') %}
    with source as (
        select * from {{ ref('raw_orders_validation') }}
    ),

{% else %}
    with source as (
        select * from {{ ref('raw_orders_training') }}
    ),
{% endif %}

renamed as (

    select
        id as order_id,
        user_id as customer_id,
        try_cast(order_date as timestamp) as order_date,  -- Added cast to timestamp
        status

    from source

)

select * from renamed
