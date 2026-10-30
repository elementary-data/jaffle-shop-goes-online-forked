-- depends_on: {{ ref('raw_signups_validation') }}

{% if elementary.get_config_var('validation') %}
    with source as (
        select * from {{ ref('raw_signups_validation') }}
    ),

{% else %}
    with source as (
        select * from {{ ref('raw_signups_training') }}
    ),
{% endif %}

renamed as (

    select
        id as signup_id,
        user_id as customer_id,
        user_email as customer_email,
        hashed_password,
        signup_date

    from source

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by customer_email
            order by signup_date desc, signup_id desc
        ) as row_num

    from renamed

)

select
    signup_id,
    customer_id,
    customer_email,
    hashed_password,
    signup_date

from deduplicated
where row_num = 1
