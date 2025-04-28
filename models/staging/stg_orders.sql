SELECT
  id AS order_id,
  user_id AS customer_id,
  order_date,
  status
FROM {{ source('jaffle_shop', 'raw_orders_validation') }}
WHERE id IS NOT NULL  -- Add this line to filter out null IDs