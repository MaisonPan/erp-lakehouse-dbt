{{ config(materialized='table') }}

select
  order_id,
  customer_id,
  employee_id,
  ship_via as shipper_id,
  order_date,
  required_date,
  shipped_date,

  -- order-level measures
  freight,

  -- shipping attributes
  ship_country,
  ship_city,

  load_date,
  current_timestamp() as processed_at

from {{ ref('stg_orders') }}