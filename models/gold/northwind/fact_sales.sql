{{ config(materialized='table') }}

with od as (
  select
    order_id,
    product_id,
    unit_price,
    quantity,
    discount,
    line_amount,
    load_date
  from {{ ref('stg_order_details') }}
),

o as (
  select
    order_id,
    customer_id,
    employee_id,
    ship_via as shipper_id,
    order_date,
    required_date,
    shipped_date,
    freight,
    ship_country,
    ship_city,
    load_date
  from {{ ref('stg_orders') }}
)

select
  -- grain
  od.order_id,
  od.product_id,

  -- dims keys
  o.customer_id,
  o.employee_id,
  o.shipper_id,

  -- dates
  o.order_date,
  o.required_date,
  o.shipped_date,

  -- measures
  od.unit_price,
  od.quantity,
  od.discount,
  od.line_amount,
  o.freight,

  -- shipping attrs (useful for analysis)
  o.ship_country,
  o.ship_city,

  -- audit
  o.load_date as load_date,
  current_timestamp() as processed_at

from od
join o
  on od.order_id = o.order_id