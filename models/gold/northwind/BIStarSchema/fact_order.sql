{{ config(materialized='table') }}

with op as (
  select
    order_id,
    product_id,
    sum(quantity) as items_qty,
    sum(line_amount) as net_amount,
    sum(unit_price * quantity) as gross_amount,
    sum(unit_price * quantity * discount) as discount_amount,
    max(load_date) as load_date
  from {{ ref('stg_order_details') }}
  group by order_id, product_id
),

o as (
  select
    order_id,
    customer_id,
    employee_id,
    ship_via as shipper_id,
    order_date,
    shipped_date,
    load_date
  from {{ ref('stg_orders') }}
)

select
  op.order_id,
  op.product_id,

  o.customer_id,
  o.employee_id,
  o.shipper_id,
  o.order_date,
  o.shipped_date,

  op.items_qty,
  op.net_amount,
  op.gross_amount,
  op.discount_amount,

  op.load_date,
  current_timestamp() as processed_at

from op join o on op.order_id = o.order_id