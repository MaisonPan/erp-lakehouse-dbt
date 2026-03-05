{{ config(materialized='table') }}

select
  p.product_id,
  p.product_name,
  c.category_name,
  p.quantity_per_unit,
  p.unit_price,
  p.units_in_stock,
  p.units_on_order,
  p.reorder_level,
  p.discontinued
from {{ ref('stg_products') }} p
left join {{ ref('stg_categories') }} c
  on p.category_id = c.category_id