{{ config(materialized='table') }}

with inv as (

    select *
    from {{ ref('stg_invoices') }}

)

select
    order_id,
    customer_id,
    product_id,

    shippedDate as shipped_date,

    quantity    as invoiced_qty,
    unit_price  as invoiced_unit_price,
    discount    as invoiced_discount,

    freight,
    unit_price
        * quantity
        * (1 - discount) as invoiced_amount,

    cast(load_date as date)   as load_date,
    current_timestamp()       as processed_at

from inv