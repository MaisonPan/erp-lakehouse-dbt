{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(OrderID as int)       as order_id,
    cast(ProductID as int)     as product_id,
    cast(ProductName as string) as product_name,

    cast(UnitPrice as double)  as unit_price,
    cast(Quantity as int)      as quantity,
    cast(Discount as double)   as discount,

    cast(UnitPrice as double)
        * cast(Quantity as int)
        * (1 - cast(Discount as double))  as extended_price,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_order_details_extendeds') }}
where load_date = '{{ batch_date }}'