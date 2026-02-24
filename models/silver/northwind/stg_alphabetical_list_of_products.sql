{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(ProductID as int)        as product_id,
    cast(ProductName as string)   as product_name,
    cast(CategoryName as string)  as category_name,
    cast(UnitPrice as double)     as unit_price,

    cast(load_date as date)       as load_date,
    source_file,
    current_timestamp()           as processed_at

from {{ ref('bronze_alphabetical_list_of_products') }}
where load_date = '{{ batch_date }}'