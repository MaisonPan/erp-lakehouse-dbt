{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    CategoryName    as category_name,
    ProductName     as product_name,
    QuantityPerUnit as Quantity_Per_Unit,
    UnitsInStock    as Units_In_Stock,

    load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_products_by_categories') }}
where load_date = '{{ batch_date }}'