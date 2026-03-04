{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    ProductName as product_name,
    UnitPrice   as unit_price,
    
    load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_products_above_average_prices') }}
where load_date = '{{ batch_date }}'