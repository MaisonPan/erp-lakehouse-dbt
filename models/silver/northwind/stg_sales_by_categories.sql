{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(CategoryName as string) as category_name,
    cast(ProductSales as double) as product_sales,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_sales_by_categories') }}
where load_date = '{{ batch_date }}'