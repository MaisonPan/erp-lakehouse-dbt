{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(CategoryName as string)  as category_name,
    cast(CategorySales as double) as category_sales,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_category_sales_for_1997') }}
where load_date = '{{ batch_date }}'