{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(ProductID as int)      as product_id,
    cast(ProductName as string) as product_name,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_current_product_lists') }}
where load_date = '{{ batch_date }}'