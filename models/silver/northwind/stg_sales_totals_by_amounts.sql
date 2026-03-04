{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(OrderID as int) as order_id,
    cast(SaleAmount as double) as sale_amount,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_sales_totals_by_amounts') }}
where load_date = '{{ batch_date }}'