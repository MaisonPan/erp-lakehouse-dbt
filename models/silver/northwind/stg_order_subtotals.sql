{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(OrderID as int) as order_id,
    cast(Subtotal as double) as subtotal,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_order_subtotals') }}
where load_date = '{{ batch_date }}'