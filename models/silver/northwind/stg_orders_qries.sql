{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(OrderID as int)       as order_id,
    cast(CustomerID as string) as customer_id,
    cast(EmployeeID as int)    as employee_id,
    cast(OrderDate as date)    as order_date,
    cast(ShippedDate as date)  as shipped_date,
    cast(Freight as double)    as freight,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_orders_qries') }}
where load_date = '{{ batch_date }}'