{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_orders') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(OrderID as int)        as order_id,
        cast(CustomerID as string)  as customer_id,
        cast(EmployeeID as int)     as employee_id,
        cast(OrderDate as date)     as order_date,
        cast(RequiredDate as date)  as required_date,
        cast(ShippedDate as date)   as shipped_date,
        cast(ShipVia as int)        as ship_via,
        cast(Freight as double)     as freight,
        cast(load_date as date)     as load_date,
        source_file,
        current_timestamp()         as processed_at

    from source

),

deduplicated as (

    select *
    from (
        select *,
            row_number() over (
                partition by order_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * from deduplicated