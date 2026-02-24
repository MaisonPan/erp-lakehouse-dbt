{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_order_details') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(OrderID as int)      as order_id,
        cast(ProductID as int)    as product_id,
        cast(UnitPrice as double) as unit_price,
        cast(Quantity as int)     as quantity,
        cast(Discount as double)  as discount,
        cast(UnitPrice as double) * cast(Quantity as int) * (1 - cast(Discount as double)) as line_amount,
        cast(load_date as date)   as load_date,
        source_file,
        current_timestamp()       as processed_at

    from source

),

deduplicated as (

    select *
    from (
        select *,
            row_number() over (
                partition by order_id, product_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * except (rn) from deduplicated