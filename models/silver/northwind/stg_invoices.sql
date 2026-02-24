{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_invoices') }}
    where to_date(load_date) = to_date('{{ batch_date }}')

),

renamed as (

    select
        try_cast(OrderID as int)         as order_id,
        cast(CustomerID as string)       as customer_id,
        try_cast(ProductID as int)       as product_id,

        cast(Salesperson as string)      as salesperson,

        -- 你补充的关键字段
        try_cast(UnitPrice as decimal(38,18))    as unit_price,
        try_cast(Quantity as int)                as quantity,
        try_cast(Discount as double)             as discount,
        try_cast(ExtendedPrice as decimal(38,18)) as extended_price,
        try_cast(Freight as decimal(38,18))      as freight,

        to_date(load_date)               as load_date,
        source_file,
        current_timestamp()              as processed_at

    from source

)

select * from renamed