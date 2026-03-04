{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_products') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(ProductID as int)      as product_id,
        cast(ProductName as string) as product_name,

        cast(SupplierID as int)     as supplier_id,
        cast(CategoryID as int)     as category_id,

        cast(QuantityPerUnit as string) as quantity_per_unit,
        cast(UnitPrice as double)   as unit_price,
        cast(UnitsInStock as int)   as units_in_stock,
        cast(UnitsOnOrder as int)   as units_on_order,
        cast(ReorderLevel as int)   as reorder_level,

        cast(Discontinued as int)   as discontinued,

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
                partition by product_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * except (rn) from deduplicated