{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_shippers') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(ShipperID as int)      as shipper_id,
        cast(CompanyName as string) as company_name,
        cast(Phone as string)       as phone,

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
                partition by shipper_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * from deduplicated