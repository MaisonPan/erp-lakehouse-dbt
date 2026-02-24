{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_customerdemographics') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(CustomerTypeID as string) as customer_type_id,
        cast(CustomerDesc as string)   as customer_desc,

        cast(load_date as date)        as load_date,
        source_file,
        current_timestamp()            as processed_at
    from source

),

deduplicated as (

    select *
    from (
        select *,
            row_number() over (
                partition by customer_type_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * except (rn) from deduplicated