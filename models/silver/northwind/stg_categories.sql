{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_categories') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(CategoryID as int)        as category_id,
        cast(CategoryName as string)   as category_name,
        cast(Description as string)    as description,
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
                partition by category_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * from deduplicated