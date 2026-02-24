{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_regions') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(RegionID as int)              as region_id,
        trim(cast(RegionDescription as string)) as region_description,

        cast(load_date as date)            as load_date,
        source_file,
        current_timestamp()                as processed_at
    from source

),

deduplicated as (

    select *
    from (
        select *,
            row_number() over (
                partition by region_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * from deduplicated