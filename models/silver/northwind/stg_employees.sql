{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_employees') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(EmployeeID as int)        as employee_id,
        cast(LastName as string)       as last_name,
        cast(FirstName as string)      as first_name,
        cast(Title as string)          as title,
        cast(TitleOfCourtesy as string) as title_of_courtesy,
        cast(BirthDate as date)        as birth_date,
        cast(HireDate as date)         as hire_date,

        cast(Address as string)        as address,
        cast(City as string)           as city,
        cast(Region as string)         as region,
        cast(PostalCode as string)     as postal_code,
        cast(Country as string)        as country,
        cast(HomePhone as string)      as home_phone,
        cast(Extension as string)      as extension,

        cast(ReportsTo as int)         as reports_to,

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
                partition by employee_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * from deduplicated