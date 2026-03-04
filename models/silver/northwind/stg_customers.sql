{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

with source as (

    select *
    from {{ ref('bronze_customers') }}
    where load_date = '{{ batch_date }}'

),

renamed as (

    select
        cast(CustomerID as string)   as customer_id,
        cast(CompanyName as string)  as company_name,
        cast(ContactName as string)  as contact_name,
        cast(ContactTitle as string) as contact_title,
        cast(Address as string)      as address,
        cast(City as string)         as city,
        cast(Region as string)       as region,
        cast(PostalCode as string)   as postal_code,
        cast(Country as string)      as country,
        cast(Phone as string)        as phone,
        cast(Fax as string)          as fax,
        cast(load_date as date)      as load_date,
        source_file,
        current_timestamp()          as processed_at

    from source

),

deduplicated as (

    select *
    from (
        select *,
            row_number() over (
                partition by customer_id
                order by source_file desc
            ) as rn
        from renamed
    )
    where rn = 1

)

select * from deduplicated