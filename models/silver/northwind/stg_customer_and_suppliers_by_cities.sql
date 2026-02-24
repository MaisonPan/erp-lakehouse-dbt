{{ config(materialized='table') }}

{% set batch_date = get_batch_date() %}

select
    cast(City as string)        as city,
    cast(CompanyName as string) as company_name,
    cast(ContactName as string) as contact_name,
    cast(Relationship as string) as relationship_type,

    cast(load_date as date) as load_date,
    source_file,
    current_timestamp() as processed_at

from {{ ref('bronze_customer_and_suppliers_by_cities') }}
where load_date = '{{ batch_date }}'