{{ config(materialized='table') }}

select
  employee_id,
  first_name,
  last_name,
  title,
  title_of_courtesy,
  birth_date,
  hire_date,
  address,
  city,
  region,
  postal_code,
  country,
  home_phone,
  extension,
  reports_to
from {{ ref('stg_employees') }}