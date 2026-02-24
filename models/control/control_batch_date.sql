{{ config(materialized='view') }}

select  max(to_date(load_date)) as batch_date
from {{ ref('bronze_orders') }}