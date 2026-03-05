{{ config(materialized='table') }}

with bounds as (
  select
    min(order_date) as min_date,
    (select max(shipped_date) from {{ ref('stg_invoices') }})  as max_date
  from {{ ref('stg_orders') }}
),

dates as (
  select explode(sequence(min_date, max_date, interval 1 day)) as date_day
  from bounds
)

select
  date_day                           as date_day,
  year(date_day)                     as year,
  quarter(date_day)                  as quarter,
  month(date_day)                    as month,
  date_format(date_day,'MMM')        as month_short,
  date_format(date_day, 'yyyy-MM')   as year_month,
  weekofyear(date_day)               as week_of_year,
  dayofmonth(date_day)               as day_of_month,
  date_format(date_day, 'EEEE')      as day_name,
  case when dayofweek(date_day) in (1,7) then true else false end as is_weekend
from dates