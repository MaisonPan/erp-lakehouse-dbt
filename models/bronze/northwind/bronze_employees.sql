{{ config(materialized='incremental', on_schema_change='append_new_columns') }}

{{ northwind_subject('Employees') }}

{% if is_incremental() %}
where load_date > (select coalesce(max(load_date), date('1900-01-01')) from {{ this }})
{% endif %}