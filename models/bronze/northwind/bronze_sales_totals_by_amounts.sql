{{ config(materialized='incremental') }}

WITH src AS (
    SELECT
        *,
        regexp_extract(
            input_file_name(),
            '/Orders/([0-9]{4}-[0-9]{2}-[0-9]{2})/',
            1
        ) AS load_date
    FROM parquet.`abfss://landing@panmaisonadls.dfs.core.windows.net/northwind/Sales_Totals_by_Amounts/*/Sales_Totals_by_Amounts`
)

SELECT *
FROM src

{% if is_incremental() %}
WHERE load_date > (SELECT coalesce(max(load_date),'1900-01-01') FROM {{ this }})
{% endif %}