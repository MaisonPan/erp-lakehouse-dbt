{{ config(materialized='table') }}

SELECT
    *,
    regexp_extract(input_file_name(), '/Orders/([0-9]{4}-[0-9]{2}-[0-9]{2})/', 1) AS load_date
FROM parquet.`abfss://landing@panmaisonadls.dfs.core.windows.net/northwind/Orders/*/Orders`
