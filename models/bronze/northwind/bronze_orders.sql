{{ config(
    materialized='incremental'
) }}

SELECT
    *,
    regexp_extract(input_file_name(),
        '/Orders/([0-9]{4}-[0-9]{2}-[0-9]{2})/',1) AS load_date,
        input_file_name() as source_file
FROM parquet.`abfss://landing@panmaisonadls.dfs.core.windows.net/northwind/Orders/*/Orders`

{% if is_incremental() %}
WHERE regexp_extract(input_file_name(),
        '/Orders/([0-9]{4}-[0-9]{2}-[0-9]{2})/',1)
      > (SELECT max(load_date) FROM {{ this }})
{% endif %}
