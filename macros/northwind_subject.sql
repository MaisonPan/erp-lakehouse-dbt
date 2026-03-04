{% macro northwind_subject(subject) %}
with src as (
  select
    *,
    to_date(
      regexp_extract(
        input_file_name(),
        '/{{ subject }}/([0-9]{4}-[0-9]{2}-[0-9]{2})/',
        1
      )
    ) as load_date,
    input_file_name() as source_file
  from parquet.`abfss://landing@panmaisonadls.dfs.core.windows.net/northwind/{{ subject }}/*/*.parquet`
)
select * from src
{% endmacro %}
