{% macro get_batch_date() %}

    {% set q %}
        select max(load_date) as batch_date
        from {{ ref('bronze_orders') }}
    {% endset %}

    {% if execute %}
        {% set res = run_query(q) %}
        {% set batch = res.columns[0].values()[0] %}
        {{ return(batch) }}
    {% else %}
        {{ return(none) }}
    {% endif %}

{% endmacro %}