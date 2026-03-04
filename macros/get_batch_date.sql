{% macro get_batch_date() %}

    {% set q %}
        select batch_date from {{ ref('control_batch_date') }}
    {% endset %}

    {% if execute %}
        {% set res = run_query(q) %}
        {% set batch = res.columns[0].values()[0] %}
        {{ return(batch) }}
    {% else %}
        {{ return(none) }}
    {% endif %}

{% endmacro %}