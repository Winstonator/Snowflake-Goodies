{% macro get_last_query_id() %}
    {% if execute%}
        {% set query %}
        select last_query_id()  
        {% endset %}
        {% set query_id = run_query(query).columns[0][0] %}
        {% do return(query_id) %}
    {% endif %}
{% endmacro %}
