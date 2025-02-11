# macros/add_not_null_constraints.sql
{% macro add_not_null_constraints(not_null_fields) %}
    {%- for column in not_null_fields -%}
    {% set query %}
    ALTER TABLE {{ this }} ALTER {{ column }} NOT NULL;
    {% endset %}
    {% do run_query(query)  %}
    {%- endfor -%}
{% endmacro %}