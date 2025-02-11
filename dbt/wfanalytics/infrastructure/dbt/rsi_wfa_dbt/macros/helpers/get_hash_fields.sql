{% macro get_hash_fields(exclude_columns) -%}

    {% set hash_columns = adapter.get_columns_in_relation(this) %}

    {% set exclude_columns_cleaned = [] %}
    {% set exclude_columns_ignore = [] %}
    {% set hash_columns_cleaned = [] %}

    {% for column in exclude_columns %}
        {{ exclude_columns_cleaned.append(column.upper()) }}
    {% endfor %}
    
    {% for column in hash_columns %}
        {% if column.name in exclude_columns_cleaned %}
            {{ exclude_columns_ignore.append(column.name) }}
        {% else %}
            {{ hash_columns_cleaned.append(column.name) }}

        {%- endif %}
    {%- endfor %}
    
    {% do return( hash_columns_cleaned ) %}

{% endmacro %}