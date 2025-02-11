{% macro generate_sequence(source_table, primary_key) -%}
    {% do log(primary_key, info=True) %}
    {% do log(source_table, info=True) %}

    {% if primary_key %}
      {% set query %}
        SELECT max({{ primary_key }})+1 FROM {{ source_table }}
      {% endset %}
      {% do log(query, info=True) %}
    {% else %}
      {% set query %}
        SELECT 1 FROM {{ source_table }}
      {% endset %}
    {% endif %}

    {% if execute %}
    {% set results = run_query(query) %}

    {%- set max_sequence_id =  run_query(query).columns[0][0] %}
    {% do log(max_sequence_id, info=True) %}
    {%- set sequence_name = 'SEQ_' ~ this.identifier %}

    {% set sequence_query %}

      CREATE SEQUENCE IF NOT EXISTS {{ sequence_name }} start = {{ max_sequence_id }} increment = 1;

    {% endset %} 


    {% do run_query(sequence_query) %}
    {% endif %}
    {% do return( sequence_name ) %}



{%- endmacro %}