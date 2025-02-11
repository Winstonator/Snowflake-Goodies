{% macro udf_timestamp_to_int() %}
    {% do log("Intiating Macro udf_timestamp_to_int", info=True) %}
        {% set create_udf_stmt %}
            create or replace function {{target.database}}.core.timestamptoint(v timestamp)
                returns bigint
            as

            $$
                to_number(to_varchar(v, 'yyyymmddhh24missff3'))
            $$;
        {% endset %}
    {% do log("Sending macro to model.", info=True) %}
    {{ return(create_udf_stmt) }}
{% endmacro %}