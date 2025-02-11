{% macro udf_int_to_timestamp() %}
    {% do log("Intiating Macro udf_int_to_timestamp", info=True) %}
        {% set create_udf_stmt %}
            create or replace function {{target.database}}.core.inttotimestamp(v bigint)
                returns timestamp
            as

            $$
                to_timestamp(to_varchar(v),'yyyymmddhh24missff3')
            $$;
        {% endset %}
    {% do log("Sending macro to model.", info=True) %}
    {{ return(create_udf_stmt) }}
{% endmacro %}