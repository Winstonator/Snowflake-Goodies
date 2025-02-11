{% macro udf_date_to_int() %}
    {% do log("Intiating Macro udf_date_to_int", info=True) %}
        {% set create_udf_stmt %}
            create or replace function {{target.database}}.core.datetoint(v date)
                returns bigint
            as

            $$
                to_number(to_varchar(v, 'yyyymmdd'))
            $$;
        {% endset %}
    {% do log("Sending macro to model.", info=True) %}
    {{ return(create_udf_stmt) }}
{% endmacro %}