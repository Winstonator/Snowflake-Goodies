{% macro udf_int_to_date() %}
    {% do log("Intiating Macro udf_int_to_date", info=True) %}
        {% set create_udf_stmt %}
            create or replace function {{target.database}}.core.inttodate(v bigint)
                returns date
            as

            $$
                to_date(to_varchar(v),'yyyymmdd')
            $$;
        {% endset %}
    {% do log("Sending macro to model.", info=True) %}
    {{ return(create_udf_stmt) }}
{% endmacro %}