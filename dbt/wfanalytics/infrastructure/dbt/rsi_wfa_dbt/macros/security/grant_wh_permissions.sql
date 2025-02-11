{% macro grant_wh_permissions() %}

    {% if flags.FULL_REFRESH %}
        {% set sql %}
            {% if target.name == "DEV" or target.name == "QA" %}
                GRANT USAGE ON WAREHOUSE {{target.name}}_LOAD_WH TO ROLE {{target.name}}_LOAD_WH_U;
                GRANT USAGE,OPERATE ON WAREHOUSE {{target.name}}_LOAD_WH TO ROLE {{target.name}}_LOAD_WH_UO;
                GRANT USAGE,OPERATE,MODIFY,MONITOR ON WAREHOUSE {{target.name}}_LOAD_WH TO ROLE {{target.name}}_LOAD_WH_UOMM;
            {% endif %}
            {% if target.name == "PROD" %}
            {% endif %}
        {% endset %}

        {% do run_query(sql) %}

        {% do log("WH Privileges granted", info=True) %}
    {% endif %}

{% endmacro %}
