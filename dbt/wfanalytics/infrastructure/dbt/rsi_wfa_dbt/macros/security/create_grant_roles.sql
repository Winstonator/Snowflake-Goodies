{% macro create_grant_roles(suffix) %}

    {% if flags.FULL_REFRESH %}
        {% set sql %}
            {% if target.name == "DEV" or target.name == "QA" %}
                CREATE ROLE IF NOT EXISTS {{target.name}}_EDW_{{suffix}}_R;
                GRANT ROLE {{target.name}}_EDW_{{suffix}}_R TO ROLE {{target.name}}_SYSADMIN;

                CREATE ROLE IF NOT EXISTS {{target.name}}_EDW_{{suffix}}_RW;
                GRANT ROLE {{target.name}}_EDW_{{suffix}}_RW TO ROLE {{target.name}}_SYSADMIN;

                CREATE ROLE IF NOT EXISTS {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT ROLE {{target.name}}_EDW_{{suffix}}_RWC TO ROLE {{target.name}}_SYSADMIN;

                -- WH
                CREATE ROLE IF NOT EXISTS {{target.name}}_LOAD_WH_U;
                GRANT ROLE {{target.name}}_LOAD_WH_U TO ROLE {{target.name}}_SYSADMIN;

                CREATE ROLE IF NOT EXISTS {{target.name}}_LOAD_WH_UO;
                GRANT ROLE {{target.name}}_LOAD_WH_UO TO ROLE {{target.name}}_SYSADMIN;

                CREATE ROLE IF NOT EXISTS {{target.name}}_LOAD_WH_UOMM;
                GRANT ROLE {{target.name}}_LOAD_WH_UOMM TO ROLE {{target.name}}_SYSADMIN;

                GRANT ROLE {{target.name}}_EDW_{{suffix}}_R TO ROLE DOPSREADNONPROD;
                GRANT ROLE {{target.name}}_EDW_{{suffix}}_R TO ROLE ENTERPRISEDATAREADNONPROD;
                GRANT ROLE {{target.name}}_EDW_{{suffix}}_RWC TO ROLE ENTERPRISEDATAWRITENONPROD;

                GRANT ROLE {{target.name}}_LOAD_WH_U TO ROLE DOPSREADNONPROD;
                GRANT ROLE {{target.name}}_LOAD_WH_U TO ROLE ENTERPRISEDATAREADNONPROD;
                GRANT ROLE {{target.name}}_LOAD_WH_U TO ROLE ERPREADNONPROD;

                GRANT ROLE {{target.name}}_LOAD_WH_UO TO ROLE DOPSWRITENONPROD;
                GRANT ROLE {{target.name}}_LOAD_WH_UO TO ROLE ENTERPRISEDATAWRITENONPROD;
                GRANT ROLE {{target.name}}_LOAD_WH_UO TO ROLE ERPWRITENONPROD;

                GRANT ROLE {{target.name}}_LOAD_WH_UOMM TO ROLE SNOWFLAKEADMINNONPROD;
            {% endif %}
            {% if target.name == "PROD" %}
            {% endif %}
        {% endset %}

        {% do run_query(sql) %}

        {% do log("Roles created", info=True) %}
    {% endif %}

{% endmacro %}
