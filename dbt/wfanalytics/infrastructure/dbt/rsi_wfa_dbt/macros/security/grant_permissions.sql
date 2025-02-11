{% macro grant_permissions(suffix) %}

    {% if flags.FULL_REFRESH %}
        {% set sql %}
            {% if target.name == "DEV" or target.name == "QA" %}
                -- READ 
                GRANT USAGE,MONITOR ON DATABASE {{target.name}}_EDW TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE,MONITOR ON SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON ALL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON FUTURE TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON ALL VIEWS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON ALL SEQUENCES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON ALL STAGES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON FUTURE STAGES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON ALL FILE FORMATS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON ALL STREAMS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT SELECT ON FUTURE STREAMS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT MONITOR ON ALL TASKS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;
                GRANT MONITOR ON FUTURE TASKS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_R;

                -- READ, WRITE
                GRANT USAGE,MONITOR ON DATABASE {{target.name}}_EDW TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE,MONITOR ON SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT,INSERT,UPDATE,TRUNCATE,DELETE,REFERENCES ON ALL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT,INSERT,UPDATE,TRUNCATE,DELETE,REFERENCES ON FUTURE TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT ON ALL VIEWS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE ON ALL SEQUENCES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE,READ,WRITE ON ALL STAGES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE,READ,WRITE ON FUTURE STAGES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE ON ALL FILE FORMATS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT ON ALL STREAMS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT SELECT ON FUTURE STREAMS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT MONITOR,OPERATE ON ALL TASKS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;
                GRANT MONITOR,OPERATE ON FUTURE TASKS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RW;

                -- READ, WRITE, CREATE
                GRANT USAGE,MONITOR ON DATABASE {{target.name}}_EDW TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE,MONITOR,CREATE TABLE,CREATE VIEW,CREATE MATERIALIZED VIEW,CREATE MASKING POLICY,CREATE SEQUENCE,CREATE FUNCTION,CREATE STREAM,CREATE TASK,CREATE PROCEDURE, CREATE STAGE ON SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT,INSERT,UPDATE,TRUNCATE,DELETE,REFERENCES ON ALL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT,INSERT,UPDATE,TRUNCATE,DELETE,REFERENCES ON FUTURE TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT ON ALL EXTERNAL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT ON FUTURE EXTERNAL TABLES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT ON ALL VIEWS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT ON FUTURE VIEWS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE ON ALL SEQUENCES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE ON FUTURE SEQUENCES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE,READ,WRITE ON ALL STAGES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE,READ,WRITE ON FUTURE STAGES IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE ON ALL FILE FORMATS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE ON FUTURE FILE FORMATS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE ON ALL FUNCTIONS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT USAGE ON FUTURE FUNCTIONS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT ON ALL STREAMS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT SELECT ON FUTURE STREAMS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT MONITOR,OPERATE ON ALL TASKS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
                GRANT MONITOR,OPERATE ON FUTURE TASKS IN SCHEMA {{target.name}}_EDW.{{target.schema}} TO ROLE {{target.name}}_EDW_{{suffix}}_RWC;
            {% endif %}
            {% if target.name == "PROD" %}
            {% endif %}
        {% endset %}

        {% do run_query(sql) %}

        {% do log("Privileges granted", info=True) %}
    {% endif %}

{% endmacro %}
