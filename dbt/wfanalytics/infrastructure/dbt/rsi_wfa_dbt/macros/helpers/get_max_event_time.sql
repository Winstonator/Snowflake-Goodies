# macros/get_max_event_time.sql
{% macro get_max_event_time(date_field) %}
{% if execute and is_incremental() and env_var('ENABLE_MAX_EVENT_TIME_MACRO', '1') == '1' %}
{% set query %}
SELECT max({{date_field}}) FROM {{ this }};
{% endset %}
{% set max_event_time = run_query(query).columns[0][0] %}
{% do return(max_event_time) %}
{% endif %}
{% endmacro %}