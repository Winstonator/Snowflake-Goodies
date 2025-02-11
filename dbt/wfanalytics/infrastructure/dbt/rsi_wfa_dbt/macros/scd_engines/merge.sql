{% macro get_scd_merge_sql(target, source, unique_key, dest_columns, timestamp_column, predicates=none) -%}
  {{ adapter.dispatch('get_scd_merge_sql')(target, source, unique_key, dest_columns, timestamp_column, predicates) }}
{%- endmacro %}

{% macro default__get_scd_merge_sql(target, source, unique_key, dest_columns, predicates,  timestamp_column) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}
    {{ sql_header if sql_header is not none }}
    merge into {{ target }} as DBT_INTERNAL_DEST
        using  {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }} --> Key = key (source - surrogate key on ODS)
        AND DBT_INTERNAL_DEST.DISC_DT = to_date('9999-12-31') --> to current record in the CORE
        AND DBT_INTERNAL_DEST.DBT_HASH = DBT_INTERNAL_SOURCE.DBT_HASH 
    
    {% if unique_key %}

    when not matched AND DBT_INTERNAL_SOURCE.{{ unique_key }} NOT IN 
    (SELECT {{ unique_key }} FROM {{target}} DBT_INTERNAL_DEST where DBT_INTERNAL_DEST.{{ unique_key }} =  DBT_INTERNAL_SOURCE.{{ unique_key }})
    then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

    when not matched AND DBT_INTERNAL_SOURCE.DBT_HASH NOT IN 
    (SELECT DBT_HASH FROM {{target}} DBT_INTERNAL_DEST 
    WHERE DBT_INTERNAL_DEST.{{ unique_key }}  = DBT_INTERNAL_SOURCE.{{ unique_key }}
    and DBT_INTERNAL_DEST.DISC_DT = to_date('9999-12-31'))
    then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})
    {% endif %};

    UPDATE {{ target }} DBT_INTERNAL_DEST
    SET
    "DISC_DT" = DBT_INTERNAL_SOURCE.RPT_EFF_DT-1,
    "IS_CURRENT" = FALSE
    FROM (
    SELECT
        DBT_HASH, RPT_EFF_DT,
        {{ unique_key }} 
        FROM
        {{ source }}
    ) DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_DEST.{{ unique_key }}  = DBT_INTERNAL_SOURCE.{{ unique_key }}  
    and DBT_INTERNAL_DEST.DBT_HASH <> DBT_INTERNAL_SOURCE.DBT_HASH 
    and DBT_INTERNAL_DEST.DISC_DT = to_date('9999-12-31');


{% endmacro %}

{% macro default__get_merge_sql(target, source, unique_key, dest_columns, predicates) -%}
    {%- set predicates = [] if predicates is none else [] + predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set update_columns = config.get('merge_update_columns', default = dest_columns | map(attribute="quoted") | list) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {%- set no_update_columns = config.get('merge_no_update_columns', default = []) -%}

    {% if unique_key %}
        {% set unique_key_match %}
            DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
        {% endset %}
        {% do predicates.append(unique_key_match) %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using {{ source }} as DBT_INTERNAL_SOURCE
        on {{ predicates | join(' and ') }}

    {% if unique_key %}
    when matched then update set
        {% if config.get('merge_update_columns') %}
            {%- for column_name in update_columns -%}
                {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
                {%- if not loop.last %}, {%- endif %}
            {%- endfor %}
        {% else %}
            {%- for column in dest_columns -%}
                {%- if column.name not in no_update_columns %}
                    {{ adapter.quote(column.name) }} = DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}
                    {%- if not loop.last %}, {%- endif %}
                {%- endif %}
            {%- endfor -%}
        {% endif %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{% endmacro %}