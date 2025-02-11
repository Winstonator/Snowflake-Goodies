{% macro dedupe_tables( Input_Table, SK_Name ) %}
    {% set table_name =  Input_Table %}
    {% set current_time = dbt_utils.pretty_time(format='%Y%m%d%H%M%S')%}
      {% do log(current_time, info=True) %}
  
    {% set query %}
            CREATE OR REPLACE TABLE CORE.{{Input_Table}}_DEDUPS  AS
            with X as (Select {{ SK_Name }}, eff_dt, count(*)  FROM CORE.{{Input_Table}} group by 1,2 having count(*)> 1),
                    TEMP1 AS ( SELECT *,  ROW_NUMBER() OVER(PARTITION BY {{ SK_Name }}, EFF_DT ORDER BY DISC_DT DESC) AS ROW_NUM FROM CORE.{{Input_Table}}
                                    WHERE {{ SK_Name }} in ( select distinct {{ SK_Name }} from x) ),
                Y as (Select {{ SK_Name }}, eff_dt, count(*)  FROM CORE.{{Input_Table}} group by 1,2 having count(*)=1),
                    TEMP2 AS (SELECT *, 1 AS ROW_NUM FROM CORE.{{Input_Table}} WHERE {{ SK_Name }} IN ( SELECT {{ SK_Name }} FROM Y))
                            
            SELECT  {{ dbt_utils.star(ref(table_name) ) }}
            FROM TEMP1 WHERE ROW_NUM =1        
            UNION         
            SELECT {{ dbt_utils.star(ref(table_name) )}}
            FROM TEMP2;
    {% endset %}
    {% do run_query(query) %}
    {% set query %}
            ALTER TABLE CORE.{{Input_Table}} RENAME TO {{Input_Table}}_BKP;
            ALTER TABLE CORE.{{Input_Table}}_DEDUPS RENAME TO {{Input_Table}};
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}