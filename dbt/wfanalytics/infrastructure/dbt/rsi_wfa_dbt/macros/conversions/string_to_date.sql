{% macro string_to_date(Input_Field) %}
Case 
When To_Char({{ Input_Field }}) is null or length(To_Char({{ Input_Field }})) <> 8 then '1900-01-01'
When TRY_TO_DATE(CONCAT(SUBSTRING({{ Input_Field }},1,4),'-',SUBSTRING({{ Input_Field }},5,6),'-',SUBSTRING({{ Input_Field }},7,8))) is null then '1900-01-01'
When To_Char({{ Input_Field }}) ='99999999' then '1700-01-01'
When TO_DATE(SUBSTRING({{ Input_Field }},1,8),'YYYYMMDD') = '1940-01-01' then '1900-01-01'
When TO_DATE(SUBSTRING({{ Input_Field }},1,8),'YYYYMMDD') < '1700-01-02' then '1700-01-01'
When TO_DATE(SUBSTRING({{ Input_Field }},1,8),'YYYYMMDD') > '4000-12-30' then '4000-12-30'
else TO_DATE(SUBSTRING({{ Input_Field }},1,8),'YYYYMMDD')
END
{% endmacro %}