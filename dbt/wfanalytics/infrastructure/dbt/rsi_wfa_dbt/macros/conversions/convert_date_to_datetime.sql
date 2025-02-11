{% macro convert_date_to_datetime( Input_Field ) %}
CASE WHEN TRY_TO_DATE(CONCAT(SUBSTRING({{ Input_Field }}::VARCHAR,1,4),'-',SUBSTRING({{ Input_Field }}::VARCHAR,5,2),'-',SUBSTRING({{ Input_Field }}::VARCHAR,7,2))) IS NOT NULL THEN
CONCAT(SUBSTRING({{ Input_Field }}::VARCHAR,1,4),'-',SUBSTRING({{ Input_Field }}::VARCHAR,5,2),'-',SUBSTRING({{ Input_Field }}::VARCHAR,7,2))
ELSE '1900-01-01'
END::DATE 
{% endmacro %}