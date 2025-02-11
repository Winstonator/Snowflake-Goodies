{% macro date_parsing( Input_Field ) %}
CONCAT(
      SUBSTRING({{ Input_Field }},1,4),'-',
      SUBSTRING({{ Input_Field }},5,2),'-',
      SUBSTRING({{ Input_Field }},7,2),' ',
      SUBSTRING({{ Input_Field }},9,2),':',
      SUBSTRING({{ Input_Field }},11,2),':',
      SUBSTRING({{ Input_Field }},13,2)
      )::timestamp_ltz(9)
{% endmacro %}