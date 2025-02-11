{% macro default_boolean( Input_Field ) %}
    CASE WHEN  TRY_TO_BOOLEAN({{ Input_Field }}::TEXT) IS NULL THEN FALSE ELSE {{ Input_Field }} END
{% endmacro %}