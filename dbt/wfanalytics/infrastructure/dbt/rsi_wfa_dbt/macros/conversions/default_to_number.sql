{% macro default_to_number( Input_Field ) %}
    CASE WHEN  TRY_TO_NUMBER( {{ Input_Field}}::TEXT, 38, 0  ) IS NULL THEN NULL ELSE {{ Input_Field }} END
{% endmacro %}