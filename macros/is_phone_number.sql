{% macro is_phone_number(value) %}
    REGEXP_LIKE(
        {{ value }}::string,
        '(\\+?\\d{1,2}[ \\-\\.]?)?(\\(?\\d{3}\\)?[ \\-\\.]?\\d{3}[ \\-\\.]?\\d{4})'
    )
{% endmacro %}
