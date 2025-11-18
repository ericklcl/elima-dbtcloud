{% macro is_phone_like(value) %}
(
    CASE 
        WHEN REGEXP_LIKE(
                {{ value }}::string,
                '(\\+?\\d{1,2}[\\s\\-\\.]?)?(\\(?\\d{3}\\)?[\\s\\-\\.]?\\d{3}[\\s\\-\\.]?\\d{4})'
             )
        THEN TRUE
        ELSE FALSE
    END
)
{% endmacro %}
