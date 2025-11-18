{% macro parse_identifier_value(raw_text) %}
    CASE
        WHEN regexp_substr({{ raw_text }}::string,
                           '^[[:space:]]*[A-Za-z0-9]+[[:space:]]*:(.+)$',
                           1, 1, 'e', 1) IS NOT NULL
        THEN trim(regexp_substr({{ raw_text }}::string,
                                '^[[:space:]]*[A-Za-z0-9]+[[:space:]]*:(.+)$',
                                1, 1, 'e', 1))
        ELSE NULL
    END
{% endmacro %}