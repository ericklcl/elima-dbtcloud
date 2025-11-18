{% macro is_party_like(value) %}
(
    CASE
        -- Python: if not s → False
        WHEN {{ value }} IS NULL OR TRIM({{ value }}::string) = '' THEN FALSE

        -- Python: if ":" in s → False
        WHEN CONTAINS({{ value }}::string, ':') THEN FALSE

        -- Python: if is_phone_like(s) → True
        WHEN {{ is_phone_like(value) }} THEN TRUE

        -- Python: if ONLY_DIG → False
        WHEN REGEXP_LIKE({{ value }}::string, '^[0-9]+$') THEN FALSE

        -- Python regex:
        -- ^[A-Za-z][A-Za-z0-9 &\-/\.'' ]{2,}$
        WHEN REGEXP_LIKE(
                {{ value }}::string,
                '^[A-Za-z][A-Za-z0-9 &\\-/\\.'' ]{2,}$'
             )
        THEN TRUE

        ELSE FALSE
    END
)
{% endmacro %}
