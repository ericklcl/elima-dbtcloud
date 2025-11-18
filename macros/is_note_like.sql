{% macro is_note_like(value) %}
(
    CASE
        ---------------------------------------------------------------------
        -- 1. Python: if not s → False
        ---------------------------------------------------------------------
        WHEN {{ value }} IS NULL OR TRIM({{ value }}::string) = '' THEN FALSE

        ---------------------------------------------------------------------
        -- 2. Python: if ":" in s → False
        ---------------------------------------------------------------------
        WHEN CONTAINS({{ value }}::string, ':') THEN FALSE

        ---------------------------------------------------------------------
        -- 3. Python: if ONLY_DIG.match(s) → False
        --    Regex: ^\d+$
        ---------------------------------------------------------------------
        WHEN REGEXP_LIKE({{ value }}::string, '^[0-9]+$') THEN FALSE

        ---------------------------------------------------------------------
        -- 4. Python:
        --    low = s.lower()
        --    return any(w in low for w in ["add ", "required", ...])
        --
        -- Implemented using LOWER() + CONTAINS()
        ---------------------------------------------------------------------
        WHEN 
            CONTAINS(LOWER({{ value }}::string), 'add ') OR
            CONTAINS(LOWER({{ value }}::string), 'required') OR
            CONTAINS(LOWER({{ value }}::string), 'precall') OR
            CONTAINS(LOWER({{ value }}::string), 'charge') OR
            CONTAINS(LOWER({{ value }}::string), 'bundle') OR
            CONTAINS(LOWER({{ value }}::string), 'ppd') OR
            CONTAINS(LOWER({{ value }}::string), 'drop') OR
            CONTAINS(LOWER({{ value }}::string), 'please') OR
            CONTAINS(LOWER({{ value }}::string), 'must')
        THEN TRUE

        ELSE FALSE
    END
)
{% endmacro %}
