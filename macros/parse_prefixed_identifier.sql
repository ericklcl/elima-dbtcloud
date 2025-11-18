{% macro parse_prefixed_identifier(raw_value) %}
    {# Convert value to string safely #}
    {% set raw = raw_value %}

    (
        WITH src AS (
            SELECT {{ raw }}::string AS input
        ),
        parsed AS (
            SELECT
                input,
                regexp_substr(input, '^[[:space:]]*([A-Za-z0-9]+)[[:space:]]*:[[:space:]]*(.+)[[:space:]]*$', 1, 1, 'e', 1) AS prefix,
                regexp_substr(input, '^[[:space:]]*([A-Za-z0-9]+)[[:space:]]*:[[:space:]]*(.+)[[:space:]]*$', 1, 1, 'e', 2) AS value
            FROM src
        )
        SELECT
            CASE 
                WHEN upper(prefix) IN ('PO','SO','CN','VT','SI','F8') 
                THEN upper(prefix)
                ELSE NULL
            END AS prefix,
            CASE 
                WHEN upper(prefix) IN ('PO','SO','CN','VT','SI','F8') 
                THEN trim(value)
                ELSE NULL
            END AS value
        FROM parsed
    )
{% endmacro %}
