{# 
    Normalizes stop reference IDs by removing leading zeros from numeric prefix.
    
    Processes IDs in format "NUMBER_SUFFIX" (e.g., "000123_PICKUP" → "123_PICKUP").
    Returns original value if pattern doesn't match or input is null.
    
    Examples:
    - "000123_PICKUP" → "123_PICKUP"  
    - "007_DELIVERY" → "7_DELIVERY"
    - "ABC_123" → "ABC_123" (unchanged - no leading digits)
    
    Requires: strip_leading_zeros_if_numeric() macro
    Pattern: ^(\d+)(_.+)$ (digits + underscore + suffix)
#}
{% macro normalize_stop_reference_id(s) %}
    {# Return null if input is null #}
    {% if s is none %}
        {{ return(s) }}
    {% endif %}

    {% set input_str = s | string %}

    {# Extract numeric prefix and suffix using regex groups #}
    {% set head = "REGEXP_SUBSTR('" ~ input_str ~ "', '^(\\\\d+)(_.+)$', 1, 1, 'e', 1)" %}
    {% set tail = "REGEXP_SUBSTR('" ~ input_str ~ "', '^(\\\\d+)(_.+)$', 1, 1, 'e', 2)" %}

    {# If no match, return original; if match, normalize numeric part #}
    {% set head_sql %}
        CASE 
            WHEN {{ head }} IS NULL THEN '{{ input_str }}'
            ELSE {{ strip_leading_zeros_if_numeric(head) }} || {{ tail }}
        END
    {% endset %}

    {{ return(head_sql) }}
{% endmacro %}
