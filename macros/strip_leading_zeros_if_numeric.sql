{# 
    Strips leading zeros from strings that contain only digits.
    
    Normalizes numeric strings by removing leading zeros (e.g., "000123" → "123").
    Non-numeric strings and null values are returned unchanged.
    
    Examples:
    - "000123" → "123"
    - "ABC123" → "ABC123" (unchanged)
    - "000000" → "0"
    
    Uses REGEXP_LIKE to detect numeric-only strings, LTRIM to remove zeros.
#}
{% macro strip_leading_zeros_if_numeric(value) %}
    {# Return null if input is null #}
    {%- if value is none %}
        null
    {%- else %}
        {# Handle both literal strings and column references #}
        {%- if value is string and value.startswith("'") and value.endswith("'") %}
            case
                when regexp_like({{ value }}, '^[0-9]+$') then
                    case
                        when ltrim({{ value }}, '0') = '' then '0'
                        else ltrim({{ value }}, '0')
                    end
                else {{ value }}
            end
        {%- else %}
            case
                when regexp_like({{ value }}, '^[0-9]+$') then
                    case
                        when ltrim({{ value }}, '0') = '' then '0'
                        else ltrim({{ value }}, '0')
                    end
                else {{ value }}
            end
        {%- endif %}
    {%- endif %}
{% endmacro %}
