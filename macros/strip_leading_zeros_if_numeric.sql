{% macro strip_leading_zeros_if_numeric(value) %}
    {# Handle null inputs #}
    {%- if value is none %}
        null
    {%- else %}
        {# Detect if the value is a literal string (quoted) or a column/expression #}
        {%- if value is string and value.startswith("'") and value.endswith("'") %}
            {# Literal string path #}
            case
                when regexp_like({{ value }}, '^[0-9]+$') then
                    case
                        when ltrim({{ value }}, '0') = '' then '0'
                        else ltrim({{ value }}, '0')
                    end
                else {{ value }}
            end
        {%- else %}
            {# Column or SQL expression path #}
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
