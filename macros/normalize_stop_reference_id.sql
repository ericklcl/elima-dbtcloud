{% macro normalize_stop_reference_id(s) %}
    {# Return input if null #}
    {% if s is none %}
        {{ return(s) }}
    {% endif %}

    {# Convert to string #}
    {% set input_str = s | string %}

    {# Extract the numeric head: group 1 from ^(\d+)(_.+)$ #}
    {% set head = "REGEXP_SUBSTR('" ~ input_str ~ "', '^(\\\\d+)(_.+)$', 1, 1, 'e', 1)" %}

    {# Extract the tail: group 2 #}
    {% set tail = "REGEXP_SUBSTR('" ~ input_str ~ "', '^(\\\\d+)(_.+)$', 1, 1, 'e', 2)" %}

    {# If regex did not match, return original string #}
    {% set head_sql %}
        CASE 
            WHEN {{ head }} IS NULL THEN '{{ input_str }}'
            ELSE {{ strip_leading_zeros_if_numeric(head) }} || {{ tail }}
        END
    {% endset %}

    {{ return(head_sql) }}
{% endmacro %}
