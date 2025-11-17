{% macro normalize_stop_reference_id(column) %}
{# 
  Equivalent to Python:
    m = re.match(r'^(\d+)(_.+)$', s)
    head = m.group(1)
    tail = m.group(2)
    return strip_leading_zeros_if_numeric(head) + tail
#}

{% set pattern = '^(\\\\d+)(_.+)$' %}

CASE
    -- If NULL or empty
    WHEN {{ column }} IS NULL OR {{ column }} = '' THEN {{ column }}

    -- If regex does not match, return original value
    WHEN REGEXP_SUBSTR({{ column }}::VARCHAR, '{{ pattern }}', 1, 1, 'e', 1) IS NULL
        THEN {{ column }}::VARCHAR

    ELSE
        -- Extract head (group 1) and strip leading zeros
        {{ strip_leading_zeros_if_numeric(
            "REGEXP_SUBSTR(" ~ column ~ "::VARCHAR, '" ~ pattern ~ "', 1, 1, 'e', 1)"
        ) }}
        ||
        -- Extract tail (group 2)
        REGEXP_SUBSTR({{ column }}::VARCHAR, '{{ pattern }}', 1, 1, 'e', 2)
END
{% endmacro %}
