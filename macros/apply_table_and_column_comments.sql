{% macro apply_table_and_column_comments(model) %}

    {# Collect all comment statements here #}
    {% set statements = [] %}

    {# Table comment #}
    {% if model.description %}
        {% set table_comment %}
COMMENT ON TABLE {{ model.database }}.{{ model.schema }}.{{ model.name }} 
    IS '{{ model.description | replace("'", "''") }}'
        {% endset %}
        {% do statements.append(table_comment) %}
    {% endif %}

    {# Column comments #}
    {% for column in model.columns %}
        {% if column.description %}
            {% set col_comment %}
COMMENT ON COLUMN {{ model.database }}.{{ model.schema }}.{{ model.name }}.{{ column.name }}
    IS '{{ column.description | replace("'", "''") }}'
            {% endset %}
            {% do statements.append(col_comment) %}
        {% endif %}
    {% endfor %}

    {# Prevent empty SQL (Snowflake dislikes empty statements) #}
    {% if statements | length == 0 %}
        {% set fallback %}
SELECT 1 /* No comments to apply */
        {% endset %}
        {% do statements.append(fallback) %}
    {% endif %}

    {{ statements | join(';\n') }}

{% endmacro %}
