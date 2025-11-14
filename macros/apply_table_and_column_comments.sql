{% macro apply_table_and_column_comments(model) %}
    /*
        Applies physical comments in Snowflake for:
        - The table (using the model description)
        - Each column (using column descriptions)
    */

    {% set db = model.database %}
    {% set schema = model.schema %}
    {% set table = model.name %}

    {# ----------- 1. COMMENT ON TABLE ----------- #}
    {% if model.description %}
        COMMENT ON TABLE {{ db }}.{{ schema }}.{{ table }}
            IS '{{ model.description | replace("'", "''") }}';
    {% endif %}

    {# ----------- 2. COMMENT ON COLUMNS ----------- #}
    {% for column in model.columns %}
        {% if column.description %}
            COMMENT ON COLUMN {{ db }}.{{ schema }}.{{ table }}.{{ column.name }}
                IS '{{ column.description | replace("'", "''") }}';
        {% endif %}
    {% endfor %}
{% endmacro %}
