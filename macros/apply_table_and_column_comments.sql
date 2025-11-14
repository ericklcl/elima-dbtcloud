{% macro apply_table_and_column_comments(model) %}
    {# 
        Safe version: always returns a valid SQL string.
        Prevents dbt from crashing when no descriptions exist. 
    #}

    {% set statements = [] %}

    {# ---- Table comment ---- #}
    {% if model.description %}
        {% do statements.append(
            "COMMENT ON TABLE " ~ model.database ~ "." ~ model.schema ~ "." ~ model.name ~
            " IS '" ~ (model.description | replace(\"'\", \"''\")) ~ "'"
        ) %}
    {% endif %}

    {# ---- Column comments ---- #}
    {% for column in model.columns %}
        {% if column.description %}
            {% do statements.append(
                "COMMENT ON COLUMN " ~ model.database ~ "." ~ model.schema ~ "." ~ model.name ~ "." ~ column.name ~
                " IS '" ~ (column.description | replace(\"'\", \"''\")) ~ "'"
            ) %}
        {% endif %}
    {% endfor %}

    {# ---- Prevent empty SQL (THE IMPORTANT FIX) ---- #}
    {% if statements | length == 0 %}
        {% do statements.append("/* No comments to apply */") %}
    {% endif %}

    {{ statements | join(';\n') }}
{% endmacro %}
