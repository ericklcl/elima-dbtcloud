{#
    Applies table and column comments from YAML schema to Snowflake objects.
    
    Reads model descriptions from dbt graph and generates COMMENT ON statements.
    Designed for use in post-hooks to automatically document tables after creation.
    
    Usage: post_hook="{{ apply_table_and_column_comments(this) }}"
#}
{% macro apply_table_and_column_comments(model) %}
    
    {# Find the model node in dbt graph for post-hook context #}
    {% set model_node = none %}
    {% set model_unique_id = 'model.' ~ project_name ~ '.' ~ model.name %}
    
    {% if graph is defined and graph.nodes is defined %}
        {% set model_node = graph.nodes.get(model_unique_id) %}
    {% endif %}
    
    {% set statements = [] %}

    {# Generate table comment from YAML description #}
    {% if model_node and model_node.description %}
        {% set table_comment %}
COMMENT ON TABLE {{ model.database }}.{{ model.schema }}.{{ model.name }} 
    IS '{{ model_node.description | replace("'", "''") }}'
        {% endset %}
        {% do statements.append(table_comment) %}
    {% endif %}

    {# Generate column comments from YAML descriptions #}
    {% if model_node and model_node.columns %}
        {% for column_name, column_info in model_node.columns.items() %}
            {% if column_info.description %}
                {% set col_comment %}
COMMENT ON COLUMN {{ model.database }}.{{ model.schema }}.{{ model.name }}.{{ column_name }}
    IS '{{ column_info.description | replace("'", "''") }}'
                {% endset %}
                {% do statements.append(col_comment) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {# Debug logging in debug mode #}
    {% if flags.DEBUG %}
        {% if model_node %}
            {% do log("Found model node for " ~ model.name ~ " with " ~ (model_node.columns | length) ~ " columns", info=true) %}
        {% else %}
            {% do log("No model node found for " ~ model.name ~ " (unique_id: " ~ model_unique_id ~ ")", info=true) %}
        {% endif %}
    {% endif %}

    {# Fallback for empty statements #}
    {% if statements | length == 0 %}
        {% set fallback %}
SELECT 1 /* No comments defined for {{ model.name }} */
        {% endset %}
        {% do statements.append(fallback) %}
    {% endif %}

    {{ statements | join(';\n') }}

{% endmacro %}
