{% macro apply_table_and_column_comments(model) %}
    
    {# Find the model node in the graph using the correct approach for post-hooks #}
    {% set model_node = none %}
    {% set model_unique_id = 'model.' ~ project_name ~ '.' ~ model.name %}
    
    {# Access the graph using the context that's available in post-hooks #}
    {% if graph is defined and graph.nodes is defined %}
        {% set model_node = graph.nodes.get(model_unique_id) %}
    {% endif %}
    
    {# Collect all comment statements here #}
    {% set statements = [] %}

    {# Apply table comment if available #}
    {% if model_node and model_node.description %}
        {% set table_comment %}
COMMENT ON TABLE {{ model.database }}.{{ model.schema }}.{{ model.name }} 
    IS '{{ model_node.description | replace("'", "''") }}'
        {% endset %}
        {% do statements.append(table_comment) %}
    {% endif %}

    {# Apply column comments if available #}
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

    {# Debug information (only in debug mode) #}
    {% if flags.DEBUG %}
        {% if model_node %}
            {% do log("Found model node for " ~ model.name ~ " with " ~ (model_node.columns | length) ~ " columns", info=true) %}
        {% else %}
            {% do log("No model node found for " ~ model.name ~ " (unique_id: " ~ model_unique_id ~ ")", info=true) %}
        {% endif %}
    {% endif %}

    {# Prevent empty SQL (Snowflake requires at least one statement) #}
    {% if statements | length == 0 %}
        {% set fallback %}
SELECT 1 /* No comments defined for {{ model.name }} */
        {% endset %}
        {% do statements.append(fallback) %}
    {% endif %}

    {{ statements | join(';\n') }}

{% endmacro %}
