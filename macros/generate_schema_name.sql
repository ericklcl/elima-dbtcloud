{#
    Overrides dbt's default schema naming behavior.
    
    Uses custom schema name directly instead of concatenating with target schema.
    Returns target schema if no custom schema specified.
    
    dbt calls this macro automatically when models specify custom_schema.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}