{#
    Merges data from external stage into target table with metadata tracking.
    
    Updates existing files if modified timestamp changed, inserts new files.
    Optionally truncates target table before merge operation.
    
    Parameters:
    - target_table: Destination table name
    - stage_path: Snowflake external stage path 
    - file_format: File format name for parsing
    - system_id: Source system identifier (default: "FOURKITES")
    - stage_id: Processing stage identifier (default: "ACTIVE")
    - truncate_before_merge: Clear table before merge (default: False)
#}
{% macro merge_stage_into_table(
    target_table,
    stage_path,
    file_format,
    system_id="FOURKITES",
    stage_id="ACTIVE",
    truncate_before_merge=False
) %}
    {{ log("🟦 [merge_stage_into_table] Starting process for: " ~ target_table, info=True) }}
    {{ log("📂 Stage: " ~ stage_path ~ " | Format: " ~ file_format, info=True) }}
    {{ log("🔖 System ID: " ~ system_id ~ " | Stage ID: " ~ stage_id, info=True) }}
    {{ log("🧹 Truncate before merge: " ~ truncate_before_merge, info=True) }}
    {{ log("------------------------------------------------------------", info=True) }}

    {% set start_time = modules.datetime.datetime.now() %}

    {# Truncate table if requested #}
    {% if truncate_before_merge %}
        {{ log("🚨 Truncating table before merge: " ~ target_table, info=True) }}
        {% set truncate_sql %} TRUNCATE TABLE {{ target_table }}; {% endset %}
        {{ run_query(truncate_sql) }}
    {% endif %}

    {% set merge_sql %}
        MERGE INTO {{ target_table }} AS T
        USING (
            SELECT
                $1 AS RAW_PAYLOAD,
                '{{ system_id }}' AS _SYSTEM_ID,
                '{{ stage_id }}' AS _STAGE_ID,
                METADATA$FILENAME AS _META_FILENAME,
                METADATA$FILE_ROW_NUMBER::NUMBER AS _META_ROW_NUMBER,
                METADATA$FILE_LAST_MODIFIED AS _META_FILE_LAST_MODIFIED,
                CURRENT_TIMESTAMP() AS _META_INGESTION_TIMESTAMP
            FROM {{ stage_path }}
            (FILE_FORMAT => '{{ file_format }}')
        ) AS S
        ON T._META_FILENAME = S._META_FILENAME
        
        -- Update if file exists but timestamp changed
        WHEN MATCHED 
         AND T._META_FILE_LAST_MODIFIED <> S._META_FILE_LAST_MODIFIED THEN
          UPDATE SET
            PAYLOAD = S.RAW_PAYLOAD,
            _SYSTEM_ID = S._SYSTEM_ID,
            _STAGE_ID = S._STAGE_ID,
            _META_ROW_NUMBER = S._META_ROW_NUMBER,
            _META_FILE_LAST_MODIFIED = S._META_FILE_LAST_MODIFIED,
            _META_INGESTION_TIMESTAMP = S._META_INGESTION_TIMESTAMP

        -- Insert new files
        WHEN NOT MATCHED THEN
          INSERT (
            PAYLOAD,
            _SYSTEM_ID,
            _STAGE_ID,
            _META_FILENAME,
            _META_ROW_NUMBER,
            _META_FILE_LAST_MODIFIED,
            _META_INGESTION_TIMESTAMP
          )
          VALUES (
            S.RAW_PAYLOAD,
            S._SYSTEM_ID,
            S._STAGE_ID,
            S._META_FILENAME,
            S._META_ROW_NUMBER,
            S._META_FILE_LAST_MODIFIED,
            S._META_INGESTION_TIMESTAMP
          );
    {% endset %}

    {# Execute the merge operation #}
    {{ log("⚙️ Executing MERGE statement...", info=True) }}
    {% set merge_result = run_query(merge_sql) %}

{% endmacro %}
