{% macro merge_stage_into_table(
    target_table,
    stage_path,
    file_format,
    system_id="FOURKITES",
    stage_id="ACTIVE",
    truncate_before_merge=False
) %}
    {#---------------------------------------------
      Macro: merge_stage_into_table
      Description:
        Dynamically merges staged JSON/CSV data into a target Snowflake table,
        avoiding duplicates by comparing metadata fields.
        Supports optional truncation and logs execution details.
      Params:
        - target_table: fully qualified table (e.g. RAW.R_FOURKITES_JSON_PAYLOAD)
        - stage_path: stage and subpath (e.g. @RAW.STG_FOURKITES_S3/active)
        - file_format: file format name (e.g. RAW.FF_JSON_FOURKITES)
        - system_id: source system tag (default: FOURKITES)
        - stage_id: stage identifier (ACTIVE, ARCHIVE, etc.)
        - truncate_before_merge: boolean to truncate target table before merging
    ----------------------------------------------#}
    {{
        log(
            "🟦 [merge_stage_into_table] Starting process for: " ~ target_table,
            info=True,
        )
    }}
    {{ log("📂 Stage: " ~ stage_path ~ " | Format: " ~ file_format, info=True) }}
    {{ log("🔖 System ID: " ~ system_id ~ " | Stage ID: " ~ stage_id, info=True) }}
    {{ log("🧹 Truncate before merge: " ~ truncate_before_merge, info=True) }}
    {{ log("------------------------------------------------------------", info=True) }}

    {% set start_time = modules.datetime.datetime.now() %}

    {% if truncate_before_merge %}
        {{ log("🚨 Truncating table before merge: " ~ target_table, info=True) }}
        {% set truncate_sql %}
            TRUNCATE TABLE {{ target_table }};
        {% endset %}
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
           AND T._META_ROW_NUMBER = S._META_ROW_NUMBER

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

    {{ log("⚙️ Executing MERGE statement...", info=True) }}
    {% set merge_result = run_query(merge_sql) %}

    {# Count inserted rows from the previous statement #}
    {% set count_sql %}
        SELECT ROWS_INSERTED
        FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
        WHERE ROWS_INSERTED IS NOT NULL
        LIMIT 1;
    {% endset %}

    {% set count_result = run_query(count_sql) %}
    {% set inserted_count = 0 %}
    {% if count_result and count_result.columns[0].values()[0] is not none %}
        {% set inserted_count = count_result.columns[0].values()[0] %}
    {% endif %}

    {% set end_time = modules.datetime.datetime.now() %}
    {% set duration = (end_time - start_time).total_seconds() %}

    {{ log("✅ Merge completed successfully for " ~ target_table, info=True) }}
    {{ log("📊 Rows inserted: " ~ inserted_count, info=True) }}
    {{ log("⏱️ Duration: " ~ duration ~ " seconds", info=True) }}
    {{ log("------------------------------------------------------------", info=True) }}

{% endmacro %}
