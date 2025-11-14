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
        
        -- 🔄 UPDATE se o arquivo for o mesmo mas mudou o timestamp
        WHEN MATCHED 
         AND T._META_FILE_LAST_MODIFIED <> S._META_FILE_LAST_MODIFIED THEN
          UPDATE SET
            PAYLOAD = S.RAW_PAYLOAD,
            _SYSTEM_ID = S._SYSTEM_ID,
            _STAGE_ID = S._STAGE_ID,
            _META_ROW_NUMBER = S._META_ROW_NUMBER,
            _META_FILE_LAST_MODIFIED = S._META_FILE_LAST_MODIFIED,
            _META_INGESTION_TIMESTAMP = S._META_INGESTION_TIMESTAMP

        -- 🆕 INSERT se o arquivo não existe
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

{% endmacro %}
