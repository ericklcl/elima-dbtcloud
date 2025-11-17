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

    {# Optional truncate #}
    {% if truncate_before_merge %}
        {{ log("🚨 Truncating table before merge: " ~ target_table, info=True) }}
        {% set truncate_sql %} TRUNCATE TABLE {{ target_table }}; {% endset %}
        {{ run_query(truncate_sql) }}
    {% endif %}

    {# MERGE STATEMENT #}
    {% set merge_sql %}

        MERGE INTO {{ target_table }} AS T
        USING (

            SELECT *
            FROM (
                SELECT
                    $1 AS RAW_PAYLOAD,
                    '{{ system_id }}' AS _SYSTEM_ID,
                    '{{ stage_id }}' AS _STAGE_ID,
                    METADATA$FILENAME AS _META_FILENAME,
                    METADATA$FILE_ROW_NUMBER::NUMBER AS _META_ROW_NUMBER,
                    METADATA$FILE_LAST_MODIFIED AS _META_FILE_LAST_MODIFIED,
                    CURRENT_TIMESTAMP() AS _META_INGESTION_TIMESTAMP,

                    -- ⭐ One row per file AND per unique payload
                    ROW_NUMBER() OVER (
                        PARTITION BY METADATA$FILENAME, $1
                        ORDER BY METADATA$FILE_ROW_NUMBER
                    ) AS FILE_PAYLOAD_RANK

                FROM {{ stage_path }}
                     (FILE_FORMAT => '{{ file_format }}')
            )
            WHERE FILE_PAYLOAD_RANK = 1   -- ⭐ KEEP ONLY UNIQUE PAYLOAD PER FILE

        ) AS S

        ON T._META_FILENAME = S._META_FILENAME

        -- Update if file exists but timestamp changed
        WHEN MATCHED 
         AND T._META_FILE_LAST_MODIFIED <> S._META_FILE_LAST_MODIFIED THEN
          UPDATE SET
            PAYLOAD                   = S.RAW_PAYLOAD,
            _SYSTEM_ID                = S._SYSTEM_ID,
            _STAGE_ID                 = S._STAGE_ID,
            _META_ROW_NUMBER          = S._META_ROW_NUMBER,
            _META_FILE_LAST_MODIFIED  = S._META_FILE_LAST_MODIFIED,
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

    {{ log("⚙️ Executing MERGE statement...", info=True) }}
    {% set merge_result = run_query(merge_sql) %}

{% endmacro %}