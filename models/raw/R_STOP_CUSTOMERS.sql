{{ config(
    materialized = 'incremental',
    unique_key = ['STOP_ID', '_STAGE_ID'],
    schema = 'RAW',
    incremental_strategy = 'merge',
    post_hook="{{ apply_table_and_column_comments(this) }}"
) }}

WITH SRC AS (
    SELECT 
        $1 AS PAYLOAD,
        _SYSTEM_ID,
        _STAGE_ID,
        _META_FILENAME,
        _META_ROW_NUMBER,
        _META_FILE_LAST_MODIFIED,
        _META_INGESTION_TIMESTAMP,

        -- Row Hash
        MD5(TO_JSON($1)) AS _META_ROW_HASH

        
    FROM FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }}
)
SELECT
    f.VALUE:fourKitesStopID::VARCHAR AS STOP_ID,
    f.VALUE:customer:ID::VARCHAR AS CUSTOMER_ID,    
    SRC._SYSTEM_ID,
    SRC._STAGE_ID,
    SRC._META_FILENAME,
    SRC._META_ROW_NUMBER,
    SRC._META_FILE_LAST_MODIFIED,
    SRC._META_INGESTION_TIMESTAMP,

    SRC._META_ROW_HASH
FROM SRC,
     LATERAL FLATTEN(input => SRC.PAYLOAD:stops) f

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS BASE
    WHERE SRC._META_FILENAME = BASE._META_FILENAME
      AND SRC._META_FILE_LAST_MODIFIED = BASE._META_FILE_LAST_MODIFIED
      AND f.VALUE:fourKitesStopID = BASE.STOP_ID
      AND SRC._STAGE_ID = BASE._STAGE_ID      
)
{% endif %}
