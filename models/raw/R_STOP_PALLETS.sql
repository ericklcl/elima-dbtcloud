{{ config(
    materialized = 'incremental',
    unique_key = ['STOP_ID', '_STAGE_ID', '_META_FILENAME'],
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
        MD5(TO_JSON($1)) AS _META_ROW_HASH
    FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }}
),

-- Expand stops
STOPS AS (
    SELECT
        f_stop.value                               AS STOP_OBJ,
        f_stop.value:fourKitesStopID::VARCHAR      AS STOP_ID,
        SRC._SYSTEM_ID,
        SRC._STAGE_ID,
        SRC._META_FILENAME,
        SRC._META_ROW_NUMBER,
        SRC._META_FILE_LAST_MODIFIED,
        SRC._META_INGESTION_TIMESTAMP,
        SRC._META_ROW_HASH        
    FROM SRC,
         LATERAL FLATTEN(input => PAYLOAD:stops) f_stop
),

-- Expand pallets
PALLETS AS (
    SELECT
        s.STOP_ID,        
        s.STOP_OBJ,
        pallet.value                                AS PALLET_OBJ,
        pallet.seq                                  AS PALLET_SEQ,
        s._SYSTEM_ID,
        s._STAGE_ID,
        s._META_FILENAME,
        s._META_ROW_NUMBER,
        s._META_FILE_LAST_MODIFIED,
        s._META_INGESTION_TIMESTAMP,
        s._META_ROW_HASH
    FROM STOPS s,
         LATERAL FLATTEN(input => s.STOP_OBJ:pallets) pallet
    WHERE ARRAY_SIZE(OBJECT_KEYS(pallet.value)) > 0  
)

SELECT
    STOP_ID,    
    ROW_NUMBER() OVER (
        PARTITION BY STOP_ID, _META_FILENAME
        ORDER BY PALLET_SEQ
    ) AS PALLET_SEQ,

    PALLETS._SYSTEM_ID,
    PALLETS._STAGE_ID,
    PALLETS._META_FILENAME,
    PALLETS._META_ROW_NUMBER,
    PALLETS._META_FILE_LAST_MODIFIED,
    PALLETS._META_INGESTION_TIMESTAMP,

    PALLETS._META_ROW_HASH
FROM PALLETS

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS BASE
    WHERE 
        BASE.STOP_ID = PALLETS.STOP_ID
        AND BASE._STAGE_ID = PALLETS._STAGE_ID
        AND BASE._META_FILENAME = PALLETS._META_FILENAME
        AND BASE._META_FILE_LAST_MODIFIED = PALLETS._META_FILE_LAST_MODIFIED
)
{% endif %}