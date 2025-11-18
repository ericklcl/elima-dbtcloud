{{ config(
    materialized = 'incremental',
    unique_key = ['SHIPMENT_ID', '_STAGE_ID', 'LOCATION_KIND'],
    incremental_strategy = 'merge',
    post_hook="{{ apply_table_and_column_comments(this) }}"
) }}

WITH SRC AS (
    SELECT 
        $1 AS PAYLOAD,
        'LATEST' as LOCATION_KIND,  

        -- Metadata
        _SYSTEM_ID,
        _STAGE_ID,
        _META_FILENAME,
        _META_ROW_NUMBER,
        _META_FILE_LAST_MODIFIED,
        _META_INGESTION_TIMESTAMP,

        -- Hash Row        
        md5(to_json($1)) as _META_ROW_HASH
    FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }}
    WHERE IS_OBJECT($1:latestLocation)
)
SELECT
    PAYLOAD:fourKitesShipmentID::varchar AS SHIPMENT_ID,
    LOCATION_KIND,    
    PAYLOAD:latestLocation:latitude::float AS LATITUDE,
    PAYLOAD:latestLocation:longitude::float AS LONGITUDE,
    PAYLOAD:latestLocation:name::varchar AS PLACE_NAME,
    PAYLOAD:latestLocation:updatedTime::timestamp_tz AS UPDATED_TIME_TZ,
    PAYLOAD:latestLocation:timeZone::varchar AS TIME_ZONE,
    
    -- Metadata
    _SYSTEM_ID,
    _STAGE_ID,
    _META_FILENAME,
    _META_ROW_NUMBER,
    _META_FILE_LAST_MODIFIED,
    _META_INGESTION_TIMESTAMP,

    -- Hash Row
    _META_ROW_HASH
FROM SRC

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS BASE
    WHERE SRC._META_FILENAME = BASE._META_FILENAME
      AND SRC._META_FILE_LAST_MODIFIED = BASE._META_FILE_LAST_MODIFIED
      AND SRC.PAYLOAD:fourKitesShipmentID::varchar = BASE.SHIPMENT_ID
      AND SRC._STAGE_ID = BASE._STAGE_ID
      AND SRC.LOCATION_KIND = BASE.LOCATION_KIND
)
{% endif %}