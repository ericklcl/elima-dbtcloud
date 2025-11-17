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

        
    FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }}
)
SELECT
    f.VALUE:fourKitesStopID::VARCHAR AS STOP_ID,
    SRC.PAYLOAD:fourKitesShipmentID::VARCHAR AS SHIPMENT_ID,    
    {{ normalize_stop_reference_id("f.VALUE:stopReferenceId::VARCHAR") }} AS STOP_REFERENCE_ID,
    f.VALUE:stopType::VARCHAR AS STOP_TYPE,
    f.VALUE:status::VARCHAR AS STATUS,
    f.VALUE:stopName::VARCHAR AS STOP_NAME,
    f.VALUE:city::VARCHAR AS CITY,
    f.VALUE:state::VARCHAR AS STATE,
    f.VALUE:country::VARCHAR AS COUNTRY,    
     {{ strip_leading_zeros_if_numeric("f.VALUE:externalAddressID::VARCHAR") }} AS EXTERNAL_ADDRESS_ID,
    f.VALUE:latitude::FLOAT AS LATITUDE,
    f.VALUE:longitude::FLOAT AS LONGITUDE,
    f.VALUE:timeZone::VARCHAR AS TIME_ZONE,
    f.VALUE:sequence AS SEQUENCE_NUM,
    TO_TIMESTAMP_TZ(f.VALUE:arrivalTime::VARCHAR) AS ARRIVAL_TIME_TZ,
    TO_TIMESTAMP_TZ(f.VALUE:departureTime::VARCHAR) AS DEPARTURE_TIME_TZ,
    f.VALUE:postalCode::VARCHAR AS POSTAL_CODE,
    f.VALUE:deleted::BOOLEAN AS DELETED,
    f.VALUE:deletedBy::VARCHAR AS DELETED_BY,
    TO_TIMESTAMP_TZ(f.VALUE:deletedAt::VARCHAR) AS DELETED_AT_TZ,
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
