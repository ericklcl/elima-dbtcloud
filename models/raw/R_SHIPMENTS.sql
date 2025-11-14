{{ config(
    materialized = 'incremental',
    unique_key = ['SHIPMENT_ID', '_STAGE_ID'],
    incremental_strategy = 'merge',
    post_hook = "{{ apply_table_and_column_comments(this) }}"
) }}

WITH STAGED AS (

    SELECT
        -- Business Fields
        $1:fourKitesShipmentID::varchar AS SHIPMENT_ID,
        {{ strip_leading_zeros_if_numeric("$1:loadNumber::varchar") }} AS LOAD_NUMBER,
        $1:status::varchar AS STATUS,
        $1:SCAC::varchar AS SCAC,
        $1:totalDistanceInMeters::number AS TOTAL_DISTANCE_M,
        $1:remainingDistanceInMeters::number AS REMAINING_DISTANCE_M,
        TO_TIMESTAMP_TZ($1:fourKitesETA::varchar) AS FOURKITES_ETA_TZ,
        $1:timeZone::varchar AS TIME_ZONE,
        $1:numberOfDeliveryStops::number AS NUM_DELIVERY_STOPS,
        $1:deleted::boolean AS DELETED,
        $1:deletedBy::varchar AS DELETED_BY,
        TO_TIMESTAMP_TZ($1:deletedAt::varchar) AS DELETED_AT_TZ,

        -- Metadata
        _SYSTEM_ID,
        _STAGE_ID,
        _META_FILENAME,
        _META_ROW_NUMBER,
        _META_FILE_LAST_MODIFIED,
        _META_INGESTION_TIMESTAMP,

        -- Row Hash
        MD5(TO_JSON($1)) AS _META_ROW_HASH

    FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }} AS SRC
)

SELECT
    SRC.SHIPMENT_ID,
    SRC.LOAD_NUMBER,
    SRC.STATUS,
    SRC.SCAC,
    SRC.TOTAL_DISTANCE_M,
    SRC.REMAINING_DISTANCE_M,
    SRC.FOURKITES_ETA_TZ,
    SRC.TIME_ZONE,
    SRC.NUM_DELIVERY_STOPS,
    SRC.DELETED,
    SRC.DELETED_BY,
    SRC.DELETED_AT_TZ,
    SRC._SYSTEM_ID,
    SRC._STAGE_ID,
    SRC._META_FILENAME,
    SRC._META_ROW_NUMBER,
    SRC._META_FILE_LAST_MODIFIED,
    SRC._META_INGESTION_TIMESTAMP,
    SRC._META_ROW_HASH

FROM STAGED AS SRC

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS BASE
    WHERE SRC._META_FILENAME = BASE._META_FILENAME
      AND SRC._META_FILE_LAST_MODIFIED = BASE._META_FILE_LAST_MODIFIED
      AND SRC.SHIPMENT_ID = BASE.SHIPMENT_ID
      AND SRC._STAGE_ID = BASE._STAGE_ID
)
{% endif %}
