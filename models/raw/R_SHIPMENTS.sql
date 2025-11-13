{{
  config(
    materialized='incremental',
    unique_key=['SHIPMENT_ID', '_STAGE_ID'],
    incremental_strategy='merge'
  )
}}

WITH STAGED AS (
select 
    $1:fourKitesShipmentID::varchar as SHIPMENT_ID,
    {{ strip_leading_zeros_if_numeric("$1:loadNumber::varchar") }} as LOAD_NUMBER,
    $1:status::varchar as STATUS,
    $1:SCAC::varchar as SCAC,
    $1:totalDistanceInMeters::number as TOTAL_DISTANCE_M,
    $1:remainingDistanceInMeters::number as REMAINING_DISTANCE_M,
    to_timestamp_tz($1:fourKitesETA::varchar) as FOURKITES_ETA_TZ,
    $1:timeZone::varchar as TIME_ZONE,
    $1:numberOfDeliveryStops::number as NUM_DELIVERY_STOPS,
    $1:deleted::boolean as DELETED,
    $1:deletedBy::varchar as DELETED_BY,
    to_timestamp_tz($1:deletedAt::varchar) as DELETED_AT_TZ,

    -- Metadata
    _SYSTEM_ID,
    _STAGE_ID,
    _META_FILENAME,
    _META_ROW_NUMBER,
    _META_FILE_LAST_MODIFIED,
    _META_INGESTION_TIMESTAMP,

    -- Row Hash
    md5(to_json($1)) as _META_ROW_HASH

from {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }} as SRC
)
select
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
    SRC._META_FILE_LAST_MODIFIED,
    SRC._META_INGESTION_TIMESTAMP,
    SRC._META_ROW_HASH
FROM STAGED AS SRC
{% if is_incremental() %}
where NOT EXISTS (
    select 1
    from {{ this }} as BASE
    where  SRC._META_FILENAME = BASE._META_FILENAME
      and SRC._META_FILE_LAST_MODIFIED = BASE._META_FILE_LAST_MODIFIED
      and SRC.SHIPMENT_ID = BASE.SHIPMENT_ID
)
{% endif %}
