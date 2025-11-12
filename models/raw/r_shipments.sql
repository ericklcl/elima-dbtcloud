{{ 
  config(
    materialized='incremental',
    unique_key=['SHIPMENT_ID', '_META_FILENAME', '_META_ROW_NUMBER'],
    schema='raw',
    incremental_strategy='merge'
  )
}}

select 
    $1:fourKitesShipmentID::varchar as SHIPMENT_ID,
    $1:loadNumber::varchar as LOAD_NUMBER,
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
    _SYSTEM_ID,
    _STAGE_ID,
    _META_FILENAME,
    _META_ROW_NUMBER,
    _META_FILE_LAST_MODIFIED,
    _META_INGESTION_TIMESTAMP
from {{ source('raw','r_fourkites_json_payload') }}
