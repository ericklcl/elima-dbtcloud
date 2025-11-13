{{
  config(
    materialized='incremental',
    unique_key= ['SHIPMENT_ID', 'PARTY_ROLE'],
    incremental_strategy='merge'
  )
}}

SELECT    
    t.$1:fourKitesShipmentID::VARCHAR AS SHIPMENT_ID,
    f.KEY   AS PARTY_ROLE,
    f.VALUE AS PARTY_NAME,

    -- Metadata
    _SYSTEM_ID,
    _STAGE_ID,
    _META_FILENAME,
    _META_ROW_NUMBER,
    _META_FILE_LAST_MODIFIED,
    _META_INGESTION_TIMESTAMP,

    -- Hash Row
    md5(to_json(t.$1)) as _META_ROW_HASH
from {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }} AS t,
     LATERAL FLATTEN(INPUT => t.$1:stakeholders) f