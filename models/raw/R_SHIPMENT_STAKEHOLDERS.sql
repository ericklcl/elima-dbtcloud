{{
  config(
    materialized='incremental',
    unique_key= ['SHIPMENT_ID', 'PARTY_ROLE', '_STAGE_ID'],
    incremental_strategy='merge',
    post_hook="{{ apply_table_and_column_comments(this) }}"
  )
}}

WITH STAGED AS (
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
)
SELECT
    SRC.SHIPMENT_ID,
    SRC.PARTY_ROLE,
    SRC.PARTY_NAME,
    SRC._SYSTEM_ID,
    SRC._STAGE_ID,
    SRC._META_FILENAME,
    SRC._META_ROW_NUMBER,
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
      and SRC._STAGE_ID = BASE._STAGE_ID
)
{% endif %}