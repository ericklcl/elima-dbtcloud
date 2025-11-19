{{ config(
    materialized='incremental',
    unique_key=['SHIPMENT_ID','REF_SEQ','_META_FILENAME'],
    schema = 'RAW',
    incremental_strategy = 'merge'
) }}

WITH BASE AS (
    SELECT
        t.PAYLOAD,
        t.PAYLOAD:fourKitesShipmentID::string AS SHIPMENT_ID,
        f.index + 1 AS REF_SEQ,
        f.value::string AS RAW_TEXT,
        t.PAYLOAD:stops AS STOPS,

        -- Metadata
        t._SYSTEM_ID,
        t._STAGE_ID,
        t._META_FILENAME,
        t._META_ROW_NUMBER,
        t._META_FILE_LAST_MODIFIED,
        t._META_INGESTION_TIMESTAMP,

        -- Row Hash
        md5(to_json(t.PAYLOAD)) as _META_ROW_HASH
    
    FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }} t
    , LATERAL FLATTEN(input => t.PAYLOAD:identifiers:referenceNumbers) f
),

STOP_IDS AS (
    SELECT
        b.SHIPMENT_ID,

        {{ strip_leading_zeros_if_numeric("st.value:externalAddressID::string") }} AS EXT_ID,
        {{ strip_leading_zeros_if_numeric("st.value:customer:ID::string") }} AS CUST_ID

    FROM BASE b,
    LATERAL FLATTEN(input => b.STOPS) st
),

IDS_UNIFIED AS (
    SELECT DISTINCT SHIPMENT_ID, ID
    FROM (
        SELECT SHIPMENT_ID, EXT_ID AS ID FROM STOP_IDS WHERE EXT_ID IS NOT NULL
        UNION
        SELECT SHIPMENT_ID, CUST_ID AS ID FROM STOP_IDS WHERE CUST_ID IS NOT NULL
    )
)

SELECT
    SHIPMENT_ID,
    REF_SEQ,
    RAW_TEXT,

    -- PREFIX (inline-safe version)
    {{ parse_identifier_prefix("RAW_TEXT") }} AS IDENTIFIER_TYPE,

    -- VALUE (inline-safe version)
    {{ parse_identifier_value("RAW_TEXT") }} AS IDENTIFIER_VALUE,

    -- FLAGS
    {{ is_note_like("RAW_TEXT") }} AS NOTE_FLAG,
    {{ is_party_like("RAW_TEXT") }} AS PARTY_FLAG,
    {{ is_phone_like("RAW_TEXT") }} AS PHONE_FLAG,

    -- STOP_HINT logic (Python-equivalent)
    CASE 
        WHEN TRIM(RAW_TEXT) IN (
            SELECT ID
            FROM IDS_UNIFIED
            WHERE SHIPMENT_ID = SRC.SHIPMENT_ID
        )
        THEN TRIM(RAW_TEXT)
        ELSE NULL
    END AS STOP_HINT,

    -- Metadata
    _SYSTEM_ID,
    _STAGE_ID,
    _META_FILENAME,
    _META_ROW_NUMBER,
    _META_FILE_LAST_MODIFIED,
    _META_INGESTION_TIMESTAMP,
    _META_ROW_HASH

FROM BASE AS SRC

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS TGT
    WHERE TGT._META_FILENAME = SRC._META_FILENAME
      AND TGT._META_FILE_LAST_MODIFIED = SRC._META_FILE_LAST_MODIFIED
      AND TGT.SHIPMENT_ID = SRC.SHIPMENT_ID
      AND TGT._STAGE_ID = SRC._STAGE_ID
)
{% endif %}
