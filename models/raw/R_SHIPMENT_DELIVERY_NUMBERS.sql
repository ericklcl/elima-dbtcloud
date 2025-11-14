{{ config(
    materialized = 'incremental',
    unique_key = ['SHIPMENT_ID', 'DELIVERY_NUMBER', '_STAGE_ID'],
    schema = 'RAW',
    incremental_strategy = 'merge',
    post_hook="{{ apply_table_and_column_comments(this) }}"
) }}

WITH STAGED AS (
    SELECT
        t.$1 AS PAYLOAD,

        -- Metadata
        t._SYSTEM_ID,
        t._STAGE_ID,
        t._META_FILENAME,
        t._META_ROW_NUMBER,
        t._META_FILE_LAST_MODIFIED,
        t._META_INGESTION_TIMESTAMP,
        
        -- Generate row hash from payload
        md5(to_json(t.$1)) as _META_ROW_HASH
    FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }} AS t
)
SELECT    
    SRC.PAYLOAD:fourKitesShipmentID::varchar AS SHIPMENT_ID,
    f.index + 1                             AS DELIVERY_SEQ,
    {{ strip_leading_zeros_if_numeric("f.value::string") }} AS DELIVERY_NUMBER,
    SRC.PAYLOAD:identifiers:purchaseOrderNumbers::variant AS PURCHASE_ORDER_NUMBERS,
    -- Metadata
    SRC._SYSTEM_ID,
    SRC._STAGE_ID,
    SRC._META_FILENAME,
    SRC._META_ROW_NUMBER,
    SRC._META_FILE_LAST_MODIFIED,
    SRC._META_INGESTION_TIMESTAMP,

    -- Row Hash
    SRC._META_ROW_HASH
FROM STAGED AS SRC,
LATERAL FLATTEN( input => SRC.PAYLOAD:identifiers:purchaseOrderNumbers ) f

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} AS BASE
    WHERE SRC._META_FILENAME = BASE._META_FILENAME
      AND SRC._META_FILE_LAST_MODIFIED = BASE._META_FILE_LAST_MODIFIED
      AND SRC.PAYLOAD:fourKitesShipmentID::varchar = BASE.SHIPMENT_ID
      AND SRC._STAGE_ID = BASE._STAGE_ID
      AND f.value::string = BASE.DELIVERY_NUMBER
)
{% endif %}