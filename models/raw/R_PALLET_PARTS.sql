{{ config(
    materialized = 'incremental',
    unique_key = ['STOP_ID', 'PALLET_SEQ', 'PART_SEQ', '_META_FILENAME'],
    schema = 'RAW',
    incremental_strategy = 'merge',
    post_hook="{{ apply_table_and_column_comments(this) }}"
) }}

WITH PALLETS AS (
    SELECT *
    FROM {{ ref('R_STOP_PALLETS') }}   -- your pallet model
),

PARTS AS (
    SELECT
        p.STOP_ID,
        p.P_IDX AS PALLET_SEQ,
        part.value AS PART_OBJ,
        part.seq AS RAW_PART_SEQ,        
        p._SYSTEM_ID,
        p._META_FILENAME,
        p._META_INGESTION_TIMESTAMP,
        p._META_FILE_LAST_MODIFIED
    FROM PALLETS p,
         LATERAL FLATTEN(input => p.PALLET_OBJ:parts) part
    WHERE ARRAY_SIZE(OBJECT_KEYS(part.value)) > 0   
)

SELECT
    STOP_ID,
    PALLET_SEQ,
    ROW_NUMBER() OVER (
        PARTITION BY STOP_ID, PALLET_SEQ, _META_FILENAME
        ORDER BY RAW_PART_SEQ
    ) AS PART_SEQ,

    PART_OBJ:description::VARCHAR              AS DESCRIPTION,
    PART_OBJ:shipperPartNumber::VARCHAR        AS SHIPPER_PART_NUMBER,

    -- raw strings
    CASE WHEN PART_OBJ:quantity IS NULL THEN NULL 
         ELSE PART_OBJ:quantity::VARCHAR END   AS QUANTITY_RAW,

    TRY_TO_NUMBER(PART_OBJ:quantity::VARCHAR)  AS QUANTITY_NUM,

    CASE WHEN PART_OBJ:weight IS NULL THEN NULL 
         ELSE PART_OBJ:weight::VARCHAR END     AS WEIGHT_RAW,

    TRY_TO_NUMBER(PART_OBJ:weight::VARCHAR)    AS WEIGHT_NUM,

    _SYSTEM_ID,
    _META_FILENAME,
    _META_INGESTION_TIMESTAMP,
    _META_FILE_LAST_MODIFIED

FROM PARTS

{% if is_incremental() %}
WHERE NOT EXISTS (
    SELECT 1
    FROM {{ this }} BASE
    WHERE BASE.STOP_ID = PARTS.STOP_ID
      AND BASE.PALLET_SEQ = PARTS.P_IDX
      AND BASE._META_FILENAME = PARTS._META_FILENAME
      AND BASE._META_FILE_LAST_MODIFIED = PARTS._META_FILE_LAST_MODIFIED
)
{% endif %}
