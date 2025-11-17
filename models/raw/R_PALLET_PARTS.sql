{{ config(
    materialized = 'incremental',
    unique_key = ['STOP_ID', 'PALLET_SEQ', 'PART_SEQ', '_META_FILENAME'],
    schema = 'RAW',
    incremental_strategy = 'merge',
    post_hook="{{ apply_table_and_column_comments(this) }}"
) }}

-- 1) Read source table
WITH SRC AS (
    SELECT
        payload,
        _system_id,
        _stage_id,
        _meta_filename,
        _meta_row_number,
        _meta_file_last_modified,
        _meta_ingestion_timestamp,
        MD5(TO_JSON(payload)) AS _meta_row_hash
    FROM {{ source('RAW','R_FOURKITES_JSON_PAYLOAD') }}
),

-- 2) Flatten stops
STOPS AS (
    SELECT
        f_stop.value AS STOP_OBJ,
        f_stop.value:fourKitesStopID::varchar AS STOP_ID,

        SRC._system_id,
        SRC._stage_id,
        SRC._meta_filename,
        SRC._meta_row_number,
        SRC._meta_file_last_modified,
        SRC._meta_ingestion_timestamp,
        SRC._meta_row_hash

    FROM SRC,
         LATERAL FLATTEN(input => payload:stops) f_stop
),

-- 3) Flatten pallets
PALLETS AS (
    SELECT
        s.STOP_ID,
        s.STOP_OBJ,
        pallet.value AS PALLET_OBJ,

        ROW_NUMBER() OVER (
            PARTITION BY s.STOP_ID, s._meta_filename
            ORDER BY 1
        ) AS PALLET_SEQ_RAW,

        s._system_id,
        s._stage_id,
        s._meta_filename,
        s._meta_row_number,
        s._meta_file_last_modified,
        s._meta_ingestion_timestamp,
        s._meta_row_hash

    FROM STOPS s,
         LATERAL FLATTEN(input => TO_ARRAY(s.STOP_OBJ:pallets)) pallet
    WHERE ARRAY_SIZE(OBJECT_KEYS(pallet.value)) > 0
),

-- 4) Flatten parts
PARTS AS (
    SELECT
        p.STOP_ID,
        p.PALLET_OBJ,
        p.PALLET_SEQ_RAW,
        part.value AS PART_OBJ,

        ROW_NUMBER() OVER (
            PARTITION BY p.STOP_ID, p._meta_filename, p.PALLET_SEQ_RAW
            ORDER BY 1
        ) AS PART_SEQ_RAW,

        p._system_id,
        p._stage_id,
        p._meta_filename,
        p._meta_row_number,
        p._meta_file_last_modified,
        p._meta_ingestion_timestamp,
        p._meta_row_hash

    FROM PALLETS p,
         LATERAL FLATTEN(input => TO_ARRAY(p.PALLET_OBJ:parts)) part
    WHERE ARRAY_SIZE(OBJECT_KEYS(part.value)) > 0
)

-- 5) Final output
SELECT
    STOP_ID,

    PALLET_SEQ_RAW AS PALLET_SEQ,

    PART_SEQ_RAW AS PART_SEQ,

    PART_OBJ:description::varchar       AS DESCRIPTION,
    PART_OBJ:shipperPartNumber::varchar AS SHIPPER_PART_NUMBER,

    CASE WHEN PART_OBJ:quantity IS NULL THEN NULL
         ELSE PART_OBJ:quantity::varchar
    END AS QUANTITY_RAW,

    TRY_TO_NUMBER(PART_OBJ:quantity::varchar) AS QUANTITY_NUM,

    CASE WHEN PART_OBJ:weight IS NULL THEN NULL
         ELSE PART_OBJ:weight::varchar
    END AS WEIGHT_RAW,

    TRY_TO_NUMBER(PART_OBJ:weight::varchar) AS WEIGHT_NUM,

    -- ALL METADATA FIELDS:
    _system_id,
    _stage_id,
    _meta_filename,
    _meta_row_number,
    _meta_file_last_modified,
    _meta_ingestion_timestamp,
    _meta_row_hash

FROM PARTS