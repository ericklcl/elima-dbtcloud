{{ config(
    materialized = 'incremental',
    unique_key = 'SHIPMENT_ID',
    schema = 'STAGE',
    incremental_strategy = 'merge',
    merge_update_columns = 'all',
    post_hook = "{{ apply_table_and_column_comments(this) }}"
) }}

WITH BASE AS (
    SELECT
        SHIPMENT_ID,
        LOAD_NUMBER,
        UPPER(TRIM(STATUS)) AS STATUS,
        SCAC,

        TRY_TO_NUMBER(TOTAL_DISTANCE_M) AS TOTAL_DISTANCE_M,
        TRY_TO_NUMBER(REMAINING_DISTANCE_M) AS REMAINING_DISTANCE_M,

        FOURKITES_ETA_TZ::timestamp_tz AS FOURKITES_ETA_TZ,
        DELETED_AT_TZ::timestamp_tz AS DELETED_AT_TZ,

        TIME_ZONE,
        NUM_DELIVERY_STOPS,

        DELETED::boolean AS IS_DELETED,
        DELETED_BY,

        _SYSTEM_ID,
        _STAGE_ID,
        _META_FILENAME,
        _META_ROW_NUMBER,
        _META_FILE_LAST_MODIFIED::timestamp AS _META_FILE_LAST_MODIFIED,
        _META_INGESTION_TIMESTAMP::timestamp AS _META_INGESTION_TIMESTAMP,
        _META_ROW_HASH,

        CURRENT_TIMESTAMP() AS _PROCESSED_AT

    FROM {{ ref('R_SHIPMENTS') }}
),

-- Pick the latest record per SHIPMENT_ID
DEDUP AS (
    SELECT *
    FROM BASE
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY SHIPMENT_ID
        ORDER BY COALESCE(_META_FILE_LAST_MODIFIED, _META_INGESTION_TIMESTAMP) DESC
    ) = 1
)

SELECT *
FROM DEDUP

{% if is_incremental() %}
WHERE _META_FILE_LAST_MODIFIED > (
    SELECT COALESCE(MAX(_META_FILE_LAST_MODIFIED), '1900-01-01'::timestamp)
    FROM {{ this }}
)
{% endif %}
