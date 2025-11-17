{{ config(
    materialized = 'table',
    schema = 'RAW'
) }}

-- Static lookup table for identifier types
WITH data AS (
    SELECT *
    FROM VALUES
        ('PO',       'Purchase Order'),
        ('SO',       'Sales Order'),
        ('CN',       'Consignment/Contract'),
        ('VT',       'Material/Vehicle Type'),
        ('SI',       'Shipping Instruction'),
        ('F8',       'Freight Reference'),
        ('FREEFORM', 'Unstructured line with no prefix'),
        ('UNKNOWN',  'Could not parse prefix')
    AS t(IDENTIFIER_TYPE, DESCRIPTION)
)

SELECT *
FROM data;
