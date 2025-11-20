{{
    config(
        materialized='view'
    )
}}

-- Clean and standardize ABR data
SELECT
    abn,
    UPPER(TRIM(entity_name)) as entity_name,
    entity_type,
    entity_status,
    CONCAT_WS(', ', 
        NULLIF(TRIM(address_line_1), ''),
        NULLIF(TRIM(address_line_2), ''),
        NULLIF(TRIM(state), ''),
        NULLIF(TRIM(postcode), '')
    ) as full_address,
    UPPER(TRIM(state)) as state,
    postcode,
    start_date,
    loaded_at
FROM {{ source('raw', 'stg_abr') }}
WHERE entity_status = 'Active'
AND abn IS NOT NULL
AND entity_name IS NOT NULL
