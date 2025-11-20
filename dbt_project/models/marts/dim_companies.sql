{{
    config(
        materialized='table',
        unique_key='company_id'
    )
}}

-- Final unified company dimension combining ABR and CC data
WITH matched AS (
    SELECT
        abn,
        COALESCE(cc_company_name, abr_entity_name) as company_name,
        website_url,
        cc_industry as industry,
        full_address,
        state,
        postcode,
        entity_type,
        entity_status,
        start_date,
        'BOTH' as data_source,
        match_confidence,
        matched_at as created_at
    FROM {{ ref('int_matched_enriched') }}
),

-- ABR-only companies (not found in Common Crawl)
abr_only AS (
    SELECT
        abn,
        entity_name as company_name,
        NULL as website_url,
        NULL as industry,
        full_address,
        state,
        postcode,
        entity_type,
        entity_status,
        start_date,
        'ABR' as data_source,
        NULL as match_confidence,
        loaded_at as created_at
    FROM {{ ref('stg_abr_clean') }}
    WHERE abn NOT IN (SELECT abn FROM matched)
),

-- CC-only companies (couldn't match to ABR)
cc_only AS (
    SELECT
        abn,
        company_name,
        website_url,
        industry,
        NULL as full_address,
        NULL as state,
        NULL as postcode,
        NULL as entity_type,
        NULL as entity_status,
        NULL::DATE as start_date,
        'CC' as data_source,
        NULL as match_confidence,
        loaded_at as created_at
    FROM {{ ref('stg_cc_clean') }}
    WHERE cc_id NOT IN (SELECT cc_id FROM {{ source('raw', 'stg_matched_companies') }})
),

-- Combine all sources
combined AS (
    SELECT * FROM matched
    UNION ALL
    SELECT * FROM abr_only
    UNION ALL
    SELECT * FROM cc_only
)

SELECT
    ROW_NUMBER() OVER (ORDER BY data_source DESC, abn) as company_id,
    abn,
    company_name,
    website_url,
    industry,
    full_address,
    state,
    postcode,
    entity_type,
    entity_status,
    start_date,
    data_source,
    match_confidence,
    created_at,
    CURRENT_TIMESTAMP as updated_at
FROM combined
