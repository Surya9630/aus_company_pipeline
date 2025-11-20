{{
    config(
        materialized='view'
    )
}}

-- Combine all matched companies with enriched data from both sources
SELECT
    m.match_id,
    m.cc_id,
    m.abn,
    m.match_method,
    m.match_confidence,
    m.match_reasoning,
    
    -- Common Crawl fields
    cc.website_url,
    cc.company_name as cc_company_name,
    cc.industry as cc_industry,
    
    -- ABR fields
    abr.entity_name as abr_entity_name,
    abr.entity_type,
    abr.entity_status,
    abr.full_address,
    abr.state,
    abr.postcode,
    abr.start_date,
    
    m.created_at as matched_at
    
FROM {{ source('raw', 'stg_matched_companies') }} m
INNER JOIN {{ ref('stg_cc_clean') }} cc ON m.cc_id = cc.cc_id
INNER JOIN {{ ref('stg_abr_clean') }} abr ON m.abn = abr.abn
