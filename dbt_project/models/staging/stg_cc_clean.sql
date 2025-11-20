{{
    config(
        materialized='view'
    )
}}

-- Clean and standardize Common Crawl data
SELECT
    id as cc_id,
    website_url,
    TRIM(scraped_company_name) as company_name,
    TRIM(scraped_industry) as industry,
    scraped_abn as abn,
    html_content_snippet,
    loaded_at
FROM {{ source('raw', 'stg_common_crawl') }}
WHERE website_url IS NOT NULL
AND (scraped_company_name IS NOT NULL OR scraped_abn IS NOT NULL)
