-- sql/indexes.sql
-- Performance indexes and constraints for the data pipeline

-- ====================================================
-- Staging Table Indexes
-- ====================================================

-- Common Crawl Indexes
CREATE INDEX IF NOT EXISTS idx_cc_company_name ON stg_common_crawl(scraped_company_name);
CREATE INDEX IF NOT EXISTS idx_cc_industry ON stg_common_crawl(scraped_industry);
CREATE INDEX IF NOT EXISTS idx_cc_loaded_at ON stg_common_crawl(loaded_at);

-- ABR Indexes
CREATE INDEX IF NOT EXISTS idx_abr_state ON stg_abr(state);
CREATE INDEX IF NOT EXISTS idx_abr_postcode ON stg_abr(postcode);
CREATE INDEX IF NOT EXISTS idx_abr_entity_type ON stg_abr(entity_type);
CREATE INDEX IF NOT EXISTS idx_abr_entity_status ON stg_abr(entity_status);
CREATE INDEX IF NOT EXISTS idx_abr_full_address_gin ON stg_abr USING gin(to_tsvector('english', COALESCE(full_address, '')));

-- ====================================================
-- Matching Table Indexes
-- ====================================================

-- Match lookup indexes
CREATE INDEX IF NOT EXISTS idx_matched_cc_id ON stg_matched_companies(cc_id);
CREATE INDEX IF NOT EXISTS idx_matched_abn ON stg_matched_companies(abn);
CREATE INDEX IF NOT EXISTS idx_matched_method ON stg_matched_companies(match_method);
CREATE INDEX IF NOT EXISTS idx_matched_confidence ON stg_matched_companies(match_confidence);
CREATE INDEX IF NOT EXISTS idx_matched_created_at ON stg_matched_companies(created_at);

-- Composite index for common queries
CREATE INDEX IF NOT EXISTS idx_matched_method_confidence ON stg_matched_companies(match_method, match_confidence DESC);

-- ====================================================
-- Final Dimension Table Indexes
-- ====================================================

-- Company lookup indexes
CREATE INDEX IF NOT EXISTS idx_dim_companies_name ON dim_companies(company_name);
CREATE INDEX IF NOT EXISTS idx_dim_companies_website ON dim_companies(website_url);
CREATE INDEX IF NOT EXISTS idx_dim_companies_state ON dim_companies(state);
CREATE INDEX IF NOT EXISTS idx_dim_companies_source ON dim_companies(data_source);
CREATE INDEX IF NOT EXISTS idx_dim_companies_status ON dim_companies(entity_status);

-- Full-text search on company names
CREATE INDEX IF NOT EXISTS idx_dim_companies_name_gin ON dim_companies USING gin(to_tsvector('english', COALESCE(company_name, '')));

-- Composite index for filtered queries
CREATE INDEX IF NOT EXISTS idx_dim_companies_source_status ON dim_companies(data_source, entity_status) 
    WHERE entity_status = 'Active';

-- ====================================================
-- Analyze tables for query optimizer
-- ====================================================

ANALYZE stg_abr;
ANALYZE stg_common_crawl;
ANALYZE stg_matched_companies;
ANALYZE dim_companies;
