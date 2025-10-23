-- ═══════════════════════════════════════════════════════════════════
-- QUICK SETUP: Create input_stage for Streamlit App
-- ═══════════════════════════════════════════════════════════════════

-- Make sure to use the correct database and schema
USE DATABASE SUPERSTORE;
USE SCHEMA DEMO;

-- Method 1: Using Storage Integration (if you already have azure_int)
-- Check if integration exists
SHOW INTEGRATIONS LIKE 'azure_int';

-- If exists, create stage directly
CREATE OR REPLACE STAGE input_stage
  URL = 'azure://judodemoaest.blob.core.windows.net/construction-building-defects/'
  STORAGE_INTEGRATION = azure_int
  DIRECTORY = (ENABLE = TRUE);

-- Method 2: Using SAS Token (if you don't have integration)
-- Get SAS Token from Azure Portal, then run:
/*
CREATE OR REPLACE STAGE input_stage
  URL = 'azure://judodemoaest.blob.core.windows.net/construction-building-defects/'
  CREDENTIALS = (AZURE_SAS_TOKEN = '?sv=2022-11-02&ss=b&srt=sco&sp=rl&se=2026-12-31T23:59:59Z&st=2025-01-01T00:00:00Z&spr=https&sig=YOUR_SIGNATURE_HERE')
  DIRECTORY = (ENABLE = TRUE);
*/

-- ═══════════════════════════════════════════════════════════════════
-- Verify Stage Creation
-- ═══════════════════════════════════════════════════════════════════

-- 1. Show stage
SHOW STAGES LIKE 'input_stage';

-- 2. List files
LIST @input_stage;

-- 3. Refresh directory table
ALTER STAGE input_stage REFRESH;

-- 4. Query directory table
SELECT
    RELATIVE_PATH,
    SIZE/1024/1024 AS SIZE_MB,
    LAST_MODIFIED
FROM DIRECTORY(@input_stage)
ORDER BY LAST_MODIFIED DESC
LIMIT 10;

-- ═══════════════════════════════════════════════════════════════════
-- Test AI_COMPLETE (using actual file path)
-- ═══════════════════════════════════════════════════════════════════

-- First get an actual RELATIVE_PATH from the query above, then test:
/*
SELECT AI_COMPLETE(
    model => 'openai-gpt-4.1',
    prompt => PROMPT('Analyze this image: {0}',
                     TO_FILE('@input_stage', 'cls04_057.jpg'))  -- Replace with actual path
) AS result;
*/

-- ═══════════════════════════════════════════════════════════════════
-- If you need to create Storage Integration (first time only)
-- ═══════════════════════════════════════════════════════════════════
/*
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE STORAGE INTEGRATION azure_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'AZURE'
  ENABLED = TRUE
  AZURE_TENANT_ID = 'ef3b59df-4339-471f-b160-3ee6ce15c604'
  STORAGE_ALLOWED_LOCATIONS = ('azure://judodemoaest.blob.core.windows.net/construction-building-defects/');

-- Grant permissions
GRANT USAGE ON INTEGRATION azure_int TO ROLE ACCOUNTADMIN;

-- View integration configuration (need to configure this info in Azure)
DESC INTEGRATION azure_int;
*/
