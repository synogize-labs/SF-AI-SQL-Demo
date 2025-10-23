-- ═══════════════════════════════════════════════════════════════════
-- Snowflake Streamlit Deployment Script
-- Construction Defect Analysis App
-- ═══════════════════════════════════════════════════════════════════

-- Step 1: Set context
USE ROLE ACCOUNTADMIN;
USE DATABASE SUPERSTORE;
USE SCHEMA DEMO;
USE WAREHOUSE COMPUTE_WH;

-- Step 2: Verify prerequisites
-- Check if input_stage exists (run QUICK_SETUP.sql if not)
SHOW STAGES LIKE 'input_stage';

-- Check if AI functions are available
SELECT SYSTEM$BOOTSTRAP_DATA_REQUEST();

-- Step 3: Create stage for Streamlit app files (if deploying via SQL)
CREATE STAGE IF NOT EXISTS streamlit_stage
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for Streamlit app files';

-- Step 4: Upload app file (run from local machine using SnowSQL)
-- PUT file://streamlit_app.py @streamlit_stage/streamlit_app.py AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- Step 5: Create Streamlit app
CREATE OR REPLACE STREAMLIT CONSTRUCTION_DEFECT_ANALYSIS
    ROOT_LOCATION = '@SUPERSTORE.DEMO.streamlit_stage'
    MAIN_FILE = 'streamlit_app.py'
    QUERY_WAREHOUSE = 'COMPUTE_WH'
    COMMENT = 'AI-powered construction defect analysis using Snowflake Cortex';

-- Step 6: Grant permissions to roles
-- Replace YOUR_ROLE with actual role names
GRANT USAGE ON STREAMLIT CONSTRUCTION_DEFECT_ANALYSIS TO ROLE ACCOUNTADMIN;
-- GRANT USAGE ON STREAMLIT CONSTRUCTION_DEFECT_ANALYSIS TO ROLE YOUR_ROLE;

-- Step 7: Verify creation
SHOW STREAMLITS LIKE 'CONSTRUCTION_DEFECT_ANALYSIS';

-- Step 8: Get app URL
SELECT 
    'https://' || CURRENT_ACCOUNT() || '.snowflakecomputing.com/streamlit/' || 
    CURRENT_DATABASE() || '.' || CURRENT_SCHEMA() || '.CONSTRUCTION_DEFECT_ANALYSIS' AS APP_URL;

-- ═══════════════════════════════════════════════════════════════════
-- Optional: Monitoring queries
-- ═══════════════════════════════════════════════════════════════════

-- Check warehouse usage
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'COMPUTE_WH'
  AND START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- Check Streamlit app access logs
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
WHERE OBJECT_NAME = 'CONSTRUCTION_DEFECT_ANALYSIS'
  AND QUERY_START_TIME >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY QUERY_START_TIME DESC;

-- ═══════════════════════════════════════════════════════════════════
-- Cleanup (if needed)
-- ═══════════════════════════════════════════════════════════════════

-- Drop Streamlit app
-- DROP STREAMLIT IF EXISTS CONSTRUCTION_DEFECT_ANALYSIS;

-- Drop stage
-- DROP STAGE IF EXISTS streamlit_stage;
