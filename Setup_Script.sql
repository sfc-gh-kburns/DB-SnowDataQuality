-- ========================================================================================
--    _____      _                  _____           _       _   
--   / ____|    | |                / ____|         (_)     | |  
--  | (___   ___| |_ _   _ _ __   | (___   ___ _ __ _ _ __ | |_ 
--   \___ \ / _ \ __| | | | '_ \   \___ \ / __| '__| | '_ \| __|
--   ____) |  __/ |_| |_| | |_) |  ____) | (__| |  | | |_) | |_ 
--  |_____/ \___|\__|\__,_| .__/  |_____/ \___|_|  |_| .__/ \__|
--                        | |                        | |        
--                        |_|                        |_|                                                                                                      |___/                                                                                              |_|   |_|  
-- ========================================================================================
-- 
-- This script sets up the complete Streamlit application environment in Snowflake
-- Run this as ACCOUNTADMIN or a role with appropriate privileges
--
-- What this script does:
-- 1. Creates a dedicated role (db_snowdq_role) for the application
-- 2. Creates necessary databases and schemas
-- 3. Sets up all required permissions and grants
-- 4. Creates API integration for GitHub access
-- 5. Creates Git repository integration
-- 6. Deploys the Streamlit application
-- 7. Provides verification and completion status
--
-- GitHub Repository: https://github.com/sfc-gh-kburns/DB-SnowDataQuality
-- ========================================================================================

-- Set context to ensure we're using the right role
USE ROLE ACCOUNTADMIN;

-- Display setup start message
SELECT 'Starting Snowflake Data Quality & Documentation App Setup...' AS STATUS;

-- Set current user variable for role assignment
SET current_user_name = CURRENT_USER();

-- ========================================================================================
-- STEP 1: CREATE DEDICATED ROLE AND GRANT TO CURRENT USER
-- ========================================================================================

-- Create the application role
CREATE ROLE IF NOT EXISTS db_snowdq_role
COMMENT = 'Role for Snowflake Data Quality & Documentation Streamlit application';

-- Grant the role to the current user
GRANT ROLE db_snowdq_role TO USER IDENTIFIER($current_user_name);

-- Grant CREATE DATABASE privilege to the role
GRANT CREATE DATABASE ON ACCOUNT TO ROLE db_snowdq_role;

-- Grant access to Snowflake system database for metadata queries (must be done as ACCOUNTADMIN)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE db_snowdq_role;

-- Grant specific database roles for metadata access (must be done as ACCOUNTADMIN)
GRANT DATABASE ROLE SNOWFLAKE.OBJECT_VIEWER TO ROLE db_snowdq_role;
GRANT DATABASE ROLE SNOWFLAKE.USAGE_VIEWER TO ROLE db_snowdq_role;

-- Grant Cortex access for LLM functionality (must be done as ACCOUNTADMIN)
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE db_snowdq_role;

-- Grant Data Metric Function access for data quality monitoring (must be done as ACCOUNTADMIN)
GRANT DATABASE ROLE SNOWFLAKE.DATA_METRIC_USER TO ROLE db_snowdq_role;
GRANT APPLICATION ROLE SNOWFLAKE.DATA_QUALITY_MONITORING_LOOKUP TO ROLE db_snowdq_role;

SELECT 'Step 1/8: Created application role and granted system-level permissions' AS STATUS;

-- ========================================================================================
-- STEP 2: CREATE WAREHOUSE AND SET USER DEFAULTS
-- ========================================================================================

-- Create a dedicated warehouse for the application
CREATE OR REPLACE WAREHOUSE db_snowdq_wh
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  COMMENT = 'Warehouse for Snowflake Data Quality & Documentation app';

-- Grant warehouse usage to the role
GRANT USAGE ON WAREHOUSE db_snowdq_wh TO ROLE db_snowdq_role;

-- Set current user's default role and warehouse
ALTER USER IDENTIFIER($current_user_name) SET DEFAULT_ROLE = db_snowdq_role;
ALTER USER IDENTIFIER($current_user_name) SET DEFAULT_WAREHOUSE = db_snowdq_wh;

SELECT 'Step 2/8: Created warehouse and set user defaults' AS STATUS;

-- ========================================================================================
-- STEP 3: SWITCH TO APPLICATION ROLE AND CREATE DATABASES
-- ========================================================================================

-- Switch to the application role to create all objects
USE ROLE db_snowdq_role;

-- Create the main application database
CREATE OR REPLACE DATABASE db_snowdq
COMMENT = 'Main database for Snowflake Data Quality & Documentation application';

-- Use the main database
USE DATABASE db_snowdq;

-- Create the main schema
CREATE OR REPLACE SCHEMA public
COMMENT = 'Main schema for Streamlit application objects';

-- Create the tracking database (DB_SNOWTOOLS)
CREATE OR REPLACE DATABASE DB_SNOWTOOLS
COMMENT = 'Database for Snowflake Data Quality & Documentation application tracking';

-- Use the tracking database
USE DATABASE DB_SNOWTOOLS;

-- Create the tracking schema
CREATE OR REPLACE SCHEMA PUBLIC
COMMENT = 'Public schema for application tracking tables';

SELECT 'Step 3/8: Switched to application role and created databases/schemas' AS STATUS;

-- ========================================================================================
-- STEP 4: CREATE TRACKING TABLES
-- ========================================================================================

-- Use the tracking database
USE DATABASE DB_SNOWTOOLS;
USE SCHEMA PUBLIC;

-- Create DATA_DESCRIPTION_HISTORY table
CREATE TABLE IF NOT EXISTS DATA_DESCRIPTION_HISTORY (
    HISTORY_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    DATABASE_NAME VARCHAR(255) NOT NULL,
    SCHEMA_NAME VARCHAR(255) NOT NULL,
    OBJECT_TYPE VARCHAR(50) NOT NULL,
    OBJECT_NAME VARCHAR(255) NOT NULL,
    COLUMN_NAME VARCHAR(255),
    BEFORE_DESCRIPTION TEXT,
    AFTER_DESCRIPTION TEXT,
    SQL_EXECUTED TEXT,
    UPDATED_BY VARCHAR(255) DEFAULT CURRENT_USER(),
    UPDATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
COMMENT = 'Tracks all description changes made through the application';

-- Create DATA_QUALITY_RESULTS table
CREATE TABLE IF NOT EXISTS DATA_QUALITY_RESULTS (
    RESULT_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    MONITOR_NAME VARCHAR(255) NOT NULL,
    DATABASE_NAME VARCHAR(255) NOT NULL,
    SCHEMA_NAME VARCHAR(255) NOT NULL,
    TABLE_NAME VARCHAR(255) NOT NULL,
    COLUMN_NAME VARCHAR(255),
    METRIC_VALUE NUMBER,
    METRIC_UNIT VARCHAR(50),
    THRESHOLD_MIN NUMBER,
    THRESHOLD_MAX NUMBER,
    STATUS VARCHAR(20),
    MEASUREMENT_TIME TIMESTAMP_LTZ,
    RECORD_INSERTED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    SQL_EXECUTED TEXT
)
COMMENT = 'Stores data quality monitoring results from DMFs';

SELECT 'Step 4/8: Created tracking tables in DB_SNOWTOOLS' AS STATUS;

-- ========================================================================================
-- STEP 5: CREATE API INTEGRATION FOR GITHUB (requires ACCOUNTADMIN)
-- ========================================================================================

-- Switch back to ACCOUNTADMIN to create API integration
USE ROLE ACCOUNTADMIN;

-- Create API integration for GitHub access
CREATE OR REPLACE API INTEGRATION db_snowdq_git_api_integration
  API_PROVIDER = GIT_HTTPS_API
  API_ALLOWED_PREFIXES = ('https://github.com/sfc-gh-kburns')
  ENABLED = TRUE
  COMMENT = 'API integration for DB-SnowDataQuality GitHub repository access';

-- Grant usage on the API integration to the role
GRANT USAGE ON INTEGRATION db_snowdq_git_api_integration TO ROLE db_snowdq_role;

SELECT 'Step 5/8: Created GitHub API integration' AS STATUS;

-- ========================================================================================
-- STEP 6: CREATE GIT REPOSITORY AND DEPLOY STREAMLIT APP
-- ========================================================================================

-- Switch back to application role and database
USE ROLE db_snowdq_role;
USE DATABASE db_snowdq;
USE SCHEMA public;

-- Create Git repository object
CREATE OR REPLACE GIT REPOSITORY db_snowdq_repo
  API_INTEGRATION = db_snowdq_git_api_integration
  ORIGIN = 'https://github.com/sfc-gh-kburns/DB-SnowDataQuality.git'
  COMMENT = 'Git repository for Snowflake Data Quality & Documentation app';

-- Fetch the latest code from the repository
ALTER GIT REPOSITORY db_snowdq_repo FETCH;

-- Create the Streamlit application
CREATE OR REPLACE STREAMLIT db_snowdq_app
  ROOT_LOCATION = '@db_snowdq_repo/branches/main'
  MAIN_FILE = '/app.py'
  QUERY_WAREHOUSE = db_snowdq_wh
  COMMENT = 'Snowflake Data Quality & Documentation Streamlit Application';

SELECT 'Step 6/8: Created Git repository and deployed Streamlit app' AS STATUS;

-- ========================================================================================
-- STEP 7: VERIFICATION AND COMPLETION
-- ========================================================================================

-- Show the role and its grants for verification
SHOW GRANTS TO ROLE db_snowdq_role;

-- Display the Streamlit app information
SHOW STREAMLITS IN SCHEMA db_snowdq.public;

-- Display completion messages and next steps
SELECT 'Step 7/7: Setup completed successfully!' AS STATUS
UNION ALL
SELECT '========================================' AS STATUS
UNION ALL
SELECT 'SETUP COMPLETE - NEXT STEPS:' AS STATUS
UNION ALL
SELECT '========================================' AS STATUS
UNION ALL
SELECT '1. Grant the db_snowdq_role to users who need access:' AS STATUS
UNION ALL
SELECT '   GRANT ROLE db_snowdq_role TO USER <your_username>;' AS STATUS
UNION ALL
SELECT '' AS STATUS
UNION ALL
SELECT '2. Optionally set as default role for users:' AS STATUS
UNION ALL
SELECT '   ALTER USER <your_username> SET DEFAULT_ROLE = db_snowdq_role;' AS STATUS
UNION ALL
SELECT '' AS STATUS
UNION ALL
SELECT '3. Grant access to your data databases/schemas:' AS STATUS
UNION ALL
SELECT '   GRANT USAGE ON DATABASE <your_data_db> TO ROLE db_snowdq_role;' AS STATUS
UNION ALL
SELECT '   GRANT USAGE ON ALL SCHEMAS IN DATABASE <your_data_db> TO ROLE db_snowdq_role;' AS STATUS
UNION ALL
SELECT '   GRANT SELECT ON ALL TABLES IN DATABASE <your_data_db> TO ROLE db_snowdq_role;' AS STATUS
UNION ALL
SELECT '   GRANT SELECT ON ALL VIEWS IN DATABASE <your_data_db> TO ROLE db_snowdq_role;' AS STATUS
UNION ALL
SELECT '' AS STATUS
UNION ALL
SELECT '4. For description updates, also grant MODIFY privileges:' AS STATUS
UNION ALL
SELECT '   GRANT MODIFY ON ALL TABLES IN DATABASE <your_data_db> TO ROLE db_snowdq_role;' AS STATUS
UNION ALL
SELECT '   GRANT MODIFY ON ALL VIEWS IN DATABASE <your_data_db> TO ROLE db_snowdq_role;' AS STATUS
UNION ALL
SELECT '' AS STATUS
UNION ALL
SELECT '5. Access your Streamlit app at:' AS STATUS
UNION ALL
SELECT '   Snowsight > Projects > Streamlit > db_snowdq_app' AS STATUS
UNION ALL
SELECT '' AS STATUS
UNION ALL
SELECT 'The application is now ready to use!' AS STATUS;

-- ========================================================================================
-- OPTIONAL: ENABLE CORTEX MODELS (if restricted in your account)
-- ========================================================================================

-- Uncomment the line below if Cortex models are restricted in your account
-- ALTER ACCOUNT SET CORTEX_MODELS_ALLOWLIST = 'claude-3-5-sonnet,reka-core,mistral-large2,llama3-70b,snowflake-arctic';

-- ========================================================================================
-- SETUP SCRIPT COMPLETE
-- ========================================================================================
