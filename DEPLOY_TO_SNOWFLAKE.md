# Deploy to Snowflake Streamlit

This guide explains how to deploy the Construction Defect Analysis app directly in Snowflake.

## Prerequisites

1. Snowflake account with Streamlit enabled
2. `input_stage` created (run `QUICK_SETUP.sql` first)
3. ACCOUNTADMIN or appropriate role with:
   - CREATE STREAMLIT privilege
   - USAGE on warehouse
   - USAGE on database and schema

## Deployment Steps

### Step 1: Prepare the Stage

Run the setup script to create the required stage:

```sql
-- Run QUICK_SETUP.sql first
USE DATABASE SUPERSTORE;
USE SCHEMA DEMO;

-- Verify stage exists
SHOW STAGES LIKE 'input_stage';
```

### Step 2: Create the Streamlit App

Use Snowsight UI or SQL to create the app:

#### Option A: Using Snowsight UI (Recommended)

1. Go to **Snowsight** → **Streamlit** → **+ Streamlit App**
2. Name: `Construction_Defect_Analysis`
3. Warehouse: `COMPUTE_WH`
4. App location: `SUPERSTORE.DEMO`
5. Copy the entire contents of `streamlit_app.py` into the code editor
6. Click **Run**

#### Option B: Using SQL

```sql
-- Create Streamlit app
CREATE STREAMLIT SUPERSTORE.DEMO.CONSTRUCTION_DEFECT_ANALYSIS
  ROOT_LOCATION = '@SUPERSTORE.DEMO.streamlit_stage'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';

-- Upload the app file
PUT file://streamlit_app.py @SUPERSTORE.DEMO.streamlit_stage/streamlit_app.py AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### Step 3: Grant Permissions

```sql
-- Grant access to roles that need to use the app
GRANT USAGE ON STREAMLIT SUPERSTORE.DEMO.CONSTRUCTION_DEFECT_ANALYSIS TO ROLE YOUR_ROLE;
```

### Step 4: Access the App

Navigate to **Snowsight** → **Streamlit** → **Construction_Defect_Analysis**

Or use the direct URL:
```
https://<your-account>.snowflakecomputing.com/streamlit/SUPERSTORE.DEMO.CONSTRUCTION_DEFECT_ANALYSIS
```

## Key Differences from Local Version

| Feature | Local Version | Snowflake Version |
|---------|---------------|-------------------|
| **File Upload** | Upload to Azure Blob Storage | Upload directly to Snowflake stage |
| **Authentication** | SSO via `secrets.toml` | Built-in Snowflake session |
| **Connection** | `snowflake.connector` | `snowflake.snowpark` |
| **Secrets** | `.streamlit/secrets.toml` | Not needed |
| **Deployment** | `streamlit run app.py` | CREATE STREAMLIT command |

## Advantages of Snowflake Deployment

1. **No Infrastructure** - No need to manage servers or containers
2. **Built-in Auth** - Uses Snowflake's authentication
3. **Data Security** - Images stay within Snowflake environment
4. **Cost Efficiency** - Pay only for compute warehouse usage
5. **Easy Sharing** - Share with colleagues via Snowflake roles

## Troubleshooting

### Error: "Stage does not exist"
- Run `QUICK_SETUP.sql` to create `input_stage`
- Verify: `SHOW STAGES LIKE 'input_stage';`

### Error: "Insufficient privileges"
- Ensure role has CREATE STREAMLIT privilege
- Grant: `GRANT CREATE STREAMLIT ON SCHEMA DEMO TO ROLE YOUR_ROLE;`

### Error: "File upload failed"
- Check stage permissions
- Verify warehouse is running
- Ensure DIRECTORY is enabled on stage

### Error: "AI_COMPLETE not found"
- Snowflake Cortex AI may not be enabled
- Contact Snowflake support to enable Cortex AI features

## Updating the App

To update the deployed app:

1. **Via Snowsight**: Open the app → Edit code → Make changes → Save
2. **Via SQL**: Re-upload the file with `PUT` command and `OVERWRITE=TRUE`

## Monitoring

View app usage and performance:

```sql
-- Check app status
SHOW STREAMLITS LIKE 'CONSTRUCTION_DEFECT_ANALYSIS';

-- View warehouse usage
SELECT *
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE WAREHOUSE_NAME = 'COMPUTE_WH'
ORDER BY START_TIME DESC
LIMIT 10;
```

## Support

For issues or questions:
- Snowflake Documentation: https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit
- GitHub Issues: https://github.com/synogize-labs/SF-AI-SQL-Demo/issues
