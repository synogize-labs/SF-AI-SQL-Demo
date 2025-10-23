# Quick Start: Deploy to Snowflake

## 🚀 Fastest Way to Deploy (Snowsight UI)

### 1. Run Setup (One-time)
```sql
-- In Snowflake Worksheet, run:
USE DATABASE SUPERSTORE;
USE SCHEMA DEMO;

CREATE OR REPLACE STAGE input_stage
  URL = 'azure://judodemoaest.blob.core.windows.net/construction-building-defects/'
  STORAGE_INTEGRATION = azure_int
  DIRECTORY = (ENABLE = TRUE);
```

### 2. Create Streamlit App

1. Open **Snowsight** (https://app.snowflake.com)
2. Go to **Streamlit** tab in left menu
3. Click **+ Streamlit App**
4. Fill in:
   - **Name**: `Construction_Defect_Analysis`
   - **Warehouse**: `COMPUTE_WH`
   - **App location**: `SUPERSTORE.DEMO`
5. Copy entire contents of `streamlit_app.py` into editor
6. Click **Run** 🎉

### 3. Access Your App

Find it under **Streamlit** → **Construction_Defect_Analysis**

## 📋 What You Get

- ✅ **No servers to manage** - Runs entirely in Snowflake
- ✅ **Built-in authentication** - Uses your Snowflake login
- ✅ **Secure** - Images never leave Snowflake
- ✅ **Easy sharing** - Share via Snowflake roles
- ✅ **Pay-per-use** - Only charged for warehouse runtime

## 🔧 Configuration

The app uses these models by default:
- OpenAI GPT-4.1
- Claude 4 Sonnet
- Llama 4 Scout
- Pixtral Large

Default prompt analyzes:
- Material type
- Defects and severity
- Repairs needed
- Cost estimates

## 📁 Files Overview

| File | Purpose |
|------|---------|
| `streamlit_app.py` | **Deploy this to Snowflake** |
| `app.py` | Local development version |
| `QUICK_SETUP.sql` | Create input_stage |
| `DEPLOY_STREAMLIT.sql` | SQL deployment (alternative) |
| `DEPLOY_TO_SNOWFLAKE.md` | Detailed deployment guide |

## 🆚 Local vs Snowflake

| Feature | Local (`app.py`) | Snowflake (`streamlit_app.py`) |
|---------|------------------|--------------------------------|
| Upload | Azure Blob Storage | Snowflake stage |
| Auth | secrets.toml | Built-in session |
| Run | `streamlit run app.py` | Snowsight UI |
| Deploy | Docker/Cloud | CREATE STREAMLIT |

## ❓ Common Issues

### "Stage does not exist"
→ Run `QUICK_SETUP.sql` first

### "Insufficient privileges"
```sql
GRANT CREATE STREAMLIT ON SCHEMA DEMO TO ROLE YOUR_ROLE;
```

### "AI_COMPLETE not found"
→ Contact Snowflake to enable Cortex AI

## 📖 Full Documentation

- Detailed deployment: [`DEPLOY_TO_SNOWFLAKE.md`](DEPLOY_TO_SNOWFLAKE.md)
- SQL deployment: [`DEPLOY_STREAMLIT.sql`](DEPLOY_STREAMLIT.sql)
- GitHub: https://github.com/synogize-labs/SF-AI-SQL-Demo

---

**Estimated deployment time: 5 minutes** ⏱️
