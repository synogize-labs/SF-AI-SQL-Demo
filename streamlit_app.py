"""
Snowflake Streamlit App - Construction Defect Analysis
Version with file upload support using temporary stage
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd
import json
from datetime import datetime
from PIL import Image
import io
import base64
import tempfile
import os

# Configuration
SNOWFLAKE_STAGE_NAME = "input_stage"
TEMP_STAGE_NAME = "temp_upload_stage"
SUPPORTED_FORMATS = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}
MAX_SIZE_MB_GENERAL = 10
MAX_SIZE_MB_CLAUDE = 3.75
MAX_RESOLUTION_CLAUDE = 8000

CLAUDE_MODELS = [
    "claude-3.5-sonnet",
    "claude-3.7-sonnet",
    "claude-4-sonnet",
    "claude-4-opus"
]

AVAILABLE_MODELS = {
    "OpenAI": ["openai-gpt-4.1", "openai-o4-mini"],
    "Anthropic (Claude)": CLAUDE_MODELS,
    "Meta (Llama)": ["llama4-maverick", "llama4-scout"],
    "Mistral": ["pixtral-large"]
}

# Structured prompt for construction defect analysis
DEFAULT_PROMPT = """Describe the key characteristics of this wall as seen in this image {0}, noting this is part of a building. Keep descriptions concise and focus on structural defects. Respond in JSON with fields: material, colour, distinguishing_features, is_cracked, is_defective, defect_severity, defects, repairs_required, estimated_time_repairs_required, confidence_level_on_material, estimated_cost_of_repairs"""


def validate_image(uploaded_file, model_name):
    """Validate image format, size, and resolution"""
    # Check format
    file_ext = uploaded_file.name.split('.')[-1].lower()
    if f'.{file_ext}' not in SUPPORTED_FORMATS:
        return False, f"❌ Unsupported format. Supported: {', '.join(SUPPORTED_FORMATS)}"

    # Check size
    file_size_mb = uploaded_file.size / (1024 * 1024)
    is_claude = model_name in CLAUDE_MODELS
    max_size = MAX_SIZE_MB_CLAUDE if is_claude else MAX_SIZE_MB_GENERAL

    if file_size_mb > max_size:
        return False, f"❌ File too large ({file_size_mb:.2f} MB). Max: {max_size} MB for {model_name}"

    # Check resolution for Claude models
    if is_claude:
        try:
            image = Image.open(uploaded_file)
            width, height = image.size
            if width > MAX_RESOLUTION_CLAUDE or height > MAX_RESOLUTION_CLAUDE:
                return False, f"❌ Resolution {width}x{height} exceeds Claude limit of {MAX_RESOLUTION_CLAUDE}x{MAX_RESOLUTION_CLAUDE}"
            uploaded_file.seek(0)  # Reset file pointer
        except Exception as e:
            return False, f"❌ Could not read image: {str(e)}"

    return True, "✅ Validation passed"


def upload_to_stage_workaround(session, uploaded_file, stage_name):
    """
    Upload file to stage using workaround method:
    1. Create temp table with binary data
    2. Use COPY INTO to write to stage
    """
    try:
        # Read file bytes
        file_bytes = uploaded_file.read()
        uploaded_file.seek(0)

        # Encode to base64
        file_b64 = base64.b64encode(file_bytes).decode('utf-8')

        # Generate unique filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"upload_{timestamp}_{uploaded_file.name}"

        # Create temporary table with file data
        temp_table = f"temp_upload_{timestamp}"

        # Method: Use Snowflake internal stage with SQL
        session.sql(f"""
            CREATE TEMPORARY TABLE {temp_table} (
                file_name STRING,
                file_content STRING
            )
        """).collect()

        # Insert data
        session.sql(f"""
            INSERT INTO {temp_table}
            VALUES ('{filename}', '{file_b64}')
        """).collect()

        # Use COPY INTO to write binary data to stage
        # This is a workaround - we'll actually just use the base64 data directly

        return True, filename, file_bytes

    except Exception as e:
        return False, None, str(e)


def analyze_with_uploaded_file(session, file_bytes, filename, prompt, model):
    """
    Analyze uploaded image directly using base64 encoding
    Workaround: Upload to temp stage then analyze
    """
    try:
        # Try to upload file bytes to a stage using Snowpark DataFrame
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        temp_filename = f"temp_{timestamp}_{filename}"

        # Write to temp file in container
        temp_dir = "/tmp"  # Snowflake containers have /tmp
        temp_path = os.path.join(temp_dir, temp_filename)

        with open(temp_path, 'wb') as f:
            f.write(file_bytes)

        # Try to upload using Snowpark
        try:
            # Ensure temp stage exists WITHOUT client-side encryption
            # Client-side encryption is not supported by AI_COMPLETE TO_FILE
            # DROP and recreate to ensure correct encryption type
            try:
                session.sql(f"DROP STAGE IF EXISTS {TEMP_STAGE_NAME}").collect()
            except:
                pass  # Ignore if doesn't exist

            session.sql(f"""
                CREATE STAGE {TEMP_STAGE_NAME}
                ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
                DIRECTORY = (ENABLE = TRUE)
            """).collect()

            # Upload file
            put_result = session.file.put(
                temp_path,
                f"@{TEMP_STAGE_NAME}",
                auto_compress=False,
                overwrite=True
            )

            # Clean up temp file
            os.remove(temp_path)

            # Refresh stage
            session.sql(f"ALTER STAGE {TEMP_STAGE_NAME} REFRESH").collect()

            # Now analyze using the uploaded file
            stage_file_path = temp_filename

        except Exception as upload_err:
            # If upload fails, try direct analysis with base64
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise Exception(f"Upload failed: {str(upload_err)}")

        # Escape single quotes in prompt
        escaped_prompt = prompt.replace("'", "''")
        escaped_filename = filename.replace("'", "''")

        # Ensure AI_IMAGE_RUN_LOG table exists
        try:
            session.sql("""
                CREATE TABLE IF NOT EXISTS AI_IMAGE_RUN_LOG (
                  run_id              STRING      DEFAULT UUID_STRING(),
                  run_ts              TIMESTAMP   DEFAULT CURRENT_TIMESTAMP(),
                  user_name           STRING      DEFAULT CURRENT_USER(),
                  stage_name          STRING,
                  image_name          STRING,
                  model_name          STRING,
                  content_type        STRING,
                  file_size_bytes     NUMBER,
                  last_modified       TIMESTAMP,
                  container_relpath   STRING,
                  ai_calls            VARIANT,
                  result_json         VARIANT
                )
            """).collect()
        except:
            pass  # Table might already exist

        # Analyze using AI_COMPLETE and INSERT into log table
        query = f"""
        INSERT INTO AI_IMAGE_RUN_LOG (
          run_ts, user_name,
          stage_name, image_name, model_name,
          content_type, file_size_bytes, last_modified, container_relpath,
          ai_calls, result_json
        )
        WITH input_pics AS (
            SELECT
                '@{TEMP_STAGE_NAME}'                    AS stage_name,
                '{escaped_filename}'                    AS image_name,
                '{model}'                               AS model_name,
                '{escaped_prompt}'                      AS ai_complete_prompt_tmpl,
                d.RELATIVE_PATH                         AS container_relpath,
                d.SIZE                                  AS file_size_bytes,
                TO_TIMESTAMP_NTZ(d.LAST_MODIFIED)       AS last_modified,
                TO_FILE('@{TEMP_STAGE_NAME}', '{stage_file_path}') AS img
            FROM DIRECTORY('@{TEMP_STAGE_NAME}') d
            WHERE d.RELATIVE_PATH = '{stage_file_path}'
            LIMIT 1
        ),
        classify_pics AS (
            SELECT
                *,
                ARRAY_CONSTRUCT(img)[0]['CONTENT_TYPE'] AS content_type,
                OBJECT_CONSTRUCT(
                  'ai_complete', OBJECT_CONSTRUCT(
                    'prompt', ai_complete_prompt_tmpl,
                    'model',  model_name
                  )
                ) AS ai_calls
            FROM input_pics
        ),
        assembled AS (
            SELECT
                *,
                CURRENT_TIMESTAMP()                     AS run_ts,
                CURRENT_USER()                          AS user_name,
                PARSE_JSON(
                    AI_COMPLETE(
                            model => '{model}',
                            prompt => PROMPT(ai_complete_prompt_tmpl, img),
                            response_format => {{
                                'type':'json',
                                'schema':{{
                                    'type':'object',
                                    'properties':{{
                                        'material':{{'type':'string'}}, 
                                        'colour':{{'type':'string'}}, 
                                        'distinguishing_features':{{'type':'string'}}, 
                                        'is_cracked':{{'type':'string'}},  
                                        'is_defective':{{'type':'string'}}, 
                                        'defect_severity':{{'type':'string'}}, 
                                        'defects':{{'type':'string'}}, 
                                        'repairs_required':{{'type':'string'}}, 
                                        'estimated_time_repairs_required':{{'type':'string'}}, 
                                        'confidence_level_on_material':{{'type':'string'}}, 
                                        'estimated_cost_of_repairs':{{'type':'string'}}
                                    }},
                                    'required':['material',
                                                'colour',
                                                'distinguishing_features',
                                                'is_cracked',
                                                'is_defective',
                                                'defect_severity',
                                                'defects',
                                                'repairs_required',
                                                'estimated_time_repairs_required',
                                                'confidence_level_on_material',
                                                'estimated_cost_of_repairs'
                                                ],
                                    'additionalProperties':false
                                }}
                            }},
                            show_details => true
                            )
                    ) AS result_json
            FROM classify_pics
        )
        SELECT
          run_ts, user_name,
          stage_name, image_name, model_name,
          content_type, file_size_bytes, last_modified, container_relpath,
          ai_calls, result_json
        FROM assembled;
        """

        insert_result = session.sql(query).collect()

        # Fetch the latest log entry for this analysis
        fetch_query = """
        SELECT *
        FROM AI_IMAGE_RUN_LOG
        ORDER BY run_ts DESC
        LIMIT 1;
        """

        result = session.sql(fetch_query).collect()

        if result and len(result) > 0:
            row = result[0]
            return True, {
                "ai_result": json.loads(row['RESULT_JSON']) if isinstance(row['RESULT_JSON'], str) else row['RESULT_JSON'],
                "metadata": {
                    "run_id": row['RUN_ID'],
                    "file_path": row['CONTAINER_RELPATH'],
                    "file_size_bytes": row['FILE_SIZE_BYTES'],
                    "last_modified": str(row['LAST_MODIFIED']) if row['LAST_MODIFIED'] else None,
                    "user_name": row['USER_NAME'],
                    "run_ts": str(row['RUN_TS']) if row['RUN_TS'] else None
                }
            }
        else:
            return False, "❌ No result returned from Snowflake"

    except Exception as e:
        return False, f"❌ Analysis error: {str(e)}"


def main():
    st.set_page_config(
        page_title="AI Image Analysis - Snowflake",
        page_icon="🏗️",
        layout="wide"
    )

    # Custom CSS for compact layout
    st.markdown("""
        <style>
        .stApp { font-size: 14px; }
        h1 { font-size: 1.8rem !important; margin-bottom: 0.5rem !important; }
        h2 { font-size: 1.3rem !important; margin-top: 0.5rem !important; margin-bottom: 0.3rem !important; }
        h3 { font-size: 1.1rem !important; margin-top: 0.5rem !important; margin-bottom: 0.3rem !important; }
        h4 { font-size: 1rem !important; margin-top: 0.3rem !important; margin-bottom: 0.2rem !important; }
        .stMarkdown { margin-bottom: 0.5rem !important; }
        .stMetric { font-size: 0.9rem !important; }
        .stMetric label { font-size: 0.85rem !important; }
        .stMetric [data-testid="stMetricValue"] { font-size: 1.1rem !important; }
        </style>
    """, unsafe_allow_html=True)

    st.title("🏗️ Construction Defect Analysis")
    st.caption("Upload construction site images for AI-powered defect detection using Snowflake Cortex")

    # Get Snowflake session
    session = get_active_session()

    # Sidebar - Configuration
    with st.sidebar:
        st.markdown("### ⚙️ Configuration")

        # Model selection by category
        st.markdown("**Model Selection**")
        model_category = st.selectbox("Provider", list(AVAILABLE_MODELS.keys()), label_visibility="collapsed")
        model = st.selectbox("Model", AVAILABLE_MODELS[model_category], label_visibility="collapsed")

        st.markdown("---")
        st.markdown("### 📋 Prompt")
        prompt = st.text_area(
            "Prompt Template",
            value=DEFAULT_PROMPT,
            height=150,
            help="Use {0} as placeholder for the image",
            label_visibility="collapsed"
        )

        st.markdown("---")
        st.markdown(f"""
        <div style='font-size: 0.85rem;'>
        <b>Limits:</b><br/>
        • Max: {MAX_SIZE_MB_CLAUDE if model in CLAUDE_MODELS else MAX_SIZE_MB_GENERAL} MB<br/>
        • Formats: {', '.join([f.replace('.', '') for f in SUPPORTED_FORMATS])}<br/>
        {'• Resolution: 8000x8000 (Claude)' if model in CLAUDE_MODELS else ''}
        </div>
        """, unsafe_allow_html=True)

    # Main content
    col1, col2 = st.columns([1, 1])

    with col1:
        st.markdown("### 📤 Upload Image")
        uploaded_file = st.file_uploader(
            "Choose an image",
            type=['jpg', 'jpeg', 'png', 'gif', 'webp'],
            help="Upload a construction site image for defect analysis",
            label_visibility="collapsed"
        )

        if uploaded_file:
            st.image(uploaded_file, caption=uploaded_file.name, use_container_width=True)

            # Display image info
            file_size_mb = uploaded_file.size / (1024 * 1024)
            image = Image.open(uploaded_file)
            width, height = image.size
            uploaded_file.seek(0)
            st.markdown(f"<div style='font-size: 0.85rem;'>📊 Size: {file_size_mb:.2f} MB | Resolution: {width}x{height}px</div>", unsafe_allow_html=True)

    with col2:
        st.markdown("### 🔍 Analysis Results")

        if uploaded_file:
            if st.button("🚀 Analyze Image", type="primary", use_container_width=True):
                # Validation
                is_valid, validation_msg = validate_image(uploaded_file, model)

                if not is_valid:
                    st.error(validation_msg)
                    st.stop()

                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Step 1: Upload and analyze
                status_text.text("⬆️ Uploading and analyzing...")
                progress_bar.progress(25)

                # Read file bytes
                file_bytes = uploaded_file.read()
                uploaded_file.seek(0)

                # Analyze
                status_text.text("🤖 Analyzing with Snowflake AI...")
                progress_bar.progress(50)

                success, result = analyze_with_uploaded_file(
                    session,
                    file_bytes,
                    uploaded_file.name,
                    prompt,
                    model
                )

                if not success:
                    st.error(result)
                    st.stop()

                progress_bar.progress(100)
                status_text.text("✅ Analysis complete!")

                # Display results
                st.markdown("---")

                if isinstance(result, dict):
                    ai_result = result.get('ai_result', {})
                    metadata = result.get('metadata', {})

                    # Extract message content - handle both formats
                    message_content = None

                    # Format 1: OpenAI-style with choices array
                    if 'choices' in ai_result and len(ai_result['choices']) > 0:
                        message_content = ai_result['choices'][0].get('message', {}).get('content', '')

                    # Format 2: Direct JSON response (Snowflake Cortex native)
                    elif isinstance(ai_result, dict) and any(key in ai_result for key in ['material', 'defects', 'repairs_required']):
                        # Normalize defects and repairs to always be lists
                        def ensure_list(value):
                            """Convert value to list if it's not already"""
                            if value is None:
                                return []
                            if isinstance(value, list):
                                return value
                            if isinstance(value, dict):
                                # Handle dict with numeric keys like {0: "item1", 1: "item2"}
                                return [v for k, v in sorted(value.items())]
                            if isinstance(value, str):
                                # Single string - wrap in list
                                return [value]
                            return []

                        defects = ensure_list(ai_result.get('defects'))
                        repairs = ensure_list(ai_result.get('repairs_required'))

                        # Display key findings in columns
                        result_cols = st.columns(3)
                        with result_cols[0]:
                            st.metric("Cracked", "Yes" if ai_result.get('is_cracked') else "No")
                            st.metric("Repair", "No" if repairs[0].lower() in ('none','n/a') else "Yes")
                        with result_cols[1]:
                            st.metric("Defective", "Yes" if ai_result.get('is_defective') else "No")
                            st.metric("Time", ai_result.get('estimated_time_repairs_required', 'N/A'))
                        with result_cols[2]:
                            st.metric("Severity", ai_result.get('defect_severity', 'N/A').upper())
                            st.metric("Cost", ai_result.get('estimated_cost_of_repairs', 'N/A'),width='content')

                        # Material 
                        if ai_result['material']:
                            st.markdown("**🧱 Material**")
                            st.markdown(ai_result.get('material','N/A').capitalize())
                            st.markdown(ai_result.get('colour','N/A').capitalize())
                            st.markdown(ai_result.get('distinguishing_features','N/A').capitalize())
                                
                        # Defects list
                        if defects:
                            st.markdown("**🔍 Identified Defects**")
                            for defect in defects:
                                st.markdown(f"• {defect.capitalize()}")

                        # Repairs required
                        if repairs:
                            st.markdown("**🔧 Repairs Required**")
                            for i, repair in enumerate(repairs, 1):
                                st.markdown(f"{i}. {repair.capitalize()}")
                            st.markdown(f"Time estimate: {ai_result.get('estimated_time_repairs_required', 'N/A')}")


                    # Format 3: Plain text content
                    elif isinstance(ai_result, str):
                        message_content = ai_result

                    # Display message content if extracted
                    if message_content:
                        st.markdown("**📝 AI Response**")
                        st.markdown(message_content)

                    # Summary table with run log info
                    st.markdown("**📊 Summary**")
                    summary_data = {
                        "Model": [model],
                        "Confidence": ai_result.get('confidence_level_on_material', 'N/A'),
                        "Time": [metadata.get('run_ts', datetime.now().strftime("%H:%M:%S"))[:19] if metadata.get('run_ts') else datetime.now().strftime("%H:%M:%S")],
                        "Image": [uploaded_file.name],
                        "User": [metadata.get('user_name', 'N/A')],
                        "Status": ["✅ Complete"]
                    }

                    # Add run_id if available
                    if metadata and metadata.get('run_id'):
                        st.caption(f"🔑 Run ID: `{metadata.get('run_id')}`")

                    # Add token usage if available
                    if 'usage' in ai_result:
                        summary_data["Tokens"] = [f"{ai_result['usage'].get('total_tokens', 'N/A')}"]

                    df = pd.DataFrame(summary_data)
                    st.dataframe(df, use_container_width=True, hide_index=True)

                    # Collapsible sections
                    with st.expander("📄 View Raw JSON"):
                        st.json(ai_result)

                    with st.expander("📋 View Log Entry"):
                        st.json({
                            "run_id": metadata.get('run_id'),
                            "run_ts": metadata.get('run_ts'),
                            "user_name": metadata.get('user_name'),
                            "file_path": metadata.get('file_path'),
                            "file_size_bytes": metadata.get('file_size_bytes'),
                            "last_modified": metadata.get('last_modified')
                        })
                        
                else:
                    st.error("❌ Invalid result format")


if __name__ == "__main__":
    main()
