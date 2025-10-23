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

        # Analyze using AI_COMPLETE
        query = f"""
        WITH input_pics AS (
            SELECT
                TO_FILE('@{TEMP_STAGE_NAME}', '{stage_file_path}') AS img,
                '{filename}' AS container_relpath,
                {len(file_bytes)} AS file_size_bytes,
                CURRENT_TIMESTAMP() AS last_modified
        ),
        ai_analysis AS (
            SELECT
                container_relpath,
                file_size_bytes,
                last_modified,
                PARSE_JSON(
                    AI_COMPLETE(
                        model => '{model}',
                        prompt => PROMPT('{escaped_prompt}', img)
                    )
                ) AS result_json
            FROM input_pics
        )
        SELECT
            container_relpath,
            file_size_bytes,
            last_modified,
            result_json
        FROM ai_analysis;
        """

        result = session.sql(query).collect()

        if result and len(result) > 0:
            row = result[0]
            return True, {
                "ai_result": json.loads(row['RESULT_JSON']) if isinstance(row['RESULT_JSON'], str) else row['RESULT_JSON'],
                "metadata": {
                    "file_path": row['CONTAINER_RELPATH'],
                    "file_size_bytes": row['FILE_SIZE_BYTES'],
                    "last_modified": str(row['LAST_MODIFIED']) if row['LAST_MODIFIED'] else None
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

    st.title("🏗️ Construction Defect Analysis - AI Image Processing")
    st.markdown("Upload construction site images for AI-powered defect detection using Snowflake Cortex")

    # Get Snowflake session
    session = get_active_session()

    # Sidebar - Configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        # Model selection by category
        model_category = st.selectbox("Model Provider", list(AVAILABLE_MODELS.keys()))
        model = st.selectbox("Select Model", AVAILABLE_MODELS[model_category])

        st.markdown("---")
        st.subheader("📋 Analysis Prompt")
        prompt = st.text_area(
            "Prompt Template",
            value=DEFAULT_PROMPT,
            height=200,
            help="Use {0} as placeholder for the image"
        )

        st.markdown("---")
        st.info(f"""
        **Limits:**
        - Max size: {MAX_SIZE_MB_CLAUDE if model in CLAUDE_MODELS else MAX_SIZE_MB_GENERAL} MB
        - Formats: {', '.join(SUPPORTED_FORMATS)}
        {'- Max resolution: 8000x8000 (Claude)' if model in CLAUDE_MODELS else ''}
        """)

    # Main content
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("📤 Upload Image")
        uploaded_file = st.file_uploader(
            "Choose an image",
            type=['jpg', 'jpeg', 'png', 'gif', 'webp'],
            help="Upload a construction site image for defect analysis"
        )

        if uploaded_file:
            st.image(uploaded_file, caption=uploaded_file.name, use_container_width=True)

            # Display image info
            file_size_mb = uploaded_file.size / (1024 * 1024)
            image = Image.open(uploaded_file)
            width, height = image.size
            uploaded_file.seek(0)
            st.info(f"📊 **Size:** {file_size_mb:.2f} MB | **Resolution:** {width}x{height}px")

    with col2:
        st.subheader("🔍 Analysis Results")

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
                st.markdown("### 📊 Analysis Output")

                if isinstance(result, dict):
                    ai_result = result.get('ai_result', {})
                    metadata = result.get('metadata', {})

                    # Display metadata
                    if metadata:
                        st.markdown("### 📁 File Metadata")
                        metadata_cols = st.columns(3)
                        with metadata_cols[0]:
                            st.metric("File Path", metadata.get('file_path', 'N/A'))
                        with metadata_cols[1]:
                            file_size_kb = metadata.get('file_size_bytes', 0) / 1024
                            st.metric("File Size", f"{file_size_kb:.2f} KB")
                        with metadata_cols[2]:
                            st.metric("Last Modified", metadata.get('last_modified', 'N/A'))

                    # Display AI result
                    st.json(ai_result)

                    # Extract message content
                    if 'choices' in ai_result and len(ai_result['choices']) > 0:
                        message_content = ai_result['choices'][0].get('message', {}).get('content', '')

                        st.markdown("### 📝 AI Response")
                        st.markdown(message_content)

                        # Summary table
                        st.markdown("### 📋 Summary Table")
                        summary_data = {
                            "Model": [model],
                            "Timestamp": [datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
                            "Image": [uploaded_file.name],
                            "Status": ["✅ Completed"]
                        }

                        # Add token usage if available
                        if 'usage' in ai_result:
                            summary_data["Total Tokens"] = [ai_result['usage'].get('total_tokens', 'N/A')]
                            summary_data["Prompt Tokens"] = [ai_result['usage'].get('prompt_tokens', 'N/A')]
                            summary_data["Completion Tokens"] = [ai_result['usage'].get('completion_tokens', 'N/A')]

                        df = pd.DataFrame(summary_data)
                        st.dataframe(df, use_container_width=True)
                    else:
                        st.warning("⚠️ Unexpected response format")
                else:
                    st.error("❌ Invalid result format")


if __name__ == "__main__":
    main()
