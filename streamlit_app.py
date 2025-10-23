"""
Snowflake Streamlit App - Construction Defect Analysis
Deploy this file directly in Snowflake using CREATE STREAMLIT command
"""

import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.files import SnowflakeFile
import pandas as pd
import json
from datetime import datetime
from PIL import Image
import io

# Configuration
SNOWFLAKE_STAGE_NAME = "input_stage"
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


def sanitize_filename(filename: str) -> str:
    """Remove special characters from filename"""
    import re
    name, ext = filename.rsplit('.', 1) if '.' in filename else (filename, '')
    name = re.sub(r'[^\w\-]', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    return f"{name}.{ext}" if ext else name


def analyze_with_snowflake(session, stage_name: str, file_path: str, prompt: str, model: str):
    """
    Analyze image using Snowflake AI_COMPLETE with PARSE_JSON wrapper
    Uses CTE structure to extract file metadata and parse AI response
    """
    try:
        # Escape single quotes
        escaped_prompt = prompt.replace("'", "''")
        escaped_file_path = file_path.replace("'", "''")
        stage_ref = f'@{stage_name}'
        
        # CTE structure matching SQL script logic with PARSE_JSON
        query = f"""
        WITH input_pics AS (
            SELECT
                TO_FILE('{stage_ref}', '{escaped_file_path}') AS img,
                d.RELATIVE_PATH AS container_relpath,
                d.SIZE AS file_size_bytes,
                TO_TIMESTAMP_NTZ(d.LAST_MODIFIED) AS last_modified
            FROM DIRECTORY('{stage_ref}') d
            WHERE d.RELATIVE_PATH = '{escaped_file_path}'
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
                
                # Step 1: Upload to Snowflake stage
                status_text.text("⬆️ Uploading to Snowflake stage...")
                progress_bar.progress(33)
                
                try:
                    # Sanitize filename
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    sanitized_name = sanitize_filename(uploaded_file.name)
                    file_path = f"images/{timestamp}_{sanitized_name}"
                    
                    # Upload to stage using PUT command
                    # Write file temporarily
                    file_bytes = uploaded_file.read()
                    uploaded_file.seek(0)
                    
                    # Use Snowflake PUT to upload
                    session.sql(f"PUT 'file:///tmp/{sanitized_name}' @{SNOWFLAKE_STAGE_NAME}/{file_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE").collect()
                    
                    st.success(f"✅ Uploaded to stage: `{file_path}`")
                    progress_bar.progress(66)
                    
                except Exception as e:
                    st.error(f"❌ Upload error: {str(e)}")
                    st.stop()
                
                # Step 2: Refresh stage
                status_text.text("🔄 Refreshing stage...")
                try:
                    session.sql(f"ALTER STAGE {SNOWFLAKE_STAGE_NAME} REFRESH").collect()
                except Exception as e:
                    st.warning(f"⚠️ Stage refresh: {str(e)}")
                
                # Step 3: Analyze with AI
                status_text.text("🤖 Analyzing with Snowflake AI...")
                
                success, result = analyze_with_snowflake(session, SNOWFLAKE_STAGE_NAME, file_path, prompt, model)
                
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
