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
        st.subheader("📂 Select Image from Stage")

        # Load available images from stage
        selected_file = None
        try:
            files_query = f"""
            SELECT
                RELATIVE_PATH,
                SIZE,
                LAST_MODIFIED
            FROM DIRECTORY(@{SNOWFLAKE_STAGE_NAME})
            WHERE RELATIVE_PATH LIKE '%.jpg'
               OR RELATIVE_PATH LIKE '%.jpeg'
               OR RELATIVE_PATH LIKE '%.png'
               OR RELATIVE_PATH LIKE '%.gif'
               OR RELATIVE_PATH LIKE '%.webp'
            ORDER BY LAST_MODIFIED DESC
            LIMIT 100
            """
            files_df = session.sql(files_query).to_pandas()

            if len(files_df) > 0:
                # Create a selectbox with file names
                selected_file = st.selectbox(
                    "Choose an image from stage",
                    options=files_df['RELATIVE_PATH'].tolist(),
                    help="Select an image that has been uploaded to the Snowflake stage"
                )

                if selected_file:
                    # Display file info
                    file_info = files_df[files_df['RELATIVE_PATH'] == selected_file].iloc[0]
                    file_size_mb = file_info['SIZE'] / (1024 * 1024)

                    st.info(f"""
                    📊 **File:** {selected_file}
                    📏 **Size:** {file_size_mb:.2f} MB
                    🕒 **Modified:** {file_info['LAST_MODIFIED']}
                    """)

                    st.success(f"✅ Selected: `{selected_file}`")
            else:
                st.warning("⚠️ No images found in stage. Please upload images to the stage first.")
                st.info("""
                **To upload images to the stage:**
                1. Use SnowSQL: `PUT file://local/path/image.jpg @input_stage/images`
                2. Or upload via Snowsight UI to the stage
                3. Then refresh this page
                """)

        except Exception as e:
            st.error(f"❌ Error loading files from stage: {str(e)}")

    with col2:
        st.subheader("🔍 Analysis Results")

        if selected_file:
            if st.button("🚀 Analyze Image", type="primary", use_container_width=True):
                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Step 1: Analyze with AI (file already in stage)
                status_text.text("🤖 Analyzing with Snowflake AI...")
                progress_bar.progress(50)

                success, result = analyze_with_snowflake(session, SNOWFLAKE_STAGE_NAME, selected_file, prompt, model)
                
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
                            "Image": [selected_file],
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
