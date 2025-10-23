"""
AI Image Analysis - Snowflake Cortex
Main Streamlit application for construction defect detection
"""

import streamlit as st
from PIL import Image
import pandas as pd
from datetime import datetime

# Import custom modules
from config import (
    SUPPORTED_FORMATS,
    MAX_SIZE_MB_GENERAL,
    MAX_SIZE_MB_CLAUDE,
    MAX_RESOLUTION_CLAUDE,
    CLAUDE_MODELS,
    SNOWFLAKE_STAGE_NAME
)
from validators import ImageValidator
from azure_uploader import AzureBlobUploader
from snowflake_analyzer import SnowflakeAIAnalyzer


# Page configuration
st.set_page_config(
    page_title="AI Image Analysis - Snowflake",
    page_icon="🏗️",
    layout="wide"
)


def main():
    st.title("🏗️ Construction Defect Analysis - AI Image Processing")
    st.markdown("Upload construction site images for AI-powered defect detection using Snowflake Cortex")

    # Sidebar - Configuration
    with st.sidebar:
        st.header("⚙️ Configuration")

        model = st.selectbox(
            "Select AI Model",
            [
                # OpenAI Models
                "openai-gpt-4.1",
                "openai-o4-mini",
                # Claude Models (Anthropic)
                "claude-3.5-sonnet",
                "claude-3.7-sonnet",
                "claude-4-sonnet",
                "claude-4-opus",
                # Llama Models (Meta)
                "llama4-maverick",
                "llama4-scout",
                # Mistral Models
                "pixtral-large"
            ],
            help="Claude models have stricter size limits (3.75MB, 8000x8000px)"
        )

        prompt = st.text_area(
            "Analysis Prompt",
            value="Describe the key characteristics of this wall as seen in this image {0}, noting this is part of a building. Keep descriptions concise and focus on structural defects. Respond in JSON with fields: material, colour, distinguishing_features, is_cracked,  is_defective, defect_severity, defects, repairs_required, estimated_time_repairs_required, confidence_level_on_material, estimated_cost_of_repairs",
            height=150
        )

        st.markdown("---")
        st.markdown("### 📋 Image Requirements")

        # Display model-specific requirements
        if model in CLAUDE_MODELS:
            st.warning(
                f"**{model} limits:**\n"
                f"- Max size: {MAX_SIZE_MB_CLAUDE} MB\n"
                f"- Max resolution: {MAX_RESOLUTION_CLAUDE}x{MAX_RESOLUTION_CLAUDE}px"
            )
        else:
            st.info(f"**Max size:** {MAX_SIZE_MB_GENERAL} MB")

        st.info(f"**Formats:** {', '.join(SUPPORTED_FORMATS)}")

    # Main content
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("📤 Upload Image")
        uploaded_file = st.file_uploader(
            "Choose an image",
            type=['jpg', 'jpeg', 'png', 'gif', 'webp'],
            help="Upload a construction site image for analysis"
        )

        if uploaded_file:
            # Display image
            image = Image.open(uploaded_file)
            st.image(image, caption=f"Uploaded: {uploaded_file.name}", use_container_width=True)

            # Display image info
            file_size_mb = uploaded_file.size / (1024 * 1024)
            width, height = image.size
            st.info(f"📊 **Size:** {file_size_mb:.2f} MB | **Resolution:** {width}x{height}px")

    with col2:
        st.subheader("🔍 Analysis Results")

        if uploaded_file:
            if st.button("🚀 Analyze Image", type="primary", use_container_width=True):
                # Validation
                validator = ImageValidator()
                is_valid, error_msg = validator.validate_image(uploaded_file, model)

                if not is_valid:
                    st.error(error_msg)
                    st.stop()

                # Progress tracking
                progress_bar = st.progress(0)
                status_text = st.empty()

                # Step 1: Upload to Azure
                status_text.text("⬆️ Uploading to Azure Blob Storage...")
                progress_bar.progress(25)

                uploader = AzureBlobUploader()
                success, blob_url, file_path = uploader.upload_image(uploaded_file)

                if not success:
                    st.error(blob_url)  # Error message is in blob_url parameter
                    st.stop()

                st.success(f"✅ Uploaded to Azure")
                st.info(f"📁 Azure path: `{file_path}`")
                progress_bar.progress(50)

                # Step 2: Refresh Snowflake stage directory
                status_text.text("🔄 Refreshing Snowflake stage...")
                progress_bar.progress(60)

                analyzer = SnowflakeAIAnalyzer()
                conn_success, conn_msg = analyzer.connect()
                if not conn_success:
                    st.error(conn_msg)
                    st.stop()

                # Refresh stage to sync with Azure
                try:
                    cursor = analyzer.connection.cursor()
                    refresh_query = f"ALTER STAGE {SNOWFLAKE_STAGE_NAME} REFRESH;"
                    cursor.execute(refresh_query)
                    cursor.close()
                    st.success("✅ Stage refreshed")
                except Exception as e:
                    error_msg = str(e)
                    if "does not exist or not authorized" in error_msg:
                        st.error(f"❌ Stage '{SNOWFLAKE_STAGE_NAME}' does not exist!")
                        st.error("📋 Please run QUICK_SETUP.sql in Snowflake first:")
                        st.code(f"""
USE DATABASE SUPERSTORE;
USE SCHEMA DEMO;

CREATE OR REPLACE STAGE {SNOWFLAKE_STAGE_NAME}
  URL = 'azure://judodemoaest.blob.core.windows.net/construction-building-defects/'
  STORAGE_INTEGRATION = azure_int  -- or use CREDENTIALS with SAS_TOKEN
  DIRECTORY = (ENABLE = TRUE);
                        """, language="sql")
                        analyzer.close()
                        st.stop()
                    else:
                        st.warning(f"⚠️ Stage refresh: {error_msg}")

                # Query DIRECTORY to get actual relative path
                try:
                    cursor = analyzer.connection.cursor()
                    # Find the file we just uploaded by matching the filename
                    query = f"""
                    SELECT RELATIVE_PATH
                    FROM DIRECTORY(@{SNOWFLAKE_STAGE_NAME})
                    WHERE RELATIVE_PATH LIKE '%{file_path.split('/')[-1]}%'
                    ORDER BY LAST_MODIFIED DESC
                    LIMIT 1;
                    """
                    cursor.execute(query)
                    result = cursor.fetchone()
                    if result:
                        snowflake_file_path = result[0]
                        st.info(f"📍 Snowflake path: `{snowflake_file_path}`")
                    else:
                        snowflake_file_path = file_path  # Fallback
                        st.warning(f"⚠️ File not found in directory table, using: `{file_path}`")
                    cursor.close()
                except Exception as e:
                    snowflake_file_path = file_path
                    st.warning(f"⚠️ Could not query directory: {str(e)}")

                # Step 3: Analyze with Snowflake AI
                status_text.text("🤖 Analyzing with Snowflake AI...")
                progress_bar.progress(75)

                # Use the actual path from Snowflake directory table
                success, result = analyzer.analyze_image(SNOWFLAKE_STAGE_NAME, snowflake_file_path, prompt, model)

                if not success:
                    st.error(result)
                    analyzer.close()
                    st.stop()

                progress_bar.progress(100)
                status_text.text("✅ Analysis complete!")

                # Display results
                st.markdown("---")
                st.markdown("### 📊 Analysis Output")

                # Parse and display structured results
                try:
                    if isinstance(result, dict):
                        # Extract AI result and metadata
                        ai_result = result.get('ai_result', {})
                        metadata = result.get('metadata', {})

                        # Display metadata if available
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

                        # Display full JSON structure
                        st.json(ai_result)

                        # Extract message content if available
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

                except Exception as e:
                    st.warning(f"⚠️ Could not parse structured output: {str(e)}")
                    st.text(str(result))

                analyzer.close()
        else:
            st.info("👆 Upload an image to begin analysis")


if __name__ == "__main__":
    main()
