"""
Azure Blob Storage uploader module
"""

import streamlit as st
from azure.storage.blob import BlobServiceClient, ContentSettings
from datetime import datetime
import os
import re
from config import AZURE_CONTAINER_NAME


class AzureBlobUploader:
    """Handles Azure Blob Storage operations"""

    def __init__(self):
        self.connection_string = st.secrets.get("azure", {}).get("connection_string")
        self.container_name = AZURE_CONTAINER_NAME

    @staticmethod
    def sanitize_filename(filename: str) -> str:
        """
        Sanitize filename to avoid issues with Snowflake stage paths
        - Remove special characters
        - Replace spaces with underscores
        - Keep only alphanumeric, underscores, hyphens, and dots

        Args:
            filename: Original filename

        Returns:
            Sanitized filename safe for Snowflake
        """
        # Get file extension
        name, ext = os.path.splitext(filename)

        # Replace spaces and special chars with underscores
        name = re.sub(r'[^\w\-]', '_', name)

        # Remove consecutive underscores
        name = re.sub(r'_+', '_', name)

        # Trim underscores from start/end
        name = name.strip('_')

        # Ensure extension is lowercase
        ext = ext.lower()

        return f"{name}{ext}"

    def upload_image(self, uploaded_file) -> tuple[bool, str, str]:
        """
        Upload image to Azure Blob Storage with sanitized filename

        Args:
            uploaded_file: Streamlit UploadedFile object

        Returns:
            tuple: (success: bool, blob_url_or_error: str, relative_path: str)
        """
        try:
            if not self.connection_string:
                return False, "❌ Azure connection string not configured in secrets", ""

            blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            container_client = blob_service_client.get_container_client(
                self.container_name
            )

            # Create unique blob name with timestamp and sanitized filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            sanitized_name = self.sanitize_filename(uploaded_file.name)
            blob_name = f"images/{timestamp}_{sanitized_name}"

            # Upload with content type
            blob_client = container_client.get_blob_client(blob_name)
            content_settings = ContentSettings(content_type=uploaded_file.type)

            uploaded_file.seek(0)
            blob_client.upload_blob(
                uploaded_file,
                overwrite=True,
                content_settings=content_settings
            )

            blob_url = blob_client.url

            # Return both URL and relative path (for Snowflake stage)
            return True, blob_url, blob_name

        except Exception as e:
            return False, f"❌ Azure upload error: {str(e)}", ""
