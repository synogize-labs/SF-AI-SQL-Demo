"""
Image validation module for Snowflake AI_COMPLETE requirements
"""

import os
from PIL import Image
from config import (
    SUPPORTED_FORMATS,
    MAX_SIZE_MB_GENERAL,
    MAX_SIZE_MB_CLAUDE,
    MAX_RESOLUTION_CLAUDE,
    CLAUDE_MODELS
)


class ImageValidator:
    """Validates images against Snowflake AI_COMPLETE requirements"""

    @staticmethod
    def validate_image(uploaded_file, model_name: str = "gpt-4o"):
        """
        Validate image against Snowflake AI_COMPLETE limitations

        Args:
            uploaded_file: Streamlit UploadedFile object
            model_name: AI model name to determine validation rules

        Returns:
            tuple: (is_valid: bool, error_message: str or None)
        """
        errors = []

        # Check file format
        file_ext = os.path.splitext(uploaded_file.name)[1].lower()
        if file_ext not in SUPPORTED_FORMATS:
            errors.append(
                f"❌ Unsupported format: {file_ext}. "
                f"Supported formats: {', '.join(SUPPORTED_FORMATS)}"
            )

        # Check file size
        file_size_mb = uploaded_file.size / (1024 * 1024)
        is_claude = model_name in CLAUDE_MODELS
        max_size = MAX_SIZE_MB_CLAUDE if is_claude else MAX_SIZE_MB_GENERAL

        if file_size_mb > max_size:
            errors.append(
                f"❌ File size ({file_size_mb:.2f} MB) exceeds limit "
                f"({max_size} MB) for {model_name}"
            )

        # Check resolution for Claude models
        if is_claude:
            try:
                image = Image.open(uploaded_file)
                width, height = image.size
                if width > MAX_RESOLUTION_CLAUDE or height > MAX_RESOLUTION_CLAUDE:
                    errors.append(
                        f"❌ Image resolution ({width}x{height}) exceeds Claude limit "
                        f"({MAX_RESOLUTION_CLAUDE}x{MAX_RESOLUTION_CLAUDE} pixels)"
                    )
                uploaded_file.seek(0)  # Reset file pointer
            except Exception as e:
                errors.append(f"❌ Cannot read image dimensions: {str(e)}")

        if errors:
            return False, "\n".join(errors)
        return True, None
