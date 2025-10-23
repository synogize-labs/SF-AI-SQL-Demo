"""
Configuration constants for the AI Image Analysis application
"""

# Supported image formats
SUPPORTED_FORMATS = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}

# Size limits in MB
MAX_SIZE_MB_GENERAL = 10
MAX_SIZE_MB_CLAUDE = 3.75

# Resolution limits
MAX_RESOLUTION_CLAUDE = 8000

# Azure Storage
AZURE_CONTAINER_NAME = "construction-building-defects"

# Snowflake Stage Configuration
SNOWFLAKE_STAGE_NAME = "input_stage"  # Match the SQL script

# Available AI models for image analysis
AVAILABLE_MODELS = {
    "OpenAI": [
        "openai-gpt-4.1",
        "openai-o4-mini"
    ],
    "Anthropic (Claude)": [
        "claude-3.5-sonnet",
        "claude-3.7-sonnet",
        "claude-4-sonnet",
        "claude-4-opus"
    ],
    "Meta (Llama)": [
        "llama4-maverick",
        "llama4-scout"
    ],
    "Mistral": [
        "pixtral-large"
    ]
}

# Models with special size/resolution limits
CLAUDE_MODELS = [
    "claude-3.5-sonnet",
    "claude-3.7-sonnet",
    "claude-4-sonnet",
    "claude-4-opus"
]
