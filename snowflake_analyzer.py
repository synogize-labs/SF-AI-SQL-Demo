"""
Snowflake AI analysis module using Cortex AI_COMPLETE
"""

import streamlit as st
import snowflake.connector
import json


class SnowflakeAIAnalyzer:
    """Handles Snowflake AI_COMPLETE operations"""

    def __init__(self):
        self.connection = None

    def connect(self):
        """
        Establish Snowflake connection

        Returns:
            tuple: (success: bool, message: str)
        """
        try:
            sf_config = st.secrets.get("snowflake", {})

            # Build connection parameters
            conn_params = {
                "user": sf_config.get("user"),
                "account": sf_config.get("account"),
                "warehouse": sf_config.get("warehouse"),
                "database": sf_config.get("database"),
                "schema": sf_config.get("schema"),
                "role": sf_config.get("role")
            }

            # Add authenticator if specified (e.g., externalbrowser for SSO)
            authenticator = sf_config.get("authenticator")
            if authenticator:
                conn_params["authenticator"] = authenticator
            else:
                # Use password authentication if no authenticator specified
                conn_params["password"] = sf_config.get("password")

            self.connection = snowflake.connector.connect(**conn_params)
            return True, "✅ Connected to Snowflake"
        except Exception as e:
            return False, f"❌ Snowflake connection error: {str(e)}"

    def analyze_image(self, stage_name: str, file_path: str, prompt: str, model: str = "openai-gpt-4.1") -> tuple[bool, any]:
        """
        Analyze image using Snowflake AI_COMPLETE with PARSE_JSON wrapper
        Uses CTE structure to extract file metadata and parse AI response

        Args:
            stage_name: Snowflake stage name WITHOUT @ (e.g., 'input_stage')
            file_path: Relative path to image in stage (e.g., 'cls04_057.jpg' or 'images/test.jpg')
            prompt: Analysis prompt for the AI model
            model: AI model name (e.g., 'openai-gpt-4.1', 'claude-3.5-sonnet')

        Returns:
            tuple: (success: bool, result_with_metadata: dict or str)
        """
        try:
            if not self.connection:
                success, msg = self.connect()
                if not success:
                    return False, msg

            cursor = self.connection.cursor()

            # Escape single quotes in prompt and file_path for SQL
            escaped_prompt = prompt.replace("'", "''")
            escaped_file_path = file_path.replace("'", "''")
            stage_ref = f'@{stage_name}'

            # CTE structure matching the SQL script logic
            # Use PARSE_JSON wrapper to ensure consistent JSON response
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
                            prompt => PROMPT('{prompt}', img)
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

            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                container_relpath, file_size_bytes, last_modified, result_json = result

                if not result_json:
                    return False, "❌ Snowflake returned empty result"

                # PARSE_JSON ensures result_json is already parsed dict
                # Add metadata to response
                response_with_metadata = {
                    "ai_result": result_json,
                    "metadata": {
                        "file_path": container_relpath,
                        "file_size_bytes": file_size_bytes,
                        "last_modified": str(last_modified) if last_modified else None
                    }
                }
                return True, response_with_metadata
            else:
                return False, "❌ No result returned from Snowflake"

        except snowflake.connector.errors.ProgrammingError as e:
            return False, f"❌ Snowflake SQL error: {str(e)}"
        except Exception as e:
            return False, f"❌ Analysis error: {str(e)}"
        finally:
            if cursor:
                cursor.close()

    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
