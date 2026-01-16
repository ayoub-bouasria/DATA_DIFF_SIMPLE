"""
Configuration module for Snowflake Data Diff Tool.

This module handles Snowflake connection configuration from environment
variables or a .env file.
"""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""

    account: str
    user: str
    password: Optional[str] = None
    authenticator: Optional[str] = None  # 'externalbrowser' for SSO, 'oauth' for Azure AD token
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    role: Optional[str] = None
    token: Optional[str] = None  # OAuth token for Azure AD authentication

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Create configuration from environment variables."""
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE", "TEAM_DB"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "EXTERNAL"),
            role=os.getenv("SNOWFLAKE_ROLE"),
            token=os.getenv("SNOWFLAKE_OAUTH_TOKEN"),
        )

    def to_connection_params(self) -> dict:
        """Convert to Snowflake connector parameters."""
        params = {
            "account": self.account,
            "user": self.user,
        }

        # Authentication priority: OAuth token > SSO > password
        if self.authenticator == "oauth" and self.token:
            params["authenticator"] = "oauth"
            params["token"] = self.token
        elif self.authenticator:
            params["authenticator"] = self.authenticator
        elif self.password:
            params["password"] = self.password

        # Optional parameters
        if self.warehouse:
            params["warehouse"] = self.warehouse
        if self.database:
            params["database"] = self.database
        if self.schema:
            params["schema"] = self.schema
        if self.role:
            params["role"] = self.role

        return params


# Default comparison settings
@dataclass
class ComparisonConfig:
    """Configuration for data comparison."""

    # Numeric tolerance for floating point comparisons
    numeric_tolerance: float = 0.0001

    # Case sensitivity
    case_sensitive: bool = True

    # Max rows to display in detailed diff
    max_diff_rows: int = 100

    # Export format options
    export_format: str = "csv"  # csv, excel, html

    # Batch size for large table comparisons
    batch_size: int = 100000


# Example .env file content
ENV_TEMPLATE = """
# Snowflake Connection Settings
# =============================

# Account identifier (e.g., xy12345.eu-west-1)
SNOWFLAKE_ACCOUNT=your_account

# Username (email for Azure AD)
SNOWFLAKE_USER=your_username

# Password (leave empty if using SSO or Azure AD)
SNOWFLAKE_PASSWORD=your_password

# Authentication method - choose ONE:
# -----------------------------------
# Option 1: Azure AD SSO via browser (interactive)
# SNOWFLAKE_AUTHENTICATOR=externalbrowser

# Option 2: Azure AD OAuth token (for automation/scripts)
# SNOWFLAKE_AUTHENTICATOR=oauth
# SNOWFLAKE_OAUTH_TOKEN=your_azure_ad_token

# Option 3: Username/password (legacy)
# Leave SNOWFLAKE_AUTHENTICATOR empty and set SNOWFLAKE_PASSWORD

# Default warehouse
SNOWFLAKE_WAREHOUSE=your_warehouse

# Default database
SNOWFLAKE_DATABASE=TEAM_DB

# Default schema
SNOWFLAKE_SCHEMA=EXTERNAL

# Role (optional)
# SNOWFLAKE_ROLE=your_role
"""


def create_env_template(filepath: str = ".env.template") -> None:
    """Create a template .env file."""
    with open(filepath, "w") as f:
        f.write(ENV_TEMPLATE)
    print(f"Template .env file created: {filepath}")


if __name__ == "__main__":
    # Create template .env file when run directly
    create_env_template()
