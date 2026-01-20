"""
Configuration module for Snowflake Search Tool.

Reuses the same connection logic as DATA_DIFF_SIMPLE/PYTHON.

Loads configuration from:
1. Environment variables
2. .env file
3. snowflake.json file
"""

import os
import json
from dataclasses import dataclass, field
from typing import Optional, List
from pathlib import Path

# Try to load dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass
class SnowflakeConfig:
    """Snowflake connection configuration."""

    account: str = ""
    user: str = ""
    password: Optional[str] = None
    authenticator: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    role: Optional[str] = None

    @classmethod
    def from_env(cls) -> "SnowflakeConfig":
        """Load configuration from environment variables."""
        return cls(
            account=os.getenv("SNOWFLAKE_ACCOUNT", ""),
            user=os.getenv("SNOWFLAKE_USER", ""),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            authenticator=os.getenv("SNOWFLAKE_AUTHENTICATOR"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role=os.getenv("SNOWFLAKE_ROLE"),
        )

    @classmethod
    def from_json(cls, filepath: str) -> "SnowflakeConfig":
        """Load configuration from JSON file."""
        with open(filepath, "r") as f:
            data = json.load(f)
        return cls(
            account=data.get("account", ""),
            user=data.get("user", ""),
            password=data.get("password"),
            authenticator=data.get("authenticator"),
            warehouse=data.get("warehouse"),
            database=data.get("database"),
            schema=data.get("schema"),
            role=data.get("role"),
        )

    @classmethod
    def auto_load(cls) -> "SnowflakeConfig":
        """Auto-load configuration from available sources."""
        # Try snowflake.json first
        json_paths = [
            "snowflake.json",
            "../snowflake.json",
            os.path.expanduser("~/.snowflake/config.json"),
        ]
        for path in json_paths:
            if os.path.exists(path):
                return cls.from_json(path)

        # Fallback to environment variables
        return cls.from_env()


@dataclass
class SearchConfig:
    """Configuration for search operations."""

    # Default databases to search in
    default_databases: List[str] = field(default_factory=list)

    # Default schemas to search in (if not specified, search all)
    default_schemas: List[str] = field(default_factory=list)

    # Include views in search
    include_views: bool = True

    # Include external tables in search
    include_external_tables: bool = True

    # Case-sensitive search
    case_sensitive: bool = False

    # Maximum results to return
    max_results: int = 1000


def create_env_template(filepath: str = ".env.template") -> None:
    """Create a template .env file."""
    template = """# Snowflake Connection Configuration
# Copy this file to .env and fill in your values

# Required: Account identifier (e.g., xy12345.us-east-1)
SNOWFLAKE_ACCOUNT=

# Required: Username
SNOWFLAKE_USER=

# Authentication: Use either password or authenticator
SNOWFLAKE_PASSWORD=
# SNOWFLAKE_AUTHENTICATOR=externalbrowser

# Optional: Default context
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_ROLE=
"""
    with open(filepath, "w") as f:
        f.write(template)
    print(f"Template created: {filepath}")


if __name__ == "__main__":
    # Test loading configuration
    config = SnowflakeConfig.auto_load()
    print(f"Account: {config.account}")
    print(f"User: {config.user}")
    print(f"Database: {config.database}")
    print(f"Schema: {config.schema}")
