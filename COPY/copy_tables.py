#!/usr/bin/env python3
"""
Table Copy Automation Script.

This script automates copying tables from various sources to TEAM_DB.EXTERNAL schema.
Useful for preparing tables before running DATA_DIFF comparisons.

IMPORTANT: All copies are FULLY INDEPENDENT from source tables.
Uses CREATE TABLE AS SELECT (CTAS) to create complete physical copies.
Modifications to copied tables will NEVER affect the original source tables.

Input: CSV file with columns:
    - SOURCE_TABLE: Full path to source table (DB.SCHEMA.TABLE)
    - TARGET_TABLE: (Optional) Target table name in TEAM_DB.EXTERNAL (defaults to source table name)

Usage:
    python copy_tables.py --input tables.csv
    python copy_tables.py --input tables.csv --target-db TEAM_DB --target-schema EXTERNAL
    python copy_tables.py --input tables.csv --drop-existing

Requirements:
    - Python 3.9-3.13
    - snowflake-snowpark-python
    - pandas
    - Configure .env or snowflake.json with credentials
"""

import sys
import os
from pathlib import Path

# Add PYTHON folder to path to reuse existing modules
PYTHON_PATH = Path(__file__).parent.parent / "PYTHON"
sys.path.insert(0, str(PYTHON_PATH))

import pandas as pd
import click
import json
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
import logging

# Import from existing PYTHON module
from config import SnowflakeConfig

# Try to import Snowpark
try:
    from snowflake.snowpark import Session
    SNOWPARK_AVAILABLE = True
except ImportError:
    SNOWPARK_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TableCopyMapping:
    """Represents a table copy mapping from the input CSV."""
    source_table: str
    target_table: Optional[str] = None
    status: str = "pending"
    error: Optional[str] = None
    rows_copied: int = 0
    execution_time: float = 0.0


@dataclass
class CopyConfig:
    """Configuration for table copy operations."""
    # Target database and schema
    target_database: str = "TEAM_DB"
    target_schema: str = "EXTERNAL"

    # Drop existing table before copy
    drop_existing: bool = False

    # Use OR REPLACE instead of DROP + CREATE
    use_or_replace: bool = True

    # Continue on error
    continue_on_error: bool = True

    # Grant permissions after copy (role to grant SELECT to)
    grant_select_to: Optional[str] = None


@dataclass
class CopyResult:
    """Result of a batch copy operation."""
    start_time: datetime
    end_time: Optional[datetime] = None
    total_tables: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    total_rows_copied: int = 0
    results: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.end_time else None,
            "total_tables": self.total_tables,
            "successful": self.successful,
            "failed": self.failed,
            "skipped": self.skipped,
            "total_rows_copied": self.total_rows_copied,
            "success_rate": f"{(self.successful / self.total_tables * 100):.1f}%" if self.total_tables > 0 else "N/A",
            "results": self.results,
        }


class TableCopier:
    """
    Table copy manager.

    Handles reading input CSV, copying tables to target schema,
    and generating reports.
    """

    def __init__(
        self,
        config: Optional[CopyConfig] = None,
        snowflake_config: Optional[SnowflakeConfig] = None,
        credentials_file: Optional[str] = None,
    ):
        """
        Initialize the table copier.

        Args:
            config: Copy configuration
            snowflake_config: Snowflake connection config
            credentials_file: Path to snowflake.json
        """
        self.config = config or CopyConfig()
        self.snowflake_config = snowflake_config
        self.credentials_file = credentials_file
        self._session: Optional[Session] = None

    def _get_connection_params(self) -> Dict[str, Any]:
        """Get Snowflake connection parameters with SSO authentication."""
        # Try credentials file first
        if self.credentials_file and os.path.exists(self.credentials_file):
            with open(self.credentials_file, "r") as f:
                params = json.load(f)
                # Force SSO if no password provided
                if "password" not in params:
                    params["authenticator"] = "externalbrowser"
                return params

        # Try snowflake_config
        if self.snowflake_config:
            params = {
                "account": self.snowflake_config.account,
                "user": self.snowflake_config.user,
                "authenticator": "externalbrowser",  # SSO by default
            }
            if self.snowflake_config.warehouse:
                params["warehouse"] = self.snowflake_config.warehouse
            if self.snowflake_config.database:
                params["database"] = self.snowflake_config.database
            if self.snowflake_config.schema:
                params["schema"] = self.snowflake_config.schema
            if self.snowflake_config.role:
                params["role"] = self.snowflake_config.role
            return params

        # Try auto-load from environment or config file
        config = SnowflakeConfig.auto_load()
        params = {
            "account": config.account,
            "user": config.user,
            "authenticator": "externalbrowser",  # SSO by default
        }
        if config.warehouse:
            params["warehouse"] = config.warehouse
        if config.database:
            params["database"] = config.database
        if config.schema:
            params["schema"] = config.schema
        if config.role:
            params["role"] = config.role
        return params

    def connect(self) -> Session:
        """Establish Snowflake connection via SSO."""
        if self._session is None:
            params = self._get_connection_params()
            logger.info(f"Connecting to Snowflake account: {params.get('account')}")
            logger.info("Opening browser for SSO authentication...")
            self._session = Session.builder.configs(params).create()
            logger.info("Snowflake connection established via SSO")
        return self._session

    def close(self) -> None:
        """Close Snowflake connection."""
        if self._session is not None:
            self._session.close()
            self._session = None
            logger.info("Snowflake connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def read_input_csv(self, filepath: str) -> List[TableCopyMapping]:
        """
        Read the input CSV file with table mappings.

        Expected columns:
            - SOURCE_TABLE: Full path to source table (DB.SCHEMA.TABLE)
            - TARGET_TABLE: (Optional) Target table name

        Args:
            filepath: Path to CSV file

        Returns:
            List of TableCopyMapping objects
        """
        logger.info(f"Reading input file: {filepath}")

        df = pd.read_csv(filepath)

        # Normalize column names
        df.columns = [c.upper().strip() for c in df.columns]

        # Check required columns
        if "SOURCE_TABLE" not in df.columns:
            raise ValueError(f"Missing required column: SOURCE_TABLE. Found: {list(df.columns)}")

        mappings = []
        for _, row in df.iterrows():
            source = str(row["SOURCE_TABLE"]).strip() if pd.notna(row["SOURCE_TABLE"]) else ""

            # Skip empty rows
            if not source:
                continue

            target = None
            if "TARGET_TABLE" in df.columns and pd.notna(row.get("TARGET_TABLE")):
                target = str(row["TARGET_TABLE"]).strip()

            mappings.append(TableCopyMapping(
                source_table=source,
                target_table=target,
            ))

        logger.info(f"Loaded {len(mappings)} table mappings")
        return mappings

    def _extract_table_name(self, full_path: str) -> str:
        """Extract table name from full path (DB.SCHEMA.TABLE -> TABLE)."""
        return full_path.split(".")[-1]

    def _table_exists(self, database: str, schema: str, table: str) -> bool:
        """Check if a table exists."""
        session = self.connect()
        try:
            query = f"""
                SELECT COUNT(*)
                FROM {database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema.upper()}'
                AND TABLE_NAME = '{table.upper()}'
            """
            result = session.sql(query).collect()
            return result[0][0] > 0
        except Exception:
            return False

    def _get_row_count(self, full_table_path: str) -> int:
        """Get row count of a table."""
        session = self.connect()
        try:
            result = session.sql(f"SELECT COUNT(*) FROM {full_table_path}").collect()
            return result[0][0]
        except Exception:
            return 0

    def _source_exists(self, source_table: str) -> bool:
        """Check if source table exists."""
        session = self.connect()
        try:
            session.sql(f"SELECT 1 FROM {source_table} LIMIT 0").collect()
            return True
        except Exception:
            return False

    def copy_table(self, mapping: TableCopyMapping) -> TableCopyMapping:
        """
        Copy a single table to target schema using CTAS (CREATE TABLE AS SELECT).

        Creates a fully independent physical copy - modifications to the copy
        will NEVER affect the source table.

        Args:
            mapping: Table copy mapping

        Returns:
            Updated mapping with status and results
        """
        session = self.connect()
        start_time = datetime.now()

        source = mapping.source_table
        target_name = mapping.target_table or self._extract_table_name(source)
        target_full = f"{self.config.target_database}.{self.config.target_schema}.{target_name}"

        logger.info(f"  Source: {source}")
        logger.info(f"  Target: {target_full}")
        logger.info(f"  Mode: CTAS (independent physical copy)")

        try:
            # Check if source exists
            if not self._source_exists(source):
                mapping.status = "failed"
                mapping.error = f"Source table does not exist: {source}"
                return mapping

            # Handle existing target table
            target_exists = self._table_exists(
                self.config.target_database,
                self.config.target_schema,
                target_name
            )

            if target_exists:
                if self.config.drop_existing:
                    logger.info(f"  Dropping existing table: {target_full}")
                    session.sql(f"DROP TABLE IF EXISTS {target_full}").collect()
                elif not self.config.use_or_replace:
                    mapping.status = "skipped"
                    mapping.error = f"Target table already exists: {target_full}"
                    return mapping

            # Execute CTAS - Creates a fully independent physical copy
            if self.config.use_or_replace:
                sql = f"CREATE OR REPLACE TABLE {target_full} AS SELECT * FROM {source}"
            else:
                sql = f"CREATE TABLE {target_full} AS SELECT * FROM {source}"

            logger.info(f"  Executing CTAS (full physical copy)...")
            session.sql(sql).collect()

            # Get row count of copied table
            mapping.rows_copied = self._get_row_count(target_full)

            # Grant permissions if configured
            if self.config.grant_select_to:
                grant_sql = f"GRANT SELECT ON TABLE {target_full} TO ROLE {self.config.grant_select_to}"
                try:
                    session.sql(grant_sql).collect()
                    logger.info(f"  Granted SELECT to {self.config.grant_select_to}")
                except Exception as e:
                    logger.warning(f"  Could not grant permissions: {e}")

            mapping.status = "success"
            mapping.execution_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"  SUCCESS: {mapping.rows_copied:,} rows copied in {mapping.execution_time:.1f}s")

        except Exception as e:
            mapping.status = "failed"
            mapping.error = str(e)
            mapping.execution_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"  FAILED: {e}")

        return mapping

    def run_batch(
        self,
        mappings: List[TableCopyMapping],
        progress_callback: Optional[callable] = None,
    ) -> CopyResult:
        """
        Run batch copy for all table mappings.

        Args:
            mappings: List of table mappings
            progress_callback: Optional callback for progress updates

        Returns:
            CopyResult with all copy results
        """
        result = CopyResult(
            start_time=datetime.now(),
            total_tables=len(mappings),
        )

        logger.info("=" * 60)
        logger.info(f"STARTING BATCH COPY: {len(mappings)} tables")
        logger.info(f"Target: {self.config.target_database}.{self.config.target_schema}")
        logger.info("=" * 60)

        for i, mapping in enumerate(mappings, 1):
            logger.info(f"\n[{i}/{len(mappings)}] {mapping.source_table}")

            if progress_callback:
                progress_callback(i, len(mappings), mapping)

            try:
                mapping = self.copy_table(mapping)

                if mapping.status == "success":
                    result.successful += 1
                    result.total_rows_copied += mapping.rows_copied
                elif mapping.status == "skipped":
                    result.skipped += 1
                else:
                    result.failed += 1

                result.results.append({
                    "source_table": mapping.source_table,
                    "target_table": mapping.target_table or self._extract_table_name(mapping.source_table),
                    "target_full_path": f"{self.config.target_database}.{self.config.target_schema}.{mapping.target_table or self._extract_table_name(mapping.source_table)}",
                    "status": mapping.status,
                    "error": mapping.error,
                    "rows_copied": mapping.rows_copied,
                    "execution_time": mapping.execution_time,
                })

            except Exception as e:
                mapping.status = "failed"
                mapping.error = str(e)
                result.failed += 1
                logger.error(f"  FAILED: {e}")

                if not self.config.continue_on_error:
                    raise

                result.results.append({
                    "source_table": mapping.source_table,
                    "target_table": mapping.target_table,
                    "status": "failed",
                    "error": str(e),
                })

        result.end_time = datetime.now()

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("BATCH COPY COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total: {result.total_tables}")
        logger.info(f"Successful: {result.successful}")
        logger.info(f"Failed: {result.failed}")
        logger.info(f"Skipped: {result.skipped}")
        logger.info(f"Total rows copied: {result.total_rows_copied:,}")
        duration = (result.end_time - result.start_time).total_seconds()
        logger.info(f"Duration: {duration:.1f} seconds")

        return result

    def export_results(
        self,
        copy_result: CopyResult,
        output_path: str,
        format: str = "excel",
    ) -> str:
        """
        Export copy results to file.

        Args:
            copy_result: Copy results
            output_path: Output file path (without extension)
            format: Output format ('csv', 'excel', 'json')

        Returns:
            Path to exported file
        """
        df = pd.DataFrame(copy_result.results)

        if format == "csv":
            filepath = f"{output_path}.csv"
            df.to_csv(filepath, index=False)

        elif format == "excel":
            filepath = f"{output_path}.xlsx"
            with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:
                # Summary sheet
                summary_data = {
                    "Metric": [
                        "Start Time",
                        "End Time",
                        "Duration (seconds)",
                        "Total Tables",
                        "Successful",
                        "Failed",
                        "Skipped",
                        "Total Rows Copied",
                        "Success Rate",
                    ],
                    "Value": [
                        copy_result.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        copy_result.end_time.strftime("%Y-%m-%d %H:%M:%S") if copy_result.end_time else "N/A",
                        f"{(copy_result.end_time - copy_result.start_time).total_seconds():.1f}" if copy_result.end_time else "N/A",
                        copy_result.total_tables,
                        copy_result.successful,
                        copy_result.failed,
                        copy_result.skipped,
                        f"{copy_result.total_rows_copied:,}",
                        f"{(copy_result.successful / copy_result.total_tables * 100):.1f}%" if copy_result.total_tables > 0 else "N/A",
                    ],
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name="Summary", index=False)

                # Details sheet
                df.to_excel(writer, sheet_name="Details", index=False)

                # Successful copies
                success_df = df[df["status"] == "success"]
                if len(success_df) > 0:
                    success_df.to_excel(writer, sheet_name="Successful", index=False)

                # Failed copies
                failed_df = df[df["status"].isin(["failed", "skipped"])]
                if len(failed_df) > 0:
                    failed_df.to_excel(writer, sheet_name="Failed", index=False)

        elif format == "json":
            filepath = f"{output_path}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(copy_result.to_dict(), f, indent=2, default=str)

        else:
            raise ValueError(f"Unknown format: {format}")

        logger.info(f"Results exported to: {filepath}")
        return filepath


# =============================================================================
# CLI
# =============================================================================

@click.command()
@click.option(
    "--input", "-i",
    "input_file",
    required=True,
    type=click.Path(exists=True),
    help="Input CSV file with table mappings (columns: SOURCE_TABLE, TARGET_TABLE)",
)
@click.option(
    "--output", "-o",
    "output_path",
    default="copy_results",
    help="Output file path for results (without extension)",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["csv", "excel", "json"]),
    default="excel",
    help="Output format for results",
)
@click.option(
    "--target-db",
    default="TEAM_DB",
    help="Target database (default: TEAM_DB)",
)
@click.option(
    "--target-schema",
    default="EXTERNAL",
    help="Target schema (default: EXTERNAL)",
)
@click.option(
    "--drop-existing/--no-drop-existing",
    default=False,
    help="Drop existing tables before copy",
)
@click.option(
    "--or-replace/--no-or-replace",
    default=True,
    help="Use CREATE OR REPLACE (default: true)",
)
@click.option(
    "--credentials", "-c",
    type=click.Path(exists=True),
    help="Path to snowflake.json credentials file",
)
@click.option(
    "--grant-to",
    help="Role to grant SELECT permission after copy",
)
@click.option(
    "--stop-on-error/--continue-on-error",
    default=False,
    help="Stop on first error or continue with remaining tables",
)
def main(
    input_file: str,
    output_path: str,
    format: str,
    target_db: str,
    target_schema: str,
    drop_existing: bool,
    or_replace: bool,
    credentials: Optional[str],
    grant_to: Optional[str],
    stop_on_error: bool,
):
    """
    Copy tables from various sources to TEAM_DB.EXTERNAL.

    Creates FULLY INDEPENDENT physical copies using CREATE TABLE AS SELECT (CTAS).
    Modifications to copied tables will NEVER affect the original source tables.

    Reads a CSV file with source table paths and copies each table
    to the target schema (default: TEAM_DB.EXTERNAL).

    Example CSV format:

        SOURCE_TABLE,TARGET_TABLE
        DB1.SCHEMA1.TABLE1,TABLE1_COPY
        DB2.SCHEMA2.TABLE2,
        DB3.SCHEMA3.TABLE3,

    Examples:

        # Basic usage - copy tables to TEAM_DB.EXTERNAL
        python copy_tables.py -i tables.csv

        # Specify different target
        python copy_tables.py -i tables.csv --target-db MY_DB --target-schema MY_SCHEMA

        # Drop existing tables before copy
        python copy_tables.py -i tables.csv --drop-existing

        # Grant SELECT to a role after copy
        python copy_tables.py -i tables.csv --grant-to ANALYST_ROLE
    """
    if not SNOWPARK_AVAILABLE:
        click.echo("Error: Snowpark not available. Install snowflake-snowpark-python (Python 3.9-3.13).", err=True)
        sys.exit(2)

    # Build configuration
    config = CopyConfig(
        target_database=target_db,
        target_schema=target_schema,
        drop_existing=drop_existing,
        use_or_replace=or_replace,
        continue_on_error=not stop_on_error,
        grant_select_to=grant_to,
    )

    click.echo("=" * 60)
    click.echo("TABLE COPY UTILITY")
    click.echo("=" * 60)
    click.echo(f"Input file: {input_file}")
    click.echo(f"Target: {target_db}.{target_schema}")
    click.echo(f"Mode: CTAS (independent physical copy)")
    click.echo(f"Drop existing: {drop_existing}")
    click.echo(f"Use OR REPLACE: {or_replace}")
    if grant_to:
        click.echo(f"Grant SELECT to: {grant_to}")
    click.echo()

    try:
        with TableCopier(config=config, credentials_file=credentials) as copier:
            # Read input
            mappings = copier.read_input_csv(input_file)

            if not mappings:
                click.echo("No table mappings found in input file.", err=True)
                sys.exit(1)

            # Run batch copy
            result = copier.run_batch(mappings)

            # Export results
            copier.export_results(result, output_path, format=format)

            # Exit code based on results
            if result.failed > 0:
                sys.exit(2)
            else:
                sys.exit(0)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
