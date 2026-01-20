#!/usr/bin/env python3
"""
Batch Table Comparison Automation Script.

This script automates the comparison of a large number of tables between
SAS exports and Snowflake tables.

Input: CSV file with columns:
    - SAS: Source table name (SAS export in Snowflake stage or table)
    - SNOWFLAKE: Target Snowflake table name
    - PRIMARY_KEY: (Optional) Primary key column(s), comma-separated

Usage:
    python batch_compare.py --input tables.csv --output results
    python batch_compare.py --input tables.csv --output results --mode hash
    python batch_compare.py --input tables.csv --sas-path DB.SCHEMA --sf-path DB.SCHEMA

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
from dataclasses import dataclass, field, asdict
import logging

# Import from existing PYTHON module
from snowflake_compare import (
    SnowparkComparer,
    ComparisonResult,
    SNOWPARK_AVAILABLE,
    get_availability_status,
)
from config import SnowflakeConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class TableMapping:
    """Represents a table mapping from the input CSV."""
    sas_table: str
    snowflake_table: str
    primary_key: Optional[str] = None
    sas_full_path: Optional[str] = None
    snowflake_full_path: Optional[str] = None
    status: str = "pending"
    error: Optional[str] = None


@dataclass
class BatchConfig:
    """Configuration for batch comparison."""
    # Paths where to search for SAS tables (DB.SCHEMA format)
    sas_search_paths: List[str] = field(default_factory=list)

    # Paths where to search for Snowflake tables (DB.SCHEMA format)
    snowflake_search_paths: List[str] = field(default_factory=list)

    # Default comparison mode: 'snowpark', 'hash', 'auto'
    default_mode: str = "auto"

    # Numeric tolerance for comparisons
    numeric_tolerance: float = 0.0001

    # Ignore case in string comparisons
    ignore_case: bool = True

    # Ignore leading/trailing spaces
    ignore_spaces: bool = True

    # Continue on error (don't stop if one comparison fails)
    continue_on_error: bool = True

    # Maximum parallel comparisons (not implemented yet, for future use)
    max_parallel: int = 1


@dataclass
class BatchResult:
    """Result of a batch comparison run."""
    start_time: datetime
    end_time: Optional[datetime] = None
    total_tables: int = 0
    successful: int = 0
    failed: int = 0
    identical: int = 0
    different: int = 0
    skipped: int = 0
    results: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.end_time else None,
            "total_tables": self.total_tables,
            "successful": self.successful,
            "failed": self.failed,
            "identical": self.identical,
            "different": self.different,
            "skipped": self.skipped,
            "success_rate": f"{(self.successful / self.total_tables * 100):.1f}%" if self.total_tables > 0 else "N/A",
            "results": self.results,
        }


class BatchComparer:
    """
    Batch table comparison manager.

    Handles reading input CSV, resolving table paths, running comparisons,
    and generating reports.
    """

    def __init__(
        self,
        config: Optional[BatchConfig] = None,
        snowflake_config: Optional[SnowflakeConfig] = None,
        credentials_file: Optional[str] = None,
    ):
        """
        Initialize the batch comparer.

        Args:
            config: Batch configuration
            snowflake_config: Snowflake connection config
            credentials_file: Path to snowflake.json
        """
        self.config = config or BatchConfig()
        self.snowflake_config = snowflake_config
        self.credentials_file = credentials_file
        self._comparer: Optional[SnowparkComparer] = None
        self._available_tables_cache: Dict[str, List[str]] = {}

    def _get_comparer(self) -> SnowparkComparer:
        """Get or create the Snowpark comparer (single connection)."""
        if self._comparer is None:
            self._comparer = SnowparkComparer(
                snowflake_config=self.snowflake_config,
                credentials_file=self.credentials_file,
            )
        return self._comparer

    def connect(self) -> None:
        """Establish Snowflake connection."""
        comparer = self._get_comparer()
        comparer.connect()
        logger.info("Snowflake connection established")

    def close(self) -> None:
        """Close Snowflake connection."""
        if self._comparer is not None:
            self._comparer.close()
            self._comparer = None
            logger.info("Snowflake connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def read_input_csv(self, filepath: str) -> List[TableMapping]:
        """
        Read the input CSV file with table mappings.

        Expected columns:
            - SAS: Source table name
            - SNOWFLAKE: Target table name
            - PRIMARY_KEY: (Optional) Primary key column(s)

        Args:
            filepath: Path to CSV file

        Returns:
            List of TableMapping objects
        """
        logger.info(f"Reading input file: {filepath}")

        df = pd.read_csv(filepath)

        # Normalize column names
        df.columns = [c.upper().strip() for c in df.columns]

        # Check required columns
        required_cols = ["SAS", "SNOWFLAKE"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing required columns: {missing}. Found: {list(df.columns)}")

        mappings = []
        for _, row in df.iterrows():
            sas_table = str(row["SAS"]).strip() if pd.notna(row["SAS"]) else ""
            sf_table = str(row["SNOWFLAKE"]).strip() if pd.notna(row["SNOWFLAKE"]) else ""

            # Skip empty rows
            if not sas_table or not sf_table:
                continue

            pk = None
            if "PRIMARY_KEY" in df.columns and pd.notna(row.get("PRIMARY_KEY")):
                pk = str(row["PRIMARY_KEY"]).strip()

            mappings.append(TableMapping(
                sas_table=sas_table,
                snowflake_table=sf_table,
                primary_key=pk,
            ))

        logger.info(f"Loaded {len(mappings)} table mappings")
        return mappings

    def list_tables_in_schema(self, database: str, schema: str) -> List[str]:
        """
        List all tables in a database.schema.

        Args:
            database: Database name
            schema: Schema name

        Returns:
            List of table names (uppercase)
        """
        cache_key = f"{database}.{schema}"
        if cache_key in self._available_tables_cache:
            return self._available_tables_cache[cache_key]

        comparer = self._get_comparer()
        session = comparer.connect()

        try:
            query = f"""
                SELECT TABLE_NAME
                FROM {database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema.upper()}'
                AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            """
            result = session.sql(query).collect()
            tables = [row[0].upper() for row in result]
            self._available_tables_cache[cache_key] = tables
            logger.info(f"Found {len(tables)} tables in {cache_key}")
            return tables
        except Exception as e:
            logger.warning(f"Could not list tables in {cache_key}: {e}")
            return []

    def list_files_in_stage(self, stage_path: str) -> List[str]:
        """
        List files in a Snowflake stage.

        Args:
            stage_path: Stage path (e.g., @DB.SCHEMA.STAGE_NAME/path/)

        Returns:
            List of file names
        """
        comparer = self._get_comparer()
        session = comparer.connect()

        try:
            query = f"LIST {stage_path}"
            result = session.sql(query).collect()
            files = [row[0].split("/")[-1] for row in result]
            logger.info(f"Found {len(files)} files in {stage_path}")
            return files
        except Exception as e:
            logger.warning(f"Could not list files in {stage_path}: {e}")
            return []

    def resolve_table_path(
        self,
        table_name: str,
        search_paths: List[str],
    ) -> Optional[str]:
        """
        Resolve a table name to its full path by searching in configured paths.

        Args:
            table_name: Table name to search for
            search_paths: List of DB.SCHEMA paths to search in

        Returns:
            Full path (DB.SCHEMA.TABLE) if found, None otherwise
        """
        # If already fully qualified (contains 2 dots), return as-is
        if table_name.count(".") >= 2:
            return table_name

        # If partially qualified (DB.TABLE or SCHEMA.TABLE), try to complete
        if "." in table_name:
            parts = table_name.split(".")
            if len(parts) == 2:
                # Could be DB.TABLE or SCHEMA.TABLE
                # Try each search path
                for path in search_paths:
                    db, schema = path.split(".")
                    # Try as SCHEMA.TABLE
                    full_path = f"{db}.{table_name}"
                    if self._table_exists(full_path):
                        return full_path
            return table_name  # Return as-is if can't resolve

        # Search in each path
        table_upper = table_name.upper()
        for path in search_paths:
            try:
                db, schema = path.split(".")
                tables = self.list_tables_in_schema(db, schema)
                if table_upper in tables:
                    return f"{db}.{schema}.{table_name}"
            except ValueError:
                logger.warning(f"Invalid search path format: {path} (expected DB.SCHEMA)")
                continue

        return None

    def _table_exists(self, full_path: str) -> bool:
        """Check if a table exists."""
        comparer = self._get_comparer()
        session = comparer.connect()

        try:
            session.sql(f"SELECT 1 FROM {full_path} LIMIT 0").collect()
            return True
        except:
            return False

    def run_comparison(
        self,
        mapping: TableMapping,
        mode: str = "auto",
    ) -> ComparisonResult:
        """
        Run a single table comparison.

        Args:
            mapping: Table mapping with resolved paths
            mode: Comparison mode ('snowpark', 'hash', 'auto')

        Returns:
            ComparisonResult
        """
        comparer = self._get_comparer()

        sas_path = mapping.sas_full_path or mapping.sas_table
        sf_path = mapping.snowflake_full_path or mapping.snowflake_table

        # Determine comparison mode
        if mode == "hash" or (mode == "auto" and not mapping.primary_key):
            return comparer.hash_compare(
                table1=sas_path,
                table2=sf_path,
            )
        else:
            pk_cols = None
            if mapping.primary_key:
                pk_cols = [c.strip() for c in mapping.primary_key.split(",")]

            return comparer.compare(
                table1=sas_path,
                table2=sf_path,
                join_columns=pk_cols,
                abs_tol=self.config.numeric_tolerance,
                ignore_case=self.config.ignore_case,
                ignore_spaces=self.config.ignore_spaces,
            )

    def run_batch(
        self,
        mappings: List[TableMapping],
        mode: str = "auto",
        progress_callback: Optional[callable] = None,
    ) -> BatchResult:
        """
        Run batch comparison for all table mappings.

        Args:
            mappings: List of table mappings
            mode: Comparison mode
            progress_callback: Optional callback for progress updates

        Returns:
            BatchResult with all comparison results
        """
        batch_result = BatchResult(
            start_time=datetime.now(),
            total_tables=len(mappings),
        )

        logger.info("=" * 60)
        logger.info(f"STARTING BATCH COMPARISON: {len(mappings)} tables")
        logger.info("=" * 60)

        for i, mapping in enumerate(mappings, 1):
            logger.info(f"\n[{i}/{len(mappings)}] {mapping.sas_table} <-> {mapping.snowflake_table}")

            if progress_callback:
                progress_callback(i, len(mappings), mapping)

            # Resolve paths
            if not mapping.sas_full_path:
                mapping.sas_full_path = self.resolve_table_path(
                    mapping.sas_table,
                    self.config.sas_search_paths,
                )

            if not mapping.snowflake_full_path:
                mapping.snowflake_full_path = self.resolve_table_path(
                    mapping.snowflake_table,
                    self.config.snowflake_search_paths,
                )

            # Check if tables were found
            if not mapping.sas_full_path:
                logger.warning(f"  SAS table not found: {mapping.sas_table}")
                mapping.status = "skipped"
                mapping.error = f"SAS table not found in paths: {self.config.sas_search_paths}"
                batch_result.skipped += 1
                batch_result.results.append({
                    "sas_table": mapping.sas_table,
                    "snowflake_table": mapping.snowflake_table,
                    "status": "skipped",
                    "error": mapping.error,
                })
                continue

            if not mapping.snowflake_full_path:
                logger.warning(f"  Snowflake table not found: {mapping.snowflake_table}")
                mapping.status = "skipped"
                mapping.error = f"Snowflake table not found in paths: {self.config.snowflake_search_paths}"
                batch_result.skipped += 1
                batch_result.results.append({
                    "sas_table": mapping.sas_table,
                    "snowflake_table": mapping.snowflake_table,
                    "status": "skipped",
                    "error": mapping.error,
                })
                continue

            logger.info(f"  SAS path: {mapping.sas_full_path}")
            logger.info(f"  SF path:  {mapping.snowflake_full_path}")
            logger.info(f"  PK: {mapping.primary_key or 'None (hash mode)'}")

            try:
                result = self.run_comparison(mapping, mode=mode)

                if result.error_message:
                    mapping.status = "error"
                    mapping.error = result.error_message
                    batch_result.failed += 1
                    logger.error(f"  ERROR: {result.error_message}")
                else:
                    mapping.status = "success"
                    batch_result.successful += 1

                    if result.is_identical:
                        batch_result.identical += 1
                        logger.info(f"  RESULT: IDENTICAL ({result.match_percentage:.2f}%)")
                    else:
                        batch_result.different += 1
                        logger.info(f"  RESULT: DIFFERENT ({result.match_percentage:.2f}%)")
                        logger.info(f"    - Matched: {result.matched_rows:,}")
                        logger.info(f"    - Only in SAS: {result.rows_only_in_table1:,}")
                        logger.info(f"    - Only in SF: {result.rows_only_in_table2:,}")

                batch_result.results.append({
                    "sas_table": mapping.sas_table,
                    "snowflake_table": mapping.snowflake_table,
                    "sas_full_path": mapping.sas_full_path,
                    "snowflake_full_path": mapping.snowflake_full_path,
                    "primary_key": mapping.primary_key,
                    "status": mapping.status,
                    "error": mapping.error,
                    "comparison_id": result.comparison_id,
                    "is_identical": result.is_identical,
                    "match_percentage": result.match_percentage,
                    "sas_row_count": result.table1_row_count,
                    "sf_row_count": result.table2_row_count,
                    "matched_rows": result.matched_rows,
                    "rows_only_in_sas": result.rows_only_in_table1,
                    "rows_only_in_sf": result.rows_only_in_table2,
                    "execution_time": result.execution_time_seconds,
                })

            except Exception as e:
                mapping.status = "error"
                mapping.error = str(e)
                batch_result.failed += 1
                logger.error(f"  FAILED: {e}")

                if not self.config.continue_on_error:
                    raise

                batch_result.results.append({
                    "sas_table": mapping.sas_table,
                    "snowflake_table": mapping.snowflake_table,
                    "status": "error",
                    "error": str(e),
                })

        batch_result.end_time = datetime.now()

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("BATCH COMPARISON COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Total: {batch_result.total_tables}")
        logger.info(f"Successful: {batch_result.successful}")
        logger.info(f"Failed: {batch_result.failed}")
        logger.info(f"Skipped: {batch_result.skipped}")
        logger.info(f"Identical: {batch_result.identical}")
        logger.info(f"Different: {batch_result.different}")
        duration = (batch_result.end_time - batch_result.start_time).total_seconds()
        logger.info(f"Duration: {duration:.1f} seconds")

        return batch_result

    def export_results(
        self,
        batch_result: BatchResult,
        output_path: str,
        format: str = "excel",
    ) -> str:
        """
        Export batch results to file.

        Args:
            batch_result: Batch comparison results
            output_path: Output file path (without extension)
            format: Output format ('csv', 'excel', 'json')

        Returns:
            Path to exported file
        """
        df = pd.DataFrame(batch_result.results)

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
                        "Identical",
                        "Different",
                        "Success Rate",
                    ],
                    "Value": [
                        batch_result.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                        batch_result.end_time.strftime("%Y-%m-%d %H:%M:%S") if batch_result.end_time else "N/A",
                        f"{(batch_result.end_time - batch_result.start_time).total_seconds():.1f}" if batch_result.end_time else "N/A",
                        batch_result.total_tables,
                        batch_result.successful,
                        batch_result.failed,
                        batch_result.skipped,
                        batch_result.identical,
                        batch_result.different,
                        f"{(batch_result.successful / batch_result.total_tables * 100):.1f}%" if batch_result.total_tables > 0 else "N/A",
                    ],
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name="Summary", index=False)

                # Details sheet
                df.to_excel(writer, sheet_name="Details", index=False)

                # Identical tables
                identical_df = df[df.get("is_identical") == True]
                if len(identical_df) > 0:
                    identical_df.to_excel(writer, sheet_name="Identical", index=False)

                # Different tables
                different_df = df[df.get("is_identical") == False]
                if len(different_df) > 0:
                    different_df.to_excel(writer, sheet_name="Different", index=False)

                # Errors
                error_df = df[df["status"].isin(["error", "skipped"])]
                if len(error_df) > 0:
                    error_df.to_excel(writer, sheet_name="Errors", index=False)

        elif format == "json":
            filepath = f"{output_path}.json"
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(batch_result.to_dict(), f, indent=2, default=str)

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
    help="Input CSV file with table mappings (columns: SAS, SNOWFLAKE, PRIMARY_KEY)",
)
@click.option(
    "--output", "-o",
    "output_path",
    default="comparison_results",
    help="Output file path (without extension)",
)
@click.option(
    "--format", "-f",
    type=click.Choice(["csv", "excel", "json"]),
    default="excel",
    help="Output format",
)
@click.option(
    "--sas-path",
    multiple=True,
    help="DB.SCHEMA path(s) to search for SAS tables (can specify multiple)",
)
@click.option(
    "--sf-path",
    multiple=True,
    help="DB.SCHEMA path(s) to search for Snowflake tables (can specify multiple)",
)
@click.option(
    "--mode", "-m",
    type=click.Choice(["auto", "snowpark", "hash"]),
    default="auto",
    help="Comparison mode: auto (use PK if available), snowpark (require PK), hash (no PK)",
)
@click.option(
    "--tolerance", "-t",
    type=float,
    default=0.0001,
    help="Numeric tolerance for comparisons",
)
@click.option(
    "--credentials", "-c",
    type=click.Path(exists=True),
    help="Path to snowflake.json credentials file",
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
    sas_path: tuple,
    sf_path: tuple,
    mode: str,
    tolerance: float,
    credentials: Optional[str],
    stop_on_error: bool,
):
    """
    Batch compare tables between SAS exports and Snowflake.

    Reads a CSV file with table mappings and runs comparisons for each pair.

    Example CSV format:

        SAS,SNOWFLAKE,PRIMARY_KEY
        SAS_TABLE1,SF_TABLE1,ID
        SAS_TABLE2,SF_TABLE2,COL1,COL2
        SAS_TABLE3,SF_TABLE3,

    Examples:

        # Basic usage
        python batch_compare.py -i tables.csv -o results

        # Specify search paths
        python batch_compare.py -i tables.csv --sas-path TEAM_DB.SAS_DATA --sf-path TEAM_DB.PROD

        # Multiple search paths
        python batch_compare.py -i tables.csv --sas-path DB1.SCHEMA1 --sas-path DB2.SCHEMA2

        # Hash mode (no primary key needed)
        python batch_compare.py -i tables.csv --mode hash
    """
    if not SNOWPARK_AVAILABLE:
        click.echo("Error: Snowpark not available. Install snowflake-snowpark-python (Python 3.9-3.13).", err=True)
        click.echo(get_availability_status())
        sys.exit(2)

    # Build configuration
    config = BatchConfig(
        sas_search_paths=list(sas_path) if sas_path else [],
        snowflake_search_paths=list(sf_path) if sf_path else [],
        default_mode=mode,
        numeric_tolerance=tolerance,
        continue_on_error=not stop_on_error,
    )

    click.echo("=" * 60)
    click.echo("BATCH TABLE COMPARISON")
    click.echo("=" * 60)
    click.echo(f"Input file: {input_file}")
    click.echo(f"Output: {output_path}.{format}")
    click.echo(f"Mode: {mode}")
    click.echo(f"SAS paths: {config.sas_search_paths or 'None (use full paths in CSV)'}")
    click.echo(f"SF paths: {config.snowflake_search_paths or 'None (use full paths in CSV)'}")
    click.echo()

    try:
        with BatchComparer(config=config, credentials_file=credentials) as comparer:
            # Read input
            mappings = comparer.read_input_csv(input_file)

            if not mappings:
                click.echo("No table mappings found in input file.", err=True)
                sys.exit(1)

            # Run batch comparison
            result = comparer.run_batch(mappings, mode=mode)

            # Export results
            comparer.export_results(result, output_path, format=format)

            # Exit code based on results
            if result.failed > 0:
                sys.exit(2)
            elif result.different > 0:
                sys.exit(1)
            else:
                sys.exit(0)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(2)


if __name__ == "__main__":
    main()
