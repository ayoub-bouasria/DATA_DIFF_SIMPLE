#!/usr/bin/env python3
"""
Command-line interface for Snowflake Data Comparison Tool.

Usage:
    python run_comparison.py compare TABLE1 TABLE2 [--pk COL1,COL2] [--mode snowpark|local]
    python run_comparison.py batch config.yaml
    python run_comparison.py status
"""

import click
import yaml
import sys
import os
from datetime import datetime
from typing import Optional, List

from snowflake_compare import (
    SnowparkComparer,
    LocalFileComparer,
    ComparisonResult,
    SNOWPARK_AVAILABLE,
    get_availability_status,
)
from config import SnowflakeConfig, ComparisonConfig


def is_local_file(path: str) -> bool:
    """Check if the path looks like a local file."""
    if os.path.exists(path):
        return True
    lower_path = path.lower()
    return any(lower_path.endswith(ext) for ext in ['.csv', '.xlsx', '.xls', '.parquet', '.json'])


def get_comparer(table1: str, table2: str, mode: str = "auto"):
    """Get the appropriate comparer based on input type and mode."""
    # Auto-detect mode
    if mode == "auto":
        if is_local_file(table1) or is_local_file(table2):
            return LocalFileComparer(), "local"
        elif SNOWPARK_AVAILABLE:
            return SnowparkComparer(), "snowpark"
        else:
            click.echo("Warning: Snowpark not available, using local file mode.", err=True)
            return LocalFileComparer(), "local"
    elif mode == "snowpark":
        if not SNOWPARK_AVAILABLE:
            click.echo("Error: Snowpark not available. Install snowflake-snowpark-python (Python 3.9-3.13).", err=True)
            sys.exit(2)
        return SnowparkComparer(), "snowpark"
    elif mode == "local":
        return LocalFileComparer(), "local"
    else:
        click.echo(f"Error: Unknown mode '{mode}'. Use 'auto', 'snowpark', or 'local'.", err=True)
        sys.exit(2)


@click.group()
@click.version_option(version="2.0.0")
def cli():
    """Snowflake Data Diff Tool - Compare tables using datacompy.

    Supports:
    - Snowpark remote comparison (recommended for large tables)
    - Local file comparison (CSV, Excel, Parquet, JSON)
    """
    pass


@cli.command()
def status():
    """Show available comparison backends status."""
    click.echo(get_availability_status())


@cli.command()
@click.argument("table1")
@click.argument("table2")
@click.option(
    "--pk",
    "--primary-key",
    "primary_key",
    help="Primary key column(s), comma-separated (required for Snowpark)",
)
@click.option(
    "--columns",
    "-c",
    help="Columns to compare, comma-separated (default: all)",
)
@click.option(
    "--tolerance",
    "-t",
    type=float,
    default=0.0001,
    help="Numeric tolerance for float comparison",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["auto", "snowpark", "local"]),
    default="auto",
    help="Comparison mode: auto (default), snowpark (remote), or local (files)",
)
@click.option(
    "--database",
    "-d",
    help="Snowflake database (for Snowpark mode)",
)
@click.option(
    "--schema1",
    "-s1",
    help="Schema for table 1 (for Snowpark mode)",
)
@click.option(
    "--schema2",
    "-s2",
    help="Schema for table 2 (for Snowpark mode, defaults to schema1)",
)
@click.option(
    "--export",
    "-e",
    type=click.Path(),
    help="Export results to file (without extension)",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["csv", "excel", "json"]),
    default="csv",
    help="Export format",
)
@click.option(
    "--ignore-case/--no-ignore-case",
    default=True,
    help="Ignore case in string comparisons",
)
@click.option(
    "--ignore-spaces/--no-ignore-spaces",
    default=True,
    help="Ignore leading/trailing spaces",
)
def compare(
    table1: str,
    table2: str,
    primary_key: Optional[str],
    columns: Optional[str],
    tolerance: float,
    mode: str,
    database: Optional[str],
    schema1: Optional[str],
    schema2: Optional[str],
    export: Optional[str],
    format: str,
    ignore_case: bool,
    ignore_spaces: bool,
):
    """Compare two Snowflake tables or local files.

    Examples:

        # Compare Snowflake tables (Snowpark mode)
        python run_comparison.py compare TABLE1 TABLE2 --pk ID --mode snowpark

        # Compare with different schemas
        python run_comparison.py compare TABLE1 TABLE2 --pk ID -d MYDB -s1 SCHEMA1 -s2 SCHEMA2

        # Compare local CSV files
        python run_comparison.py compare file1.csv file2.csv --pk ID

        # Hash-based comparison (no primary key, local files only)
        python run_comparison.py compare file1.csv file2.csv --mode local
    """
    # Parse primary key columns
    pk_cols = None
    if primary_key:
        pk_cols = [c.strip() for c in primary_key.split(",")]

    click.echo("=" * 60)
    click.echo("SNOWFLAKE DATA COMPARISON TOOL")
    click.echo("=" * 60)
    click.echo(f"Table 1: {table1}")
    click.echo(f"Table 2: {table2}")
    if pk_cols:
        click.echo(f"Primary Key: {', '.join(pk_cols)}")
    else:
        click.echo("Primary Key: None (hash-based comparison)")
    click.echo()

    try:
        comparer, actual_mode = get_comparer(table1, table2, mode)
        click.echo(f"Mode: {actual_mode}")
        click.echo()

        with comparer:
            # Build comparison arguments based on mode
            kwargs = {
                "table1": table1,
                "table2": table2,
                "join_columns": pk_cols,
                "abs_tol": tolerance,
                "ignore_spaces": ignore_spaces,
                "ignore_case": ignore_case,
            }

            # Add Snowpark-specific arguments
            if actual_mode == "snowpark":
                if database:
                    kwargs["database"] = database
                if schema1:
                    kwargs["schema1"] = schema1
                if schema2:
                    kwargs["schema2"] = schema2

            result = comparer.compare(**kwargs)

            # Print result
            click.echo(str(result))

            # Print detailed report if available
            if result.datacompy_report and not result.error_message:
                click.echo("\nDETAILED REPORT:")
                click.echo(result.datacompy_report)

            # Show unique rows if any
            if result.df1_unq_rows is not None and len(result.df1_unq_rows) > 0:
                click.echo(f"\nRows only in Table 1 (first 10):")
                click.echo(result.df1_unq_rows.head(10).to_string())

            if result.df2_unq_rows is not None and len(result.df2_unq_rows) > 0:
                click.echo(f"\nRows only in Table 2 (first 10):")
                click.echo(result.df2_unq_rows.head(10).to_string())

            # Export if requested
            if export:
                comparer.export_results([result], export, format=format)
                click.echo(f"\nResults exported to: {export}.{format}")

            # Exit code based on result
            if result.error_message:
                click.echo(f"\nERROR: {result.error_message}", err=True)
                sys.exit(2)
            elif not result.is_identical:
                click.echo("\nResult: DIFFERENCES FOUND")
                sys.exit(1)
            else:
                click.echo("\nResult: TABLES ARE IDENTICAL")
                sys.exit(0)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        import traceback
        click.echo(traceback.format_exc(), err=True)
        sys.exit(2)


@cli.command()
@click.argument("config_file", type=click.Path(exists=True))
@click.option(
    "--export",
    "-e",
    type=click.Path(),
    help="Export results to file",
)
@click.option(
    "--format",
    "-f",
    type=click.Choice(["csv", "excel", "json"]),
    default="excel",
    help="Export format",
)
@click.option(
    "--mode",
    "-m",
    type=click.Choice(["auto", "snowpark", "local"]),
    default="auto",
    help="Comparison mode",
)
def batch(config_file: str, export: Optional[str], format: str, mode: str):
    """Run batch comparison from YAML config file.

    Config file format:

        comparisons:
          - table1: TABLE1
            table2: TABLE2
            primary_key: ID

          - table1: file1.csv
            table2: file2.csv
            primary_key: COL1,COL2
    """
    # Load configuration
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    comparisons = config.get("comparisons", [])
    if not comparisons:
        click.echo("No comparisons defined in config file.", err=True)
        sys.exit(1)

    click.echo(f"Running {len(comparisons)} comparison(s)...")
    click.echo()

    results = []
    errors = 0
    differences = 0

    for i, comp in enumerate(comparisons, 1):
        table1 = comp.get("table1")
        table2 = comp.get("table2")
        pk = comp.get("primary_key")

        if not table1 or not table2:
            click.echo(f"Skipping comparison {i}: missing table1 or table2")
            continue

        click.echo(f"[{i}/{len(comparisons)}] {table1} vs {table2}")

        pk_cols = None
        if pk:
            pk_cols = [c.strip() for c in pk.split(",")]

        try:
            comparer, actual_mode = get_comparer(table1, table2, mode)

            with comparer:
                result = comparer.compare(
                    table1=table1,
                    table2=table2,
                    join_columns=pk_cols,
                    abs_tol=comp.get("tolerance", 0.0001),
                    ignore_spaces=comp.get("ignore_spaces", True),
                    ignore_case=comp.get("ignore_case", True),
                    database=comp.get("database"),
                    schema1=comp.get("schema1"),
                    schema2=comp.get("schema2"),
                )

                results.append(result)

                if result.error_message:
                    click.echo(f"  ERROR: {result.error_message}")
                    errors += 1
                elif result.is_identical:
                    click.echo(f"  IDENTICAL ({result.match_percentage:.2f}%)")
                else:
                    click.echo(f"  DIFFERENT ({result.match_percentage:.2f}%)")
                    differences += 1

        except Exception as e:
            click.echo(f"  FAILED: {e}")
            errors += 1

    # Export results if comparer available
    if export and results:
        try:
            comparer, _ = get_comparer(results[0].table1_name, results[0].table2_name, mode)
            with comparer:
                comparer.export_results(results, export, format=format)
                click.echo(f"\nResults exported to: {export}.{format}")
        except Exception as e:
            click.echo(f"Export failed: {e}", err=True)

    # Summary
    click.echo()
    click.echo("=" * 60)
    click.echo("SUMMARY")
    click.echo("=" * 60)
    click.echo(f"Total comparisons: {len(results)}")
    click.echo(f"Identical: {len(results) - differences - errors}")
    click.echo(f"Different: {differences}")
    click.echo(f"Errors: {errors}")

    if errors > 0:
        sys.exit(2)
    elif differences > 0:
        sys.exit(1)
    else:
        sys.exit(0)


@cli.command()
def init():
    """Initialize configuration files."""
    from config import create_env_template

    # Create .env template
    create_env_template(".env.template")

    # Create example batch config
    example_config = """# Batch comparison configuration
# Run with: python run_comparison.py batch config.yaml

# Default settings (optional)
defaults:
  database: TEAM_DB
  schema1: EXTERNAL
  tolerance: 0.0001
  ignore_case: true
  ignore_spaces: true

# Comparisons to run
comparisons:
  # Snowflake tables (Snowpark mode)
  - table1: TABLE1
    table2: TABLE2
    primary_key: ID
    database: TEAM_DB
    schema1: SCHEMA1
    schema2: SCHEMA2

  # Composite primary key
  - table1: TABLE3
    table2: TABLE4
    primary_key: COL1,COL2

  # Local file comparison
  - table1: data/file1.csv
    table2: data/file2.csv
    primary_key: ID
"""

    with open("batch_config.yaml.template", "w") as f:
        f.write(example_config)

    # Create snowflake.json template
    snowflake_json = """{
    "account": "your_account.region",
    "user": "your_username",
    "password": "your_password",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema",
    "role": "your_role"
}
"""
    with open("snowflake.json.template", "w") as f:
        f.write(snowflake_json)

    click.echo("Configuration templates created:")
    click.echo("  - .env.template (copy to .env and configure)")
    click.echo("  - batch_config.yaml.template (example batch config)")
    click.echo("  - snowflake.json.template (Snowpark credentials)")


if __name__ == "__main__":
    cli()
