#!/usr/bin/env python3
"""
Command-line interface for Snowflake Data Comparison Tool.

Usage:
    python run_comparison.py compare TABLE1 TABLE2 [--pk COL1,COL2]
    python run_comparison.py batch config.yaml
    python run_comparison.py test
"""

import click
import yaml
import sys
from datetime import datetime
from typing import Optional, List

from snowflake_compare import SnowflakeDataComparer, ComparisonResult
from config import SnowflakeConfig, ComparisonConfig


@click.group()
@click.version_option(version="1.0.0")
def cli():
    """Snowflake Data Diff Tool - Compare tables using datacompy."""
    pass


@cli.command()
@click.argument("table1")
@click.argument("table2")
@click.option(
    "--pk",
    "--primary-key",
    "primary_key",
    help="Primary key column(s), comma-separated",
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
    "--case-sensitive/--no-case-sensitive",
    default=True,
    help="Case-sensitive string comparison",
)
@click.option(
    "--where1",
    help="WHERE clause filter for table 1",
)
@click.option(
    "--where2",
    help="WHERE clause filter for table 2",
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
def compare(
    table1: str,
    table2: str,
    primary_key: Optional[str],
    columns: Optional[str],
    tolerance: float,
    case_sensitive: bool,
    where1: Optional[str],
    where2: Optional[str],
    export: Optional[str],
    format: str,
):
    """Compare two Snowflake tables."""

    # Parse primary key columns
    pk_cols = None
    if primary_key:
        pk_cols = [c.strip() for c in primary_key.split(",")]

    # Parse columns to compare
    compare_cols = None
    if columns:
        compare_cols = [c.strip() for c in columns.split(",")]

    click.echo(f"Comparing tables:")
    click.echo(f"  Table 1: {table1}")
    click.echo(f"  Table 2: {table2}")
    if pk_cols:
        click.echo(f"  Primary Key: {', '.join(pk_cols)}")
    else:
        click.echo("  Method: Hash comparison (no primary key)")
    click.echo()

    try:
        with SnowflakeDataComparer() as comparer:
            result = comparer.compare(
                table1=table1,
                table2=table2,
                join_columns=pk_cols,
                columns_to_compare=compare_cols,
                numeric_tolerance=tolerance,
                case_sensitive=case_sensitive,
                where_clause1=where1,
                where_clause2=where2,
            )

            # Print result
            click.echo(str(result))

            # Export if requested
            if export:
                comparer.export_results([result], export, format=format)

            # Exit code based on result
            if result.error_message:
                click.echo(f"ERROR: {result.error_message}", err=True)
                sys.exit(2)
            elif not result.is_identical:
                sys.exit(1)
            else:
                sys.exit(0)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
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
def batch(config_file: str, export: Optional[str], format: str):
    """Run batch comparison from YAML config file."""

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

    with SnowflakeDataComparer() as comparer:
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

            result = comparer.compare(
                table1=table1,
                table2=table2,
                join_columns=pk_cols,
                columns_to_compare=comp.get("columns"),
                numeric_tolerance=comp.get("tolerance", 0.0001),
                case_sensitive=comp.get("case_sensitive", True),
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

        # Export results
        if export:
            comparer.export_results(results, export, format=format)

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
def test():
    """Run test comparisons to verify setup."""

    click.echo("Running test comparisons...")
    click.echo()

    # Test data
    test_cases = [
        {
            "name": "Simple comparison with PK",
            "table1": "TEST_TABLE_IDENTICAL_A",
            "table2": "TEST_TABLE_IDENTICAL_B",
            "pk": ["ID"],
            "expected_identical": True,
        },
        {
            "name": "Comparison with differences",
            "table1": "TEST_TABLE_A",
            "table2": "TEST_TABLE_B",
            "pk": ["ID"],
            "expected_identical": False,
        },
        {
            "name": "Hash comparison (no PK)",
            "table1": "TEST_TABLE_NO_PK_A",
            "table2": "TEST_TABLE_NO_PK_B",
            "pk": None,
            "expected_identical": False,
        },
    ]

    passed = 0
    failed = 0

    try:
        with SnowflakeDataComparer() as comparer:
            for test in test_cases:
                click.echo(f"Testing: {test['name']}")

                try:
                    result = comparer.compare(
                        table1=test["table1"],
                        table2=test["table2"],
                        join_columns=test["pk"],
                    )

                    if result.error_message:
                        click.echo(f"  SKIP (table may not exist): {result.error_message}")
                    elif result.is_identical == test["expected_identical"]:
                        click.echo(f"  PASS - {'Identical' if result.is_identical else 'Different'} as expected")
                        passed += 1
                    else:
                        click.echo(f"  FAIL - Expected {'identical' if test['expected_identical'] else 'different'}")
                        failed += 1

                except Exception as e:
                    click.echo(f"  ERROR: {e}")
                    failed += 1

    except Exception as e:
        click.echo(f"Connection error: {e}")
        click.echo("Make sure your .env file is configured correctly.")
        sys.exit(2)

    click.echo()
    click.echo(f"Results: {passed} passed, {failed} failed")

    sys.exit(0 if failed == 0 else 1)


@cli.command()
def init():
    """Initialize configuration files."""

    from config import create_env_template

    # Create .env template
    create_env_template(".env.template")

    # Create example batch config
    example_config = """# Batch comparison configuration
# Run with: python run_comparison.py batch config.yaml

comparisons:
  - table1: TEAM_DB.EXTERNAL.TABLE1
    table2: TEAM_DB.EXTERNAL.TABLE2
    primary_key: ID
    tolerance: 0.01

  - table1: TEAM_DB.EXTERNAL.TABLE3
    table2: TEAM_DB.EXTERNAL.TABLE4
    primary_key: COL1,COL2  # Composite key
    columns:
      - COL3
      - COL4
      - COL5

  - table1: TEAM_DB.EXTERNAL.TABLE5
    table2: TEAM_DB.EXTERNAL.TABLE6
    # No primary_key = hash comparison
"""

    with open("batch_config.yaml.template", "w") as f:
        f.write(example_config)

    click.echo("Configuration templates created:")
    click.echo("  - .env.template (copy to .env and configure)")
    click.echo("  - batch_config.yaml.template (example batch config)")


if __name__ == "__main__":
    cli()
