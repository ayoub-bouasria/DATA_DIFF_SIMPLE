#!/usr/bin/env python3
"""
Main entry point for Snowflake Table Comparison Tool.

This script compares two Snowflake tables and generates a detailed
difference report with a table containing all differences.

Usage:
    # With primary key
    python main.py TABLE1 TABLE2 --pk ID

    # With composite primary key
    python main.py TABLE1 TABLE2 --pk "COL1,COL2"

    # Without primary key (hash comparison)
    python main.py TABLE1 TABLE2

    # With export
    python main.py TABLE1 TABLE2 --pk ID --export results --format excel

    # As a module
    from main import compare_tables
    result, diff_df = compare_tables("TABLE1", "TABLE2", primary_key="ID")
"""

import sys
import argparse
import pandas as pd
from datetime import datetime
from typing import Optional, List, Tuple, Union

from snowflake_compare import SnowflakeTableComparer, ComparisonResult
from config import SnowflakeConfig, ComparisonConfig


def compare_tables(
    table1: str,
    table2: str,
    primary_key: Optional[Union[str, List[str]]] = None,
    abs_tol: float = 0.0001,
    rel_tol: float = 0,
    export_path: Optional[str] = None,
    export_format: str = "excel",
    max_diff_rows: int = 1000,
    verbose: bool = True,
) -> Tuple[ComparisonResult, Optional[pd.DataFrame]]:
    """
    Compare two Snowflake tables and return difference report.

    Args:
        table1: First table name (fully qualified: DB.SCHEMA.TABLE)
        table2: Second table name (fully qualified: DB.SCHEMA.TABLE)
        primary_key: Primary key column(s) for comparison. Can be:
            - None: Use hash-based comparison (compare entire rows)
            - str: Single column name or comma-separated columns ("ID" or "COL1,COL2")
            - List[str]: List of column names (["COL1", "COL2"])
        abs_tol: Absolute tolerance for numeric comparisons (default: 0.0001)
        rel_tol: Relative tolerance for numeric comparisons (default: 0)
        export_path: Path to export results (without extension)
        export_format: Export format: "excel", "csv", or "json"
        max_diff_rows: Maximum number of difference rows to retrieve
        verbose: Print progress and results to console

    Returns:
        Tuple of (ComparisonResult, DataFrame with differences)

    Example:
        # With primary key
        result, diff_df = compare_tables(
            "TEAM_DB.EXTERNAL.TABLE1",
            "TEAM_DB.EXTERNAL.TABLE2",
            primary_key="ID"
        )

        # With composite key
        result, diff_df = compare_tables(
            "TEAM_DB.EXTERNAL.TABLE1",
            "TEAM_DB.EXTERNAL.TABLE2",
            primary_key=["COL1", "COL2"]
        )

        # Without primary key (hash comparison)
        result, diff_df = compare_tables(
            "TEAM_DB.EXTERNAL.TABLE1",
            "TEAM_DB.EXTERNAL.TABLE2"
        )

        # Check results
        if result.is_identical:
            print("Tables are identical!")
        else:
            print(f"Found {len(diff_df)} differences")
            print(diff_df)
    """

    if verbose:
        print("=" * 70)
        print("SNOWFLAKE TABLE COMPARISON")
        print("=" * 70)
        print(f"Table 1: {table1}")
        print(f"Table 2: {table2}")
        if primary_key:
            pk_str = primary_key if isinstance(primary_key, str) else ", ".join(primary_key)
            print(f"Primary Key: {pk_str}")
        else:
            print("Primary Key: None (hash-based comparison)")
        print(f"Numeric Tolerance: abs={abs_tol}, rel={rel_tol}")
        print("=" * 70)
        print()

    # Configure comparison
    cmp_config = ComparisonConfig(
        numeric_tolerance=abs_tol,
        max_diff_rows=max_diff_rows,
    )

    # Run comparison
    with SnowflakeTableComparer(comparison_config=cmp_config) as comparer:
        result = comparer.compare(
            table1=table1,
            table2=table2,
            join_columns=primary_key,
            abs_tol=abs_tol,
            rel_tol=rel_tol,
        )

        # Get difference details
        diff_df = result.diff_details

        if verbose:
            # Print summary
            print(result)

            # Print detailed report
            print("\n" + "=" * 70)
            print("DETAILED REPORT")
            print("=" * 70)
            print(result.get_datacompy_report())

            # Print difference summary
            print("\n" + "=" * 70)
            print("DIFFERENCE SUMMARY")
            print("=" * 70)

            if result.is_identical:
                print("RESULT: Tables are IDENTICAL")
                print(f"  - All {result.matched_rows:,} rows match")
            else:
                print("RESULT: Tables have DIFFERENCES")
                print(f"  - Matched rows:        {result.matched_rows:,}")
                print(f"  - Only in Table 1:     {result.rows_only_in_table1:,}")
                print(f"  - Only in Table 2:     {result.rows_only_in_table2:,}")
                if result.has_primary_key:
                    print(f"  - Value differences:   {result.rows_with_diff_values:,}")
                print(f"  - Match percentage:    {result.match_percentage:.2f}%")

            # Print difference table
            if diff_df is not None and len(diff_df) > 0:
                print("\n" + "=" * 70)
                print(f"DIFFERENCE TABLE (showing up to {max_diff_rows} rows)")
                print("=" * 70)

                # Summary by diff type
                diff_summary = diff_df.groupby("diff_type").size()
                print("\nDifferences by type:")
                for diff_type, count in diff_summary.items():
                    print(f"  - {diff_type}: {count:,}")

                # Show sample
                print(f"\nSample differences (first 20 rows):")
                print("-" * 70)

                # Format for display
                pd.set_option('display.max_columns', None)
                pd.set_option('display.width', None)
                pd.set_option('display.max_colwidth', 50)

                print(diff_df.head(20).to_string(index=False))

        # Export if requested
        if export_path:
            if verbose:
                print("\n" + "=" * 70)
                print("EXPORTING RESULTS")
                print("=" * 70)

            # Export summary
            comparer.export_results([result], export_path, format=export_format)

            # Also export full difference table separately
            if diff_df is not None and len(diff_df) > 0:
                diff_export_path = f"{export_path}_differences"
                if export_format == "excel":
                    diff_df.to_excel(f"{diff_export_path}.xlsx", index=False)
                elif export_format == "csv":
                    diff_df.to_csv(f"{diff_export_path}.csv", index=False)
                elif export_format == "json":
                    diff_df.to_json(f"{diff_export_path}.json", orient="records", indent=2)

                if verbose:
                    print(f"Differences exported to: {diff_export_path}.{export_format}")

    return result, diff_df


def main():
    """Main entry point for CLI."""

    parser = argparse.ArgumentParser(
        description="Compare two Snowflake tables and generate difference report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare with primary key
  python main.py TEAM_DB.EXTERNAL.TABLE1 TEAM_DB.EXTERNAL.TABLE2 --pk ID

  # Compare with composite primary key
  python main.py TABLE1 TABLE2 --pk "COL1,COL2"

  # Compare without primary key (hash comparison)
  python main.py TABLE1 TABLE2

  # Export results to Excel
  python main.py TABLE1 TABLE2 --pk ID --export results --format excel

  # With numeric tolerance
  python main.py TABLE1 TABLE2 --pk ID --tolerance 0.01
        """
    )

    parser.add_argument(
        "table1",
        help="First table name (e.g., TEAM_DB.EXTERNAL.TABLE1)"
    )

    parser.add_argument(
        "table2",
        help="Second table name (e.g., TEAM_DB.EXTERNAL.TABLE2)"
    )

    parser.add_argument(
        "--pk", "--primary-key",
        dest="primary_key",
        default=None,
        help="Primary key column(s), comma-separated for composite keys (e.g., 'ID' or 'COL1,COL2')"
    )

    parser.add_argument(
        "--tolerance", "-t",
        type=float,
        default=0.0001,
        help="Absolute tolerance for numeric comparisons (default: 0.0001)"
    )

    parser.add_argument(
        "--rel-tolerance",
        type=float,
        default=0,
        help="Relative tolerance for numeric comparisons (default: 0)"
    )

    parser.add_argument(
        "--export", "-e",
        dest="export_path",
        default=None,
        help="Export path for results (without extension)"
    )

    parser.add_argument(
        "--format", "-f",
        dest="export_format",
        choices=["excel", "csv", "json"],
        default="excel",
        help="Export format (default: excel)"
    )

    parser.add_argument(
        "--max-rows",
        type=int,
        default=1000,
        help="Maximum number of difference rows to retrieve (default: 1000)"
    )

    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress verbose output"
    )

    args = parser.parse_args()

    # Run comparison
    try:
        result, diff_df = compare_tables(
            table1=args.table1,
            table2=args.table2,
            primary_key=args.primary_key,
            abs_tol=args.tolerance,
            rel_tol=args.rel_tolerance,
            export_path=args.export_path,
            export_format=args.export_format,
            max_diff_rows=args.max_rows,
            verbose=not args.quiet,
        )

        # Exit code: 0 if identical, 1 if different, 2 if error
        if result.error_message:
            print(f"\nERROR: {result.error_message}", file=sys.stderr)
            sys.exit(2)
        elif result.is_identical:
            if not args.quiet:
                print("\nExit code: 0 (tables identical)")
            sys.exit(0)
        else:
            if not args.quiet:
                print("\nExit code: 1 (tables different)")
            sys.exit(1)

    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
