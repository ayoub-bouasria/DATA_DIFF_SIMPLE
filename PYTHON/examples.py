#!/usr/bin/env python3
"""
Examples of using the Snowflake Data Comparison Tool with datacompy.

This file demonstrates various usage patterns for comparing
Snowflake tables using the datacompy-based comparison tool.

Prerequisites:
    1. Configure .env file with Snowflake credentials
    2. Install requirements: pip install -r requirements.txt
"""

from snowflake_compare import SnowflakeDataComparer, quick_compare
from config import SnowflakeConfig, ComparisonConfig


# ============================================================================
# BASIC EXAMPLES
# ============================================================================

def example_1_basic_comparison():
    """
    Example 1: Basic comparison with primary key.

    Uses datacompy.Compare internally with join_columns.
    """
    print("=" * 60)
    print("Example 1: Basic Comparison with Primary Key")
    print("=" * 60)

    # Simple one-liner using quick_compare
    result = quick_compare(
        table1="TEAM_DB.EXTERNAL.HISTORICO_REMESAS",
        table2="TEAM_DB.EXTERNAL.HISTORICO_REMESAS_SAS",
        join_columns="CODNUM",  # Can be string or list
    )
    print(result)

    # Access the full datacompy report
    if result.datacompy_report:
        print("\n--- DATACOMPY FULL REPORT ---")
        print(result.datacompy_report)


def example_2_hash_comparison():
    """
    Example 2: Comparison without primary key (hash-based).

    When no join_columns are specified, the tool creates SHA-256 hashes
    of each row to identify matches and differences.

    This is useful when:
    - The table has no natural primary key
    - You want to compare entire rows
    - You need to find duplicate rows

    The comparison will report:
    - Total rows in each table
    - Distinct rows (unique by hash)
    - Matched rows (identical in both tables)
    - Rows only in table 1
    - Rows only in table 2
    """
    print("=" * 60)
    print("Example 2: Hash Comparison (No Primary Key)")
    print("=" * 60)

    result = quick_compare(
        table1="TEAM_DB.EXTERNAL.EXCLUSIONES_REMESAS",
        table2="TEAM_DB.EXTERNAL.EXCLUSIONES_REMESAS_SAS",
        join_columns=None,  # No PK = hash comparison
    )

    # Print summary
    print(result)

    # Print detailed hash comparison report
    print("\n--- DETAILED HASH COMPARISON REPORT ---")
    print(result.get_datacompy_report())

    # Access specific statistics
    print("\n--- KEY STATISTICS ---")
    print(f"Table 1 rows: {result.table1_row_count:,}")
    print(f"Table 2 rows: {result.table2_row_count:,}")
    print(f"Identical rows (matched): {result.matched_rows:,}")
    print(f"Rows only in Table 1: {result.rows_only_in_table1:,}")
    print(f"Rows only in Table 2: {result.rows_only_in_table2:,}")
    print(f"Match percentage: {result.match_percentage:.2f}%")
    print(f"Tables identical: {result.is_identical}")

    # Show sample of different rows if available
    if result.diff_details is not None and len(result.diff_details) > 0:
        print("\n--- SAMPLE OF DIFFERENT ROWS ---")
        print(result.diff_details.head(10))


def example_3_composite_key():
    """
    Example 3: Comparison with composite primary key.

    Multiple columns can be specified as join columns.
    """
    print("=" * 60)
    print("Example 3: Composite Primary Key")
    print("=" * 60)

    # Method 1: List of columns
    result = quick_compare(
        table1="TEAM_DB.EXTERNAL.OUTPUT_DOMICILIACIONES_NO_NEGATIVAS",
        table2="TEAM_DB.EXTERNAL.OUTPUT_DOMICILIACIONES_NO_NEGATIVAS_SAS",
        join_columns=["CONTRATO", "CODNUM"],  # List format
    )
    print(result)

    # Method 2: Comma-separated string (equivalent)
    # result = quick_compare("TABLE1", "TABLE2", join_columns="CONTRATO,CODNUM")


def example_4_specific_columns():
    """
    Example 4: Compare only specific columns.

    Useful when you want to ignore certain columns (like timestamps).
    """
    print("=" * 60)
    print("Example 4: Compare Specific Columns Only")
    print("=" * 60)

    with SnowflakeDataComparer() as comparer:
        result = comparer.compare(
            table1="TEAM_DB.EXTERNAL.EVOLUCION_RP",
            table2="TEAM_DB.EXTERNAL.EVOLUCION_RP_SAS",
            join_columns="FICHERO_SIBS",
            columns_to_compare=["RP1", "RP5", "RP10", "RP15", "RP20"],
        )
        print(result)


def example_5_numeric_tolerance():
    """
    Example 5: Comparison with numeric tolerance.

    Useful for floating point comparisons where small differences
    should be ignored (e.g., due to precision differences between systems).
    """
    print("=" * 60)
    print("Example 5: Numeric Tolerance for Float Comparison")
    print("=" * 60)

    result = quick_compare(
        table1="TEAM_DB.EXTERNAL.REMESA_BBVA",
        table2="TEAM_DB.EXTERNAL.REMESA_BBVA_MIGRATED",
        join_columns=None,  # Hash comparison
        numeric_tolerance=0.01,  # Allow differences up to 0.01
    )
    print(result)


def example_6_case_insensitive():
    """
    Example 6: Case-insensitive string comparison.

    Ignores case differences in string columns and column names.
    """
    print("=" * 60)
    print("Example 6: Case-Insensitive Comparison")
    print("=" * 60)

    result = quick_compare(
        table1="TEAM_DB.EXTERNAL.FICHEROS_E002",
        table2="TEAM_DB.EXTERNAL.FICHEROS_E002_UPPER",
        join_columns="FICHERO",
        case_sensitive=False,  # Ignore case in comparisons
    )
    print(result)


# ============================================================================
# ADVANCED EXAMPLES
# ============================================================================

def example_hash_comparison_detailed():
    """
    Detailed example of hash-based comparison WITHOUT primary key.

    This example demonstrates how to compare tables when there is no
    unique identifier (primary key). The comparison uses SHA256 hashes
    of entire rows to identify:
    - Identical rows (present in both tables)
    - Rows only in table 1
    - Rows only in table 2

    Key points:
    - Handles duplicate rows correctly
    - Cannot identify column-level differences (rows either match or don't)
    - Useful for validation of data migrations without keys
    """
    print("=" * 70)
    print("HASH-BASED COMPARISON - DETAILED EXAMPLE")
    print("No Primary Key - Comparing Entire Rows")
    print("=" * 70)

    from snowflake_compare import SnowflakeTableComparer

    with SnowflakeTableComparer() as comparer:
        # Compare tables without specifying join_columns
        result = comparer.compare(
            table1="TEAM_DB.EXTERNAL.EXCLUSIONES_REMESAS",
            table2="TEAM_DB.EXTERNAL.EXCLUSIONES_REMESAS_SAS",
            join_columns=None,  # None = hash-based comparison
        )

        # Print formatted result
        print(result)

        # Print the detailed comparison report
        print("\n" + "=" * 70)
        print("DETAILED COMPARISON REPORT")
        print("=" * 70)
        print(result.get_datacompy_report())

        # Analyze the results
        print("\n" + "=" * 70)
        print("ANALYSIS")
        print("=" * 70)

        if result.is_identical:
            print("SUCCESS: Tables are IDENTICAL!")
            print(f"  - All {result.matched_rows:,} rows match exactly")
        else:
            print("DIFFERENCES FOUND:")
            print(f"  - Identical rows: {result.matched_rows:,}")
            print(f"  - Rows only in Table 1: {result.rows_only_in_table1:,}")
            print(f"  - Rows only in Table 2: {result.rows_only_in_table2:,}")
            print(f"  - Match rate: {result.match_percentage:.2f}%")

            # Show sample differences
            if result.diff_details is not None and len(result.diff_details) > 0:
                print("\n--- SAMPLE OF DIFFERENT ROWS ---")

                # Rows only in table 1
                only_t1 = result.diff_details[result.diff_details["diff_type"] == "ONLY_TABLE1"]
                if len(only_t1) > 0:
                    print(f"\nRows only in Table 1 (sample of {len(only_t1)}):")
                    for _, row in only_t1.head(3).iterrows():
                        print(f"  Hash: {row['row_hash']}")
                        print(f"  Data: {row['row_data'][:100]}...")

                # Rows only in table 2
                only_t2 = result.diff_details[result.diff_details["diff_type"] == "ONLY_TABLE2"]
                if len(only_t2) > 0:
                    print(f"\nRows only in Table 2 (sample of {len(only_t2)}):")
                    for _, row in only_t2.head(3).iterrows():
                        print(f"  Hash: {row['row_hash']}")
                        print(f"  Data: {row['row_data'][:100]}...")

        # Export results
        print("\n--- EXPORTING RESULTS ---")
        comparer.export_results([result], "hash_comparison_result", format="excel")
        print("Results exported to: hash_comparison_result.xlsx")

    return result


def example_7_filtered_comparison():
    """
    Example 7: Compare filtered subsets of tables.

    Use WHERE clauses to compare only specific data ranges.
    """
    print("=" * 60)
    print("Example 7: Filtered Comparison with WHERE Clause")
    print("=" * 60)

    with SnowflakeDataComparer() as comparer:
        result = comparer.compare(
            table1="TEAM_DB.EXTERNAL.HISTORICO_REMESAS",
            table2="TEAM_DB.EXTERNAL.HISTORICO_REMESAS_SAS",
            join_columns="CODNUM",
            where_clause1="FECHA >= '2024-01-01'",
            where_clause2="FECHA >= '2024-01-01'",
        )
        print(result)


def example_8_sample_comparison():
    """
    Example 8: Sample comparison for large tables.

    Limit the number of rows for quick validation of large tables.
    """
    print("=" * 60)
    print("Example 8: Sample Comparison (Limited Rows)")
    print("=" * 60)

    with SnowflakeDataComparer() as comparer:
        result = comparer.compare(
            table1="TEAM_DB.EXTERNAL.LARGE_TABLE",
            table2="TEAM_DB.EXTERNAL.LARGE_TABLE_COPY",
            join_columns="ID",
            sample_limit=10000,  # Only compare first 10,000 rows
        )
        print(result)


def example_9_batch_comparison():
    """
    Example 9: Compare multiple table pairs in batch.

    Efficient for validating multiple tables at once.
    """
    print("=" * 60)
    print("Example 9: Batch Comparison of Multiple Tables")
    print("=" * 60)

    # Define table pairs: (source, target, primary_key)
    table_pairs = [
        ("TEAM_DB.EXTERNAL.HISTORICO_REMESAS", "TEAM_DB.EXTERNAL.HISTORICO_REMESAS_SAS", "CODNUM"),
        ("TEAM_DB.EXTERNAL.OUTPUT_ENVIO", "TEAM_DB.EXTERNAL.OUTPUT_ENVIO_SAS", "CODNUM"),
        ("TEAM_DB.EXTERNAL.OUTPUT_ENVIO_BBVA", "TEAM_DB.EXTERNAL.OUTPUT_ENVIO_BBVA_SAS", "CODNUM"),
        ("TEAM_DB.EXTERNAL.EXCLUSIONES_REMESAS", "TEAM_DB.EXTERNAL.EXCLUSIONES_REMESAS_SAS", None),
    ]

    with SnowflakeDataComparer() as comparer:
        results = comparer.compare_batch(table_pairs, numeric_tolerance=0.01)

        # Summary
        print("\n" + "=" * 60)
        print("BATCH SUMMARY")
        print("=" * 60)
        for r in results:
            status = "IDENTICAL" if r.is_identical else "DIFFERENT"
            if r.error_message:
                status = f"ERROR"
            print(f"  {r.table1_name.split('.')[-1]}: {status} ({r.match_percentage:.2f}%)")


def example_10_export_results():
    """
    Example 10: Export comparison results to files.

    Supports CSV, Excel (with multiple sheets), and JSON formats.
    """
    print("=" * 60)
    print("Example 10: Export Results to Files")
    print("=" * 60)

    with SnowflakeDataComparer() as comparer:
        result = comparer.compare(
            table1="TEAM_DB.EXTERNAL.HISTORICO_REMESAS",
            table2="TEAM_DB.EXTERNAL.HISTORICO_REMESAS_SAS",
            join_columns="CODNUM",
        )

        # Export to different formats
        comparer.export_results([result], "output/comparison_csv", format="csv")
        comparer.export_results([result], "output/comparison_excel", format="excel")
        comparer.export_results([result], "output/comparison_json", format="json")

        print("Files exported to output/ directory")


def example_11_custom_config():
    """
    Example 11: Using custom configuration.

    Override default configuration for specific use cases.
    """
    print("=" * 60)
    print("Example 11: Custom Configuration")
    print("=" * 60)

    # Custom Snowflake config (instead of .env)
    sf_config = SnowflakeConfig(
        account="your_account.eu-west-1",
        user="your_username",
        authenticator="externalbrowser",  # Use SSO
        warehouse="YOUR_WAREHOUSE",
        database="TEAM_DB",
        schema="EXTERNAL",
    )

    # Custom comparison config
    cmp_config = ComparisonConfig(
        numeric_tolerance=0.001,
        case_sensitive=False,
        max_diff_rows=500,
        export_format="excel",
    )

    with SnowflakeDataComparer(sf_config, cmp_config) as comparer:
        result = comparer.compare(
            table1="TABLE1",
            table2="TABLE2",
            join_columns="ID",
        )
        print(result)


def example_12_access_datacompy_report():
    """
    Example 12: Access the full datacompy report.

    The datacompy library provides detailed comparison statistics.
    """
    print("=" * 60)
    print("Example 12: Full datacompy Report")
    print("=" * 60)

    result = quick_compare(
        table1="TEAM_DB.EXTERNAL.TEST_TABLE_A",
        table2="TEAM_DB.EXTERNAL.TEST_TABLE_B",
        join_columns="ID",
    )

    # The datacompy report contains:
    # - DataFrame Summary
    # - Column Summary
    # - Row Summary
    # - Column Comparison
    # - Sample Mismatches
    print(result.get_datacompy_report())


# ============================================================================
# PROYECTO 0 - FULL COMPARISON SUITE
# ============================================================================

def run_proyecto0_comparison():
    """
    Full comparison for PROYECTO 0 tables.

    This is a real-world example for bank remittance migration validation.
    Compares all tables defined in the PROYECTO 0 configuration.
    """
    print("=" * 60)
    print("PROYECTO 0 - Full Migration Validation")
    print("=" * 60)

    # Define all PROYECTO 0 tables
    proyecto0_tables = [
        # (table_name, primary_key, tolerance)
        ("HISTORICO_REMESAS", "CODNUM", 0.01),
        ("OUTPUT_ENVIO", "CODNUM", 0.01),
        ("OUTPUT_ENVIO_BBVA", "CODNUM", 0.01),
        ("OUTPUT_IBAN_NO_OK", "CODNUM", 0),
        ("OUTPUT_NOENVIO", "CODNUM", 0),
        ("OUTPUT_KO_EXCLUSIONES", "CODNUM", 0),
        ("OUTPUT_DOMICILIACIONES_NO_NEGATIVAS", "CONTRATO,CODNUM", 0.01),
        ("FICHEROS_E002", "FICHERO", 0),
        ("FICHEROS_REMESAS", "FICHERO", 0),
        ("FICHEROS_DEVOLUCIONES", "FICHERO", 0),
        ("EVOLUCION_RP", "FICHERO_SIBS", 0),
        ("EXCLUSIONES_REMESAS", None, 0),  # No PK
        ("REMESA_BBVA", None, 0.01),  # No PK
    ]

    results = []

    with SnowflakeDataComparer() as comparer:
        for table_name, pk, tolerance in proyecto0_tables:
            source_table = f"TEAM_DB.EXTERNAL.{table_name}"
            target_table = f"TEAM_DB.EXTERNAL.{table_name}_SAS"

            print(f"\nComparing {table_name}...")

            result = comparer.compare(
                table1=source_table,
                table2=target_table,
                join_columns=pk,
                numeric_tolerance=tolerance,
            )
            results.append(result)

            status = "IDENTICAL" if result.is_identical else "DIFFERENT"
            if result.error_message:
                status = f"ERROR: {result.error_message[:30]}"
            print(f"  -> {status} ({result.match_percentage:.2f}%)")

        # Export all results to Excel
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        comparer.export_results(
            results,
            f"proyecto0_validation_{timestamp}",
            format="excel",
        )

    # Final summary
    print("\n" + "=" * 60)
    print("PROYECTO 0 - VALIDATION SUMMARY")
    print("=" * 60)

    identical = sum(1 for r in results if r.is_identical)
    different = sum(1 for r in results if not r.is_identical and not r.error_message)
    errors = sum(1 for r in results if r.error_message)

    print(f"  Total tables:  {len(results)}")
    print(f"  Identical:     {identical}")
    print(f"  Different:     {different}")
    print(f"  Errors:        {errors}")

    if identical == len(results):
        print("\n  MIGRATION VALIDATED SUCCESSFULLY!")
    else:
        print("\n  DIFFERENCES FOUND - Review the exported report.")

    return results


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("""
╔═══════════════════════════════════════════════════════════════╗
║     SNOWFLAKE DATA COMPARISON TOOL - EXAMPLES                 ║
║     Using datacompy library with Snowpark                      ║
╚═══════════════════════════════════════════════════════════════╝

Available examples:

  BASIC EXAMPLES:
  1. Basic comparison with primary key
  2. Hash comparison (no primary key) - simple
  3. Composite primary key
  4. Compare specific columns only
  5. Numeric tolerance
  6. Case-insensitive comparison

  ADVANCED EXAMPLES:
  H. Hash comparison DETAILED (no primary key) - comprehensive analysis
  7. Filtered comparison (WHERE clause)
  8. Sample comparison (limited rows)
  9. Batch comparison
  10. Export results
  11. Custom configuration
  12. Access datacompy report

  FULL SUITE:
  P. Run PROYECTO 0 full comparison

Uncomment the example you want to run below:
""")

    # Uncomment the example you want to run:

    # Basic examples
    # example_1_basic_comparison()
    # example_2_hash_comparison()
    # example_3_composite_key()
    # example_4_specific_columns()
    # example_5_numeric_tolerance()
    # example_6_case_insensitive()

    # Advanced examples
    # example_hash_comparison_detailed()  # <-- DETAILED HASH COMPARISON (NO PK)
    # example_7_filtered_comparison()
    # example_8_sample_comparison()
    # example_9_batch_comparison()
    # example_10_export_results()
    # example_11_custom_config()
    # example_12_access_datacompy_report()

    # Full PROYECTO 0 comparison
    # run_proyecto0_comparison()

    print("\nTo run an example, edit this file and uncomment one of the functions above.")
    print("\nFor tables WITHOUT primary key, use:")
    print("  - example_2_hash_comparison() for quick comparison")
    print("  - example_hash_comparison_detailed() for comprehensive analysis")
    print("\nOr use the CLI: python run_comparison.py --help")
