"""
Snowflake Data Comparison Module using datacompy.

This module provides functionality to compare Snowflake tables using the
datacompy library with snowflake-connector-python.

Compatible with Python 3.13+ (uses pandas-based comparison instead of Snowpark).

Usage:
    from snowflake_compare import SnowflakeTableComparer, quick_compare

    # Quick comparison
    result = quick_compare("TABLE1", "TABLE2", join_columns=["ID"])

    # Advanced usage
    with SnowflakeTableComparer() as comparer:
        result = comparer.compare("TABLE1", "TABLE2", join_columns=["ID"])
        print(result)

Requirements:
    pip install datacompy snowflake-connector-python pandas
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass, field
import hashlib
import json
import logging

# Snowflake connector imports
import snowflake.connector
from snowflake.connector import DictCursor

# datacompy imports (pandas-based Compare)
import datacompy
from datacompy import Compare

from config import SnowflakeConfig, ComparisonConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class ComparisonResult:
    """Result of a table comparison."""

    comparison_id: str
    table1_name: str
    table2_name: str
    comparison_time: datetime

    # Row counts
    table1_row_count: int
    table2_row_count: int

    # Match statistics
    matched_rows: int
    rows_only_in_table1: int
    rows_only_in_table2: int
    rows_with_diff_values: int

    # Match percentage
    match_percentage: float
    is_identical: bool

    # Primary key info
    has_primary_key: bool
    primary_key_columns: List[str]

    # Column comparison
    columns_only_in_table1: List[str] = field(default_factory=list)
    columns_only_in_table2: List[str] = field(default_factory=list)
    columns_with_differences: List[str] = field(default_factory=list)

    # Execution metadata
    execution_time_seconds: float = 0.0
    error_message: Optional[str] = None

    # Detailed diff data (optional, for export)
    diff_details: Optional[pd.DataFrame] = None

    # Full datacompy report
    datacompy_report: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "comparison_id": self.comparison_id,
            "table1_name": self.table1_name,
            "table2_name": self.table2_name,
            "comparison_time": self.comparison_time.isoformat(),
            "table1_row_count": self.table1_row_count,
            "table2_row_count": self.table2_row_count,
            "matched_rows": self.matched_rows,
            "rows_only_in_table1": self.rows_only_in_table1,
            "rows_only_in_table2": self.rows_only_in_table2,
            "rows_with_diff_values": self.rows_with_diff_values,
            "match_percentage": self.match_percentage,
            "is_identical": self.is_identical,
            "has_primary_key": self.has_primary_key,
            "primary_key_columns": self.primary_key_columns,
            "columns_only_in_table1": self.columns_only_in_table1,
            "columns_only_in_table2": self.columns_only_in_table2,
            "columns_with_differences": self.columns_with_differences,
            "execution_time_seconds": self.execution_time_seconds,
            "error_message": self.error_message,
        }

    def __str__(self) -> str:
        """Return a formatted string representation."""
        status = "IDENTICAL" if self.is_identical else "DIFFERENT"
        pk_info = ", ".join(self.primary_key_columns) if self.primary_key_columns else "None (hash)"

        lines = [
            "+" + "=" * 77 + "+",
            "|" + "COMPARISON REPORT (datacompy)".center(77) + "|",
            "+" + "=" * 77 + "+",
            f"| ID: {self.comparison_id:<72} |",
            f"| Time: {self.comparison_time.strftime('%Y-%m-%d %H:%M:%S'):<70} |",
            "+" + "=" * 77 + "+",
            f"| TABLE 1: {self.table1_name:<67} |",
            f"| TABLE 2: {self.table2_name:<67} |",
            f"| Join Columns: {pk_info:<62} |",
            "+" + "=" * 77 + "+",
            "| STATISTICS:" + " " * 65 + "|",
            f"|   - Rows in Table 1:     {self.table1_row_count:>15,}" + " " * 35 + "|",
            f"|   - Rows in Table 2:     {self.table2_row_count:>15,}" + " " * 35 + "|",
            f"|   - Matched rows:        {self.matched_rows:>15,}" + " " * 35 + "|",
            f"|   - Only in Table 1:     {self.rows_only_in_table1:>15,}" + " " * 35 + "|",
            f"|   - Only in Table 2:     {self.rows_only_in_table2:>15,}" + " " * 35 + "|",
            f"|   - Different values:    {self.rows_with_diff_values:>15,}" + " " * 35 + "|",
            "+" + "=" * 77 + "+",
            f"| RESULT: {'OK' if self.is_identical else 'KO'} {status} ({self.match_percentage:.2f}% match)" + " " * (47 - len(status)) + "|",
            f"| Execution time: {self.execution_time_seconds:.2f} seconds" + " " * 52 + "|",
            "+" + "=" * 77 + "+",
        ]
        return "\n".join(lines)

    def get_datacompy_report(self) -> str:
        """Return the full datacompy report if available."""
        return self.datacompy_report or "No datacompy report available."


class SnowflakeTableComparer:
    """
    Compare Snowflake tables using datacompy with snowflake-connector-python.

    This class loads data from Snowflake into pandas DataFrames and uses
    datacompy.Compare for comparison. Suitable for small to medium tables.

    For very large tables, consider using sampling or the SQL-based approach.
    """

    def __init__(
        self,
        snowflake_config: Optional[SnowflakeConfig] = None,
        comparison_config: Optional[ComparisonConfig] = None,
    ):
        """
        Initialize the comparer.

        Args:
            snowflake_config: Snowflake connection configuration (from .env if None)
            comparison_config: Comparison settings
        """
        self.sf_config = snowflake_config or SnowflakeConfig.from_env()
        self.cmp_config = comparison_config or ComparisonConfig()
        self._connection = None

    def _get_connection_parameters(self) -> dict:
        """Build Snowflake connection parameters from config."""
        params = {
            "account": self.sf_config.account,
            "user": self.sf_config.user,
        }

        if self.sf_config.authenticator:
            params["authenticator"] = self.sf_config.authenticator
        elif self.sf_config.password:
            params["password"] = self.sf_config.password

        if self.sf_config.warehouse:
            params["warehouse"] = self.sf_config.warehouse
        if self.sf_config.database:
            params["database"] = self.sf_config.database
        if self.sf_config.schema:
            params["schema"] = self.sf_config.schema
        if self.sf_config.role:
            params["role"] = self.sf_config.role

        return params

    def connect(self):
        """Create Snowflake connection."""
        if self._connection is None:
            logger.info(f"Connecting to Snowflake account: {self.sf_config.account}")
            connection_params = self._get_connection_parameters()
            self._connection = snowflake.connector.connect(**connection_params)
            logger.info("Snowflake connection established")
        return self._connection

    def close(self) -> None:
        """Close Snowflake connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None
            logger.info("Snowflake connection closed")

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def _generate_comparison_id(self) -> str:
        """Generate a unique comparison ID."""
        timestamp = datetime.now().isoformat()
        hash_input = f"{timestamp}".encode()
        return hashlib.md5(hash_input).hexdigest()[:16]

    def _load_table_to_pandas(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        sample_limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Load a Snowflake table into a pandas DataFrame."""
        conn = self.connect()

        if columns:
            cols_str = ", ".join(f'"{c}"' for c in columns)
        else:
            cols_str = "*"

        query = f"SELECT {cols_str} FROM {table_name}"

        if sample_limit:
            query += f" LIMIT {sample_limit}"

        logger.info(f"Loading table: {table_name}")
        df = pd.read_sql(query, conn)
        logger.info(f"  Loaded {len(df):,} rows, {len(df.columns)} columns")

        return df

    def _get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        conn = self.connect()
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        cursor.close()
        return count

    def compare(
        self,
        table1: str,
        table2: str,
        join_columns: Optional[Union[str, List[str]]] = None,
        abs_tol: Optional[float] = None,
        rel_tol: float = 0,
        df1_name: Optional[str] = None,
        df2_name: Optional[str] = None,
        sample_limit: Optional[int] = None,
        ignore_spaces: bool = False,
        ignore_case: bool = False,
    ) -> ComparisonResult:
        """
        Compare two Snowflake tables using datacompy.Compare.

        Data is loaded into pandas DataFrames for comparison.

        Args:
            table1: First table name (fully qualified: DB.SCHEMA.TABLE)
            table2: Second table name (fully qualified: DB.SCHEMA.TABLE)
            join_columns: Column(s) to join on. Can be:
                - str: Single column or comma-separated columns
                - List[str]: List of column names
                - None: Hash-based comparison
            abs_tol: Absolute tolerance for numeric comparisons (default from config)
            rel_tol: Relative tolerance for numeric comparisons
            df1_name: Optional display name for table1
            df2_name: Optional display name for table2
            sample_limit: Limit rows loaded (for large tables)
            ignore_spaces: Ignore leading/trailing spaces in strings
            ignore_case: Ignore case in string comparisons

        Returns:
            ComparisonResult object with detailed comparison results
        """
        start_time = datetime.now()
        comparison_id = self._generate_comparison_id()

        # Use config defaults if not specified
        if abs_tol is None:
            abs_tol = self.cmp_config.numeric_tolerance

        # Parse join_columns if string
        if isinstance(join_columns, str):
            join_columns = [c.strip() for c in join_columns.split(",")]

        # Handle no join columns (hash comparison)
        if not join_columns or len(join_columns) == 0:
            return self._compare_hash_fallback(
                table1, table2, comparison_id, start_time, df1_name, df2_name, sample_limit
            )

        try:
            logger.info(f"Comparing tables using datacompy.Compare:")
            logger.info(f"  Table 1: {table1}")
            logger.info(f"  Table 2: {table2}")
            logger.info(f"  Join columns: {join_columns}")

            # Load tables into pandas
            df1 = self._load_table_to_pandas(table1, sample_limit=sample_limit)
            df2 = self._load_table_to_pandas(table2, sample_limit=sample_limit)

            table1_count = len(df1)
            table2_count = len(df2)

            # Normalize column names to uppercase for comparison
            df1.columns = [c.upper() for c in df1.columns]
            df2.columns = [c.upper() for c in df2.columns]
            join_columns = [c.upper() for c in join_columns]

            # Create datacompy Compare instance
            comparison = Compare(
                df1=df1,
                df2=df2,
                join_columns=join_columns,
                abs_tol=abs_tol,
                rel_tol=rel_tol,
                df1_name=df1_name or table1.split(".")[-1],
                df2_name=df2_name or table2.split(".")[-1],
                ignore_spaces=ignore_spaces,
                ignore_case=ignore_case,
            )

            # Get statistics from datacompy
            is_identical = comparison.matches()

            # Get unique rows counts
            rows_only_t1 = len(comparison.df1_unq_rows)
            rows_only_t2 = len(comparison.df2_unq_rows)

            # Get intersect rows and count mismatches
            intersect_count = comparison.intersect_rows.shape[0]

            # Count rows with different values
            # intersect_rows contains columns like 'col_match' for each column
            mismatch_columns = [c for c in comparison.intersect_rows.columns if c.endswith('_match')]
            if mismatch_columns:
                all_match = comparison.intersect_rows[mismatch_columns].all(axis=1)
                matched_rows = all_match.sum()
                rows_with_diff = intersect_count - matched_rows
            else:
                matched_rows = intersect_count
                rows_with_diff = 0

            # Get column differences
            columns_only_t1 = list(comparison.df1_unq_columns())
            columns_only_t2 = list(comparison.df2_unq_columns())

            # Calculate match percentage
            total_rows = table1_count + table2_count
            if total_rows > 0:
                match_pct = (2 * matched_rows / total_rows) * 100
            else:
                match_pct = 100.0

            # Get the full report
            datacompy_report = comparison.report()

            # Build diff details
            diff_details = self._build_diff_details(
                comparison, join_columns, self.cmp_config.max_diff_rows
            )

            # Execution time
            exec_time = (datetime.now() - start_time).total_seconds()

            return ComparisonResult(
                comparison_id=comparison_id,
                table1_name=table1,
                table2_name=table2,
                comparison_time=start_time,
                table1_row_count=table1_count,
                table2_row_count=table2_count,
                matched_rows=int(matched_rows),
                rows_only_in_table1=rows_only_t1,
                rows_only_in_table2=rows_only_t2,
                rows_with_diff_values=int(rows_with_diff),
                match_percentage=match_pct,
                is_identical=is_identical,
                has_primary_key=True,
                primary_key_columns=join_columns,
                columns_only_in_table1=columns_only_t1,
                columns_only_in_table2=columns_only_t2,
                execution_time_seconds=exec_time,
                diff_details=diff_details,
                datacompy_report=datacompy_report,
            )

        except Exception as e:
            exec_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Comparison failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return ComparisonResult(
                comparison_id=comparison_id,
                table1_name=table1,
                table2_name=table2,
                comparison_time=start_time,
                table1_row_count=0,
                table2_row_count=0,
                matched_rows=0,
                rows_only_in_table1=0,
                rows_only_in_table2=0,
                rows_with_diff_values=0,
                match_percentage=0.0,
                is_identical=False,
                has_primary_key=join_columns is not None and len(join_columns) > 0,
                primary_key_columns=join_columns or [],
                execution_time_seconds=exec_time,
                error_message=str(e),
            )

    def _compare_hash_fallback(
        self,
        table1: str,
        table2: str,
        comparison_id: str,
        start_time: datetime,
        df1_name: Optional[str] = None,
        df2_name: Optional[str] = None,
        sample_limit: Optional[int] = None,
    ) -> ComparisonResult:
        """
        Hash-based comparison when no join columns are provided.

        Uses SQL-side SHA256 hash for efficient comparison.
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()

            logger.info("=" * 60)
            logger.info("HASH-BASED COMPARISON (No Primary Key)")
            logger.info("=" * 60)
            logger.info(f"Table 1: {table1}")
            logger.info(f"Table 2: {table2}")

            # Get row counts
            table1_count = self._get_row_count(table1)
            table2_count = self._get_row_count(table2)
            logger.info(f"Table 1 row count: {table1_count:,}")
            logger.info(f"Table 2 row count: {table2_count:,}")

            # Get column info
            cursor.execute(f"SELECT * FROM {table1} LIMIT 0")
            t1_columns = [desc[0] for desc in cursor.description]
            cursor.execute(f"SELECT * FROM {table2} LIMIT 0")
            t2_columns = [desc[0] for desc in cursor.description]

            common_columns = set(t1_columns) & set(t2_columns)
            columns_only_t1 = list(set(t1_columns) - set(t2_columns))
            columns_only_t2 = list(set(t2_columns) - set(t1_columns))

            # Find matching rows using hash
            matched_hash_query = f"""
                WITH t1_hashes AS (
                    SELECT SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash, COUNT(*) AS cnt
                    FROM {table1}
                    GROUP BY row_hash
                ),
                t2_hashes AS (
                    SELECT SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash, COUNT(*) AS cnt
                    FROM {table2}
                    GROUP BY row_hash
                )
                SELECT COALESCE(SUM(LEAST(t1.cnt, t2.cnt)), 0) AS matched_rows
                FROM t1_hashes t1
                INNER JOIN t2_hashes t2 ON t1.row_hash = t2.row_hash
            """
            cursor.execute(matched_hash_query)
            matched_rows = cursor.fetchone()[0] or 0

            # Find rows only in table1
            only_t1_query = f"""
                WITH t1_hashes AS (
                    SELECT SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash, COUNT(*) AS cnt
                    FROM {table1}
                    GROUP BY row_hash
                ),
                t2_hashes AS (
                    SELECT SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash, COUNT(*) AS cnt
                    FROM {table2}
                    GROUP BY row_hash
                )
                SELECT COALESCE(SUM(
                    CASE
                        WHEN t2.row_hash IS NULL THEN t1.cnt
                        WHEN t1.cnt > t2.cnt THEN t1.cnt - t2.cnt
                        ELSE 0
                    END
                ), 0) AS only_t1
                FROM t1_hashes t1
                LEFT JOIN t2_hashes t2 ON t1.row_hash = t2.row_hash
            """
            cursor.execute(only_t1_query)
            rows_only_t1 = cursor.fetchone()[0] or 0

            # Find rows only in table2
            only_t2_query = f"""
                WITH t1_hashes AS (
                    SELECT SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash, COUNT(*) AS cnt
                    FROM {table1}
                    GROUP BY row_hash
                ),
                t2_hashes AS (
                    SELECT SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash, COUNT(*) AS cnt
                    FROM {table2}
                    GROUP BY row_hash
                )
                SELECT COALESCE(SUM(
                    CASE
                        WHEN t1.row_hash IS NULL THEN t2.cnt
                        WHEN t2.cnt > t1.cnt THEN t2.cnt - t1.cnt
                        ELSE 0
                    END
                ), 0) AS only_t2
                FROM t2_hashes t2
                LEFT JOIN t1_hashes t1 ON t1.row_hash = t2.row_hash
            """
            cursor.execute(only_t2_query)
            rows_only_t2 = cursor.fetchone()[0] or 0

            cursor.close()

            logger.info(f"Matched rows: {matched_rows:,}")
            logger.info(f"Rows only in Table 1: {rows_only_t1:,}")
            logger.info(f"Rows only in Table 2: {rows_only_t2:,}")

            # Calculate match percentage
            total_rows = table1_count + table2_count
            if total_rows > 0:
                match_pct = (2 * matched_rows / total_rows) * 100
            else:
                match_pct = 100.0

            is_identical = (
                table1_count == table2_count
                and rows_only_t1 == 0
                and rows_only_t2 == 0
            )

            # Generate report
            datacompy_report = self._generate_hash_comparison_report(
                table1, table2,
                df1_name or table1.split(".")[-1],
                df2_name or table2.split(".")[-1],
                table1_count, table2_count,
                matched_rows, rows_only_t1, rows_only_t2,
                t1_columns, t2_columns,
                common_columns, columns_only_t1, columns_only_t2,
                match_pct, is_identical
            )

            exec_time = (datetime.now() - start_time).total_seconds()

            return ComparisonResult(
                comparison_id=comparison_id,
                table1_name=table1,
                table2_name=table2,
                comparison_time=start_time,
                table1_row_count=table1_count,
                table2_row_count=table2_count,
                matched_rows=int(matched_rows),
                rows_only_in_table1=int(rows_only_t1),
                rows_only_in_table2=int(rows_only_t2),
                rows_with_diff_values=0,
                match_percentage=match_pct,
                is_identical=is_identical,
                has_primary_key=False,
                primary_key_columns=[],
                columns_only_in_table1=columns_only_t1,
                columns_only_in_table2=columns_only_t2,
                execution_time_seconds=exec_time,
                diff_details=None,
                datacompy_report=datacompy_report,
            )

        except Exception as e:
            exec_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Hash comparison failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return ComparisonResult(
                comparison_id=comparison_id,
                table1_name=table1,
                table2_name=table2,
                comparison_time=start_time,
                table1_row_count=0,
                table2_row_count=0,
                matched_rows=0,
                rows_only_in_table1=0,
                rows_only_in_table2=0,
                rows_with_diff_values=0,
                match_percentage=0.0,
                is_identical=False,
                has_primary_key=False,
                primary_key_columns=[],
                execution_time_seconds=exec_time,
                error_message=str(e),
            )

    def _generate_hash_comparison_report(
        self,
        table1: str,
        table2: str,
        df1_name: str,
        df2_name: str,
        table1_count: int,
        table2_count: int,
        matched_rows: int,
        rows_only_t1: int,
        rows_only_t2: int,
        t1_columns: List[str],
        t2_columns: List[str],
        common_columns: set,
        columns_only_t1: List[str],
        columns_only_t2: List[str],
        match_pct: float,
        is_identical: bool
    ) -> str:
        """Generate a detailed report for hash-based comparison."""

        report_lines = [
            "",
            "=" * 70,
            "HASH-BASED COMPARISON REPORT (No Primary Key)",
            "=" * 70,
            "",
            "DataFrames/Tables",
            "-" * 70,
            f"  {df1_name}: {table1}",
            f"  {df2_name}: {table2}",
            "",
            "DataFrame Summary",
            "-" * 70,
            f"  {'Metric':<30} {df1_name:>15} {df2_name:>15}",
            f"  {'-'*30} {'-'*15} {'-'*15}",
            f"  {'Total Rows':<30} {table1_count:>15,} {table2_count:>15,}",
            f"  {'Total Columns':<30} {len(t1_columns):>15} {len(t2_columns):>15}",
            "",
            "Column Summary",
            "-" * 70,
            f"  Columns in common: {len(common_columns)}",
            f"  Columns only in {df1_name}: {len(columns_only_t1)}",
            f"  Columns only in {df2_name}: {len(columns_only_t2)}",
        ]

        if columns_only_t1:
            report_lines.append(f"    -> {columns_only_t1}")
        if columns_only_t2:
            report_lines.append(f"    -> {columns_only_t2}")

        report_lines.extend([
            "",
            "Row Comparison (Hash-Based)",
            "-" * 70,
            f"  Matching rows (identical):     {matched_rows:>15,}",
            f"  Rows only in {df1_name}:       {rows_only_t1:>15,}",
            f"  Rows only in {df2_name}:       {rows_only_t2:>15,}",
            "",
            "  Note: Hash comparison uses SHA256(OBJECT_CONSTRUCT(*)) to compare",
            "  entire rows. Without a primary key, we cannot identify which",
            "  specific columns differ - rows either match completely or don't.",
            "",
            "Match Statistics",
            "-" * 70,
            f"  Match Percentage: {match_pct:.2f}%",
            f"  Tables Identical: {is_identical}",
            "",
            "=" * 70,
        ])

        return "\n".join(report_lines)

    def _build_diff_details(
        self,
        comparison: Compare,
        join_columns: List[str],
        max_rows: int = 100
    ) -> Optional[pd.DataFrame]:
        """Build detailed diff DataFrame from datacompy.Compare."""
        diff_records = []

        try:
            # Get unique rows from table 1
            df1_unq = comparison.df1_unq_rows.head(max_rows)
            for _, row in df1_unq.iterrows():
                pk_value = "|".join(str(row.get(c, "")) for c in join_columns if c in row.index)
                diff_records.append({
                    "diff_type": "ONLY_TABLE1",
                    "primary_key": pk_value,
                    "column": None,
                    "value_table1": str(row.to_dict()),
                    "value_table2": None,
                })

            # Get unique rows from table 2
            df2_unq = comparison.df2_unq_rows.head(max_rows)
            for _, row in df2_unq.iterrows():
                pk_value = "|".join(str(row.get(c, "")) for c in join_columns if c in row.index)
                diff_records.append({
                    "diff_type": "ONLY_TABLE2",
                    "primary_key": pk_value,
                    "column": None,
                    "value_table1": None,
                    "value_table2": str(row.to_dict()),
                })

        except Exception as e:
            logger.warning(f"Could not build diff details: {e}")

        return pd.DataFrame(diff_records) if diff_records else None

    def compare_batch(
        self,
        table_pairs: List[Tuple[str, str, Optional[Union[str, List[str]]]]],
        **kwargs,
    ) -> List[ComparisonResult]:
        """
        Compare multiple table pairs.

        Args:
            table_pairs: List of (table1, table2, join_columns) tuples
            **kwargs: Additional arguments passed to compare()

        Returns:
            List of ComparisonResult objects
        """
        results = []
        total = len(table_pairs)

        for i, pair in enumerate(table_pairs, 1):
            table1, table2, join_cols = pair
            logger.info(f"[{i}/{total}] Comparing {table1} vs {table2}...")

            result = self.compare(table1, table2, join_columns=join_cols, **kwargs)
            results.append(result)

            status = "IDENTICAL" if result.is_identical else "DIFFERENT"
            if result.error_message:
                status = f"ERROR: {result.error_message[:50]}"
            logger.info(f"  -> {status} ({result.match_percentage:.2f}%)")

        return results

    def export_results(
        self,
        results: List[ComparisonResult],
        output_path: str,
        format: str = "csv",
        include_details: bool = True,
    ) -> str:
        """
        Export comparison results to file.

        Args:
            results: List of comparison results
            output_path: Output file path (without extension)
            format: Export format (csv, excel, json)
            include_details: Include detailed diff data in export

        Returns:
            Path to exported file
        """
        # Convert results to DataFrame
        records = [r.to_dict() for r in results]
        df = pd.DataFrame(records)

        if format == "csv":
            filepath = f"{output_path}.csv"
            df.to_csv(filepath, index=False)

        elif format == "excel":
            filepath = f"{output_path}.xlsx"
            with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:
                # Summary sheet
                df.to_excel(writer, sheet_name="Summary", index=False)

                # Detailed diff sheets
                if include_details:
                    for result in results:
                        if result.diff_details is not None and len(result.diff_details) > 0:
                            sheet_name = f"diff_{result.comparison_id[:25]}"
                            result.diff_details.to_excel(
                                writer, sheet_name=sheet_name, index=False
                            )

                        # Add datacompy report
                        if result.datacompy_report:
                            report_sheet = f"report_{result.comparison_id[:23]}"
                            report_lines = result.datacompy_report.split('\n')
                            report_df = pd.DataFrame({"Report": report_lines})
                            report_df.to_excel(writer, sheet_name=report_sheet, index=False)

        elif format == "json":
            filepath = f"{output_path}.json"
            output_data = []
            for r in results:
                data = r.to_dict()
                if include_details and r.diff_details is not None:
                    data["diff_details"] = r.diff_details.to_dict(orient="records")
                if r.datacompy_report:
                    data["datacompy_report"] = r.datacompy_report
                output_data.append(data)

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, default=str, ensure_ascii=False)

        else:
            raise ValueError(f"Unknown format: {format}. Use 'csv', 'excel', or 'json'.")

        logger.info(f"Results exported to: {filepath}")
        return filepath


# Alias for backwards compatibility
SnowflakeDataComparer = SnowflakeTableComparer


def quick_compare(
    table1: str,
    table2: str,
    join_columns: Optional[Union[str, List[str]]] = None,
    **kwargs,
) -> ComparisonResult:
    """
    Quick comparison function for simple use cases.

    Uses datacompy.Compare with pandas DataFrames.

    Args:
        table1: First table name (fully qualified: DB.SCHEMA.TABLE)
        table2: Second table name
        join_columns: Column(s) to join on - string, list, or None for hash
        **kwargs: Additional arguments passed to compare()

    Returns:
        ComparisonResult object

    Example:
        # With join column
        result = quick_compare("DB.SCHEMA.TABLE1", "DB.SCHEMA.TABLE2", "ID")

        # With composite key
        result = quick_compare("DB.SCHEMA.TABLE1", "DB.SCHEMA.TABLE2", ["COL1", "COL2"])

        # Hash comparison (no join columns)
        result = quick_compare("DB.SCHEMA.TABLE1", "DB.SCHEMA.TABLE2")

        # With numeric tolerance
        result = quick_compare("DB.SCHEMA.TABLE1", "DB.SCHEMA.TABLE2", "ID", abs_tol=0.01)
    """
    with SnowflakeTableComparer() as comparer:
        return comparer.compare(table1, table2, join_columns=join_columns, **kwargs)


# Convenience alias
compare_tables = quick_compare


if __name__ == "__main__":
    print("""
+===========================================================================+
|     SNOWFLAKE DATA COMPARISON TOOL                                        |
|     Using datacompy.Compare (pandas-based comparison)                     |
|     Compatible with Python 3.13+                                          |
+===========================================================================+

This module uses datacompy with snowflake-connector-python.
Data is loaded into pandas DataFrames for comparison.

Usage:
    from snowflake_compare import quick_compare, SnowflakeTableComparer

    # Quick comparison
    result = quick_compare(
        "TEAM_DB.EXTERNAL.TABLE1",
        "TEAM_DB.EXTERNAL.TABLE2",
        join_columns="ID"
    )
    print(result)
    print(result.get_datacompy_report())

    # Advanced usage with context manager
    with SnowflakeTableComparer() as comparer:
        result = comparer.compare("TABLE1", "TABLE2", join_columns=["COL1", "COL2"])
        comparer.export_results([result], "output", format="excel")

Or use the CLI:
    python run_comparison.py compare TABLE1 TABLE2 --pk ID
    python run_comparison.py batch config.yaml --export results --format excel
""")
