"""
Snowflake Data Comparison Module using datacompy.

This module provides functionality to compare Snowflake tables using the
datacompy library with native Snowflake/Snowpark integration.

The comparison happens SERVER-SIDE in Snowflake, which is much more efficient
than loading data into pandas for large tables.

Usage:
    from snowflake_compare import SnowflakeTableComparer, quick_compare

    # Quick comparison
    result = quick_compare("TABLE1", "TABLE2", join_columns=["ID"])

    # Advanced usage
    with SnowflakeTableComparer() as comparer:
        result = comparer.compare("TABLE1", "TABLE2", join_columns=["ID"])
        print(result)

Requirements:
    pip install datacompy[snowflake]
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass, field
import hashlib
import json
import logging

# Snowpark imports
from snowflake.snowpark import Session
from snowflake.snowpark import DataFrame as SnowparkDataFrame

# datacompy imports
import datacompy
from datacompy import SnowflakeCompare

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
            "╔" + "═" * 77 + "╗",
            "║" + "COMPARISON REPORT (datacompy)".center(77) + "║",
            "╠" + "═" * 77 + "╣",
            f"║ ID: {self.comparison_id:<72} ║",
            f"║ Time: {self.comparison_time.strftime('%Y-%m-%d %H:%M:%S'):<70} ║",
            "╠" + "═" * 77 + "╣",
            f"║ TABLE 1: {self.table1_name:<67} ║",
            f"║ TABLE 2: {self.table2_name:<67} ║",
            f"║ Join Columns: {pk_info:<62} ║",
            "╠" + "═" * 77 + "╣",
            "║ STATISTICS:" + " " * 65 + "║",
            f"║   - Rows in Table 1:     {self.table1_row_count:>15,}" + " " * 35 + "║",
            f"║   - Rows in Table 2:     {self.table2_row_count:>15,}" + " " * 35 + "║",
            f"║   - Matched rows:        {self.matched_rows:>15,}" + " " * 35 + "║",
            f"║   - Only in Table 1:     {self.rows_only_in_table1:>15,}" + " " * 35 + "║",
            f"║   - Only in Table 2:     {self.rows_only_in_table2:>15,}" + " " * 35 + "║",
            f"║   - Different values:    {self.rows_with_diff_values:>15,}" + " " * 35 + "║",
            "╠" + "═" * 77 + "╣",
            f"║ RESULT: {'✓' if self.is_identical else '✗'} {status} ({self.match_percentage:.2f}% match)" + " " * (47 - len(status)) + "║",
            f"║ Execution time: {self.execution_time_seconds:.2f} seconds" + " " * 52 + "║",
            "╚" + "═" * 77 + "╝",
        ]
        return "\n".join(lines)

    def get_datacompy_report(self) -> str:
        """Return the full datacompy report if available."""
        return self.datacompy_report or "No datacompy report available."


class SnowflakeTableComparer:
    """
    Compare Snowflake tables using datacompy's native SnowflakeCompare.

    This class uses Snowpark to perform comparisons SERVER-SIDE in Snowflake,
    which is much more efficient for large tables than loading data into pandas.
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
        self._session: Optional[Session] = None

    def _get_connection_parameters(self) -> dict:
        """Build Snowpark connection parameters from config."""
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

    def connect(self) -> Session:
        """Create Snowpark session."""
        if self._session is None:
            logger.info(f"Creating Snowpark session for account: {self.sf_config.account}")
            connection_params = self._get_connection_parameters()
            self._session = Session.builder.configs(connection_params).create()
            logger.info("Snowpark session created successfully")
        return self._session

    def close(self) -> None:
        """Close Snowpark session."""
        if self._session is not None:
            self._session.close()
            self._session = None
            logger.info("Snowpark session closed")

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

    def compare(
        self,
        table1: str,
        table2: str,
        join_columns: Optional[Union[str, List[str]]] = None,
        abs_tol: Optional[float] = None,
        rel_tol: float = 0,
        df1_name: Optional[str] = None,
        df2_name: Optional[str] = None,
    ) -> ComparisonResult:
        """
        Compare two Snowflake tables using datacompy's SnowflakeCompare.

        The comparison happens SERVER-SIDE in Snowflake using Snowpark,
        which is efficient for large tables.

        Args:
            table1: First table name (fully qualified: DB.SCHEMA.TABLE)
            table2: Second table name (fully qualified: DB.SCHEMA.TABLE)
            join_columns: Column(s) to join on. Can be:
                - str: Single column or comma-separated columns
                - List[str]: List of column names
            abs_tol: Absolute tolerance for numeric comparisons (default from config)
            rel_tol: Relative tolerance for numeric comparisons
            df1_name: Optional display name for table1
            df2_name: Optional display name for table2

        Returns:
            ComparisonResult object with detailed comparison results

        Example:
            result = comparer.compare(
                "TEAM_DB.EXTERNAL.TABLE1",
                "TEAM_DB.EXTERNAL.TABLE2",
                join_columns="ID",  # or ["ID"] or "COL1,COL2"
                abs_tol=0.01
            )
        """
        start_time = datetime.now()
        comparison_id = self._generate_comparison_id()

        # Use config defaults if not specified
        if abs_tol is None:
            abs_tol = self.cmp_config.numeric_tolerance

        # Parse join_columns if string
        if isinstance(join_columns, str):
            join_columns = [c.strip() for c in join_columns.split(",")]

        # Ensure we have join columns
        if not join_columns or len(join_columns) == 0:
            return self._compare_hash_fallback(
                table1, table2, comparison_id, start_time, df1_name, df2_name
            )

        try:
            session = self.connect()

            logger.info(f"Comparing tables using datacompy.SnowflakeCompare:")
            logger.info(f"  Table 1: {table1}")
            logger.info(f"  Table 2: {table2}")
            logger.info(f"  Join columns: {join_columns}")

            # Create SnowflakeCompare instance
            # datacompy accepts table names directly as strings
            comparison = SnowflakeCompare(
                session,
                table1,
                table2,
                join_columns=join_columns,
                abs_tol=abs_tol,
                rel_tol=rel_tol,
                df1_name=df1_name or table1.split(".")[-1],
                df2_name=df2_name or table2.split(".")[-1],
            )

            # Get row counts
            table1_count = session.table(table1).count()
            table2_count = session.table(table2).count()

            # Get statistics from datacompy
            is_identical = comparison.matches()

            # Get unique rows counts
            rows_only_t1 = comparison.df1_unq_rows.count()
            rows_only_t2 = comparison.df2_unq_rows.count()

            # Get intersect rows and count mismatches
            intersect_rows_df = comparison.intersect_rows
            intersect_count = intersect_rows_df.count()

            # Count rows with different values in intersect
            # (intersect rows that are not fully matching)
            matched_rows = comparison.count_matching_rows() if hasattr(comparison, 'count_matching_rows') else intersect_count
            rows_with_diff = intersect_count - matched_rows if intersect_count > matched_rows else 0

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

            # Build diff details (limited sample)
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
                matched_rows=matched_rows,
                rows_only_in_table1=rows_only_t1,
                rows_only_in_table2=rows_only_t2,
                rows_with_diff_values=rows_with_diff,
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
    ) -> ComparisonResult:
        """
        Comprehensive hash-based comparison when no join columns are provided.

        This method:
        1. Creates SHA256 hash of each row (using OBJECT_CONSTRUCT)
        2. Handles duplicate rows correctly by counting occurrences
        3. Identifies rows only in table1, only in table2, and matching rows
        4. Provides sample of different rows for analysis
        5. Generates a detailed report similar to datacompy

        Note: Without a primary key, we cannot identify "value differences" -
        rows either match completely or don't match at all.
        """
        try:
            session = self.connect()

            logger.info("=" * 60)
            logger.info("HASH-BASED COMPARISON (No Primary Key)")
            logger.info("=" * 60)
            logger.info(f"Table 1: {table1}")
            logger.info(f"Table 2: {table2}")

            # Get row counts
            table1_count = session.table(table1).count()
            table2_count = session.table(table2).count()
            logger.info(f"Table 1 row count: {table1_count:,}")
            logger.info(f"Table 2 row count: {table2_count:,}")

            # Get column info
            t1_columns = [col.name for col in session.table(table1).schema.fields]
            t2_columns = [col.name for col in session.table(table2).schema.fields]
            common_columns = set(t1_columns) & set(t2_columns)
            columns_only_t1 = list(set(t1_columns) - set(t2_columns))
            columns_only_t2 = list(set(t2_columns) - set(t1_columns))

            logger.info(f"Common columns: {len(common_columns)}")
            if columns_only_t1:
                logger.info(f"Columns only in Table 1: {columns_only_t1}")
            if columns_only_t2:
                logger.info(f"Columns only in Table 2: {columns_only_t2}")

            # Create hash tables with counts for duplicate handling
            # This handles tables with duplicate rows correctly
            hash_count_t1_query = f"""
                SELECT
                    SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash,
                    COUNT(*) AS row_count
                FROM {table1}
                GROUP BY row_hash
            """

            hash_count_t2_query = f"""
                SELECT
                    SHA2(OBJECT_CONSTRUCT(*), 256) AS row_hash,
                    COUNT(*) AS row_count
                FROM {table2}
                GROUP BY row_hash
            """

            # Count unique hash values (distinct rows)
            distinct_t1 = session.sql(f"SELECT COUNT(DISTINCT SHA2(OBJECT_CONSTRUCT(*), 256)) FROM {table1}").collect()[0][0]
            distinct_t2 = session.sql(f"SELECT COUNT(DISTINCT SHA2(OBJECT_CONSTRUCT(*), 256)) FROM {table2}").collect()[0][0]

            logger.info(f"Distinct rows in Table 1: {distinct_t1:,}")
            logger.info(f"Distinct rows in Table 2: {distinct_t2:,}")

            # Find matching rows (exist in both tables)
            # Using a more precise count that handles duplicates
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
                SELECT SUM(LEAST(t1.cnt, t2.cnt)) AS matched_rows
                FROM t1_hashes t1
                INNER JOIN t2_hashes t2 ON t1.row_hash = t2.row_hash
            """
            matched_result = session.sql(matched_hash_query).collect()[0][0]
            matched_rows = int(matched_result) if matched_result else 0

            logger.info(f"Matched rows (identical in both): {matched_rows:,}")

            # Find rows only in table1 (considering duplicates)
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
                SELECT SUM(
                    CASE
                        WHEN t2.row_hash IS NULL THEN t1.cnt
                        WHEN t1.cnt > t2.cnt THEN t1.cnt - t2.cnt
                        ELSE 0
                    END
                ) AS only_t1
                FROM t1_hashes t1
                LEFT JOIN t2_hashes t2 ON t1.row_hash = t2.row_hash
            """
            only_t1_result = session.sql(only_t1_query).collect()[0][0]
            rows_only_t1 = int(only_t1_result) if only_t1_result else 0

            # Find rows only in table2 (considering duplicates)
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
                SELECT SUM(
                    CASE
                        WHEN t1.row_hash IS NULL THEN t2.cnt
                        WHEN t2.cnt > t1.cnt THEN t2.cnt - t1.cnt
                        ELSE 0
                    END
                ) AS only_t2
                FROM t2_hashes t2
                LEFT JOIN t1_hashes t1 ON t1.row_hash = t2.row_hash
            """
            only_t2_result = session.sql(only_t2_query).collect()[0][0]
            rows_only_t2 = int(only_t2_result) if only_t2_result else 0

            logger.info(f"Rows only in Table 1: {rows_only_t1:,}")
            logger.info(f"Rows only in Table 2: {rows_only_t2:,}")

            # Verify counts add up
            verify_t1 = matched_rows + rows_only_t1
            verify_t2 = matched_rows + rows_only_t2
            logger.info(f"Verification - Table 1: {matched_rows} + {rows_only_t1} = {verify_t1} (expected {table1_count})")
            logger.info(f"Verification - Table 2: {matched_rows} + {rows_only_t2} = {verify_t2} (expected {table2_count})")

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

            logger.info(f"Match percentage: {match_pct:.2f}%")
            logger.info(f"Tables identical: {is_identical}")

            # Get sample of different rows for analysis
            diff_details = self._get_hash_diff_samples(
                session, table1, table2, self.cmp_config.max_diff_rows
            )

            # Generate detailed report
            datacompy_report = self._generate_hash_comparison_report(
                table1, table2,
                df1_name or table1.split(".")[-1],
                df2_name or table2.split(".")[-1],
                table1_count, table2_count,
                distinct_t1, distinct_t2,
                matched_rows, rows_only_t1, rows_only_t2,
                t1_columns, t2_columns,
                common_columns, columns_only_t1, columns_only_t2,
                match_pct, is_identical
            )

            exec_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"Execution time: {exec_time:.2f} seconds")

            return ComparisonResult(
                comparison_id=comparison_id,
                table1_name=table1,
                table2_name=table2,
                comparison_time=start_time,
                table1_row_count=table1_count,
                table2_row_count=table2_count,
                matched_rows=matched_rows,
                rows_only_in_table1=rows_only_t1,
                rows_only_in_table2=rows_only_t2,
                rows_with_diff_values=0,  # N/A for hash comparison
                match_percentage=match_pct,
                is_identical=is_identical,
                has_primary_key=False,
                primary_key_columns=[],
                columns_only_in_table1=columns_only_t1,
                columns_only_in_table2=columns_only_t2,
                execution_time_seconds=exec_time,
                diff_details=diff_details,
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

    def _get_hash_diff_samples(
        self,
        session: Session,
        table1: str,
        table2: str,
        max_rows: int = 100
    ) -> Optional[pd.DataFrame]:
        """
        Get sample of rows that are different between tables (hash comparison).

        Returns a DataFrame with sample rows that exist only in one table.
        """
        diff_records = []

        try:
            # Sample rows only in table1
            sample_t1_query = f"""
                WITH t1_hashes AS (
                    SELECT *, SHA2(OBJECT_CONSTRUCT(*), 256) AS _row_hash
                    FROM {table1}
                ),
                t2_hashes AS (
                    SELECT DISTINCT SHA2(OBJECT_CONSTRUCT(*), 256) AS _row_hash
                    FROM {table2}
                )
                SELECT t1.*
                FROM t1_hashes t1
                LEFT JOIN t2_hashes t2 ON t1._row_hash = t2._row_hash
                WHERE t2._row_hash IS NULL
                LIMIT {max_rows // 2}
            """
            sample_t1 = session.sql(sample_t1_query).to_pandas()

            for _, row in sample_t1.iterrows():
                row_dict = row.drop("_ROW_HASH", errors="ignore").to_dict()
                diff_records.append({
                    "diff_type": "ONLY_TABLE1",
                    "row_hash": row.get("_ROW_HASH", "")[:16] if "_ROW_HASH" in row else "",
                    "row_data": str(row_dict),
                })

            # Sample rows only in table2
            sample_t2_query = f"""
                WITH t1_hashes AS (
                    SELECT DISTINCT SHA2(OBJECT_CONSTRUCT(*), 256) AS _row_hash
                    FROM {table1}
                ),
                t2_hashes AS (
                    SELECT *, SHA2(OBJECT_CONSTRUCT(*), 256) AS _row_hash
                    FROM {table2}
                )
                SELECT t2.*
                FROM t2_hashes t2
                LEFT JOIN t1_hashes t1 ON t1._row_hash = t2._row_hash
                WHERE t1._row_hash IS NULL
                LIMIT {max_rows // 2}
            """
            sample_t2 = session.sql(sample_t2_query).to_pandas()

            for _, row in sample_t2.iterrows():
                row_dict = row.drop("_ROW_HASH", errors="ignore").to_dict()
                diff_records.append({
                    "diff_type": "ONLY_TABLE2",
                    "row_hash": row.get("_ROW_HASH", "")[:16] if "_ROW_HASH" in row else "",
                    "row_data": str(row_dict),
                })

        except Exception as e:
            logger.warning(f"Could not get diff samples: {e}")

        return pd.DataFrame(diff_records) if diff_records else None

    def _generate_hash_comparison_report(
        self,
        table1: str,
        table2: str,
        df1_name: str,
        df2_name: str,
        table1_count: int,
        table2_count: int,
        distinct_t1: int,
        distinct_t2: int,
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
            f"  {'Distinct Rows (by hash)':<30} {distinct_t1:>15,} {distinct_t2:>15,}",
            f"  {'Duplicate Rows':<30} {table1_count - distinct_t1:>15,} {table2_count - distinct_t2:>15,}",
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
            "Verification",
            "-" * 70,
            f"  {df1_name}: {matched_rows:,} matched + {rows_only_t1:,} only = {matched_rows + rows_only_t1:,} (actual: {table1_count:,})",
            f"  {df2_name}: {matched_rows:,} matched + {rows_only_t2:,} only = {matched_rows + rows_only_t2:,} (actual: {table2_count:,})",
            "",
            "=" * 70,
        ])

        return "\n".join(report_lines)

    def _build_diff_details(
        self,
        comparison: SnowflakeCompare,
        join_columns: List[str],
        max_rows: int = 100
    ) -> Optional[pd.DataFrame]:
        """Build detailed diff DataFrame from SnowflakeCompare."""
        diff_records = []

        try:
            # Get sample of unique rows from table 1
            df1_unq = comparison.df1_unq_rows.limit(max_rows).to_pandas()
            for _, row in df1_unq.iterrows():
                pk_value = "|".join(str(row.get(c, "")) for c in join_columns if c in row.index)
                diff_records.append({
                    "diff_type": "ONLY_TABLE1",
                    "primary_key": pk_value,
                    "column": None,
                    "value_table1": str(row.to_dict()),
                    "value_table2": None,
                })

            # Get sample of unique rows from table 2
            df2_unq = comparison.df2_unq_rows.limit(max_rows).to_pandas()
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

        Example:
            pairs = [
                ("DB.SCHEMA.TABLE1", "DB.SCHEMA.TABLE1_COPY", "ID"),
                ("DB.SCHEMA.TABLE2", "DB.SCHEMA.TABLE2_COPY", ["COL1", "COL2"]),
                ("DB.SCHEMA.TABLE3", "DB.SCHEMA.TABLE3_COPY", None),  # Hash comparison
            ]
            results = comparer.compare_batch(pairs)
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
                            # Split report into lines for better Excel display
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

    Uses datacompy's SnowflakeCompare for server-side comparison.

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
╔═══════════════════════════════════════════════════════════════════════════╗
║     SNOWFLAKE DATA COMPARISON TOOL                                         ║
║     Using datacompy.SnowflakeCompare (server-side comparison)              ║
╚═══════════════════════════════════════════════════════════════════════════╝

This module uses datacompy's native Snowflake integration via Snowpark.
Comparisons happen SERVER-SIDE in Snowflake for optimal performance.

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
