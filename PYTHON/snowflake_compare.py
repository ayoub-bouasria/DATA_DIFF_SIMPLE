"""
Snowflake Data Comparison Module using datacompy.

This module provides functionality to compare Snowflake tables using:
1. SnowparkComparer - Remote comparison using datacompy.snowflake (recommended for large tables)
2. SnowflakeTableComparer - Local comparison using pandas (downloads data first)
3. LocalFileComparer - Compare local CSV/Excel files

Compatible with Python 3.9-3.13 for Snowpark, any version for local files.

Usage:
    from snowflake_compare import SnowparkComparer, quick_compare

    # Remote comparison (Snowpark - recommended)
    with SnowparkComparer() as comparer:
        result = comparer.compare("TABLE1", "TABLE2", join_columns=["ID"])
        print(result)

Requirements:
    pip install snowflake-snowpark-python datacompy pandas
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, List, Dict, Any, Tuple, Union
from dataclasses import dataclass, field
import hashlib
import json
import logging
import sys
import os

# datacompy imports (pandas-based Compare)
import datacompy
from datacompy.core import Compare

from config import SnowflakeConfig, ComparisonConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# Check available backends
# ============================================================================

SNOWPARK_AVAILABLE = False
SNOWPARK_ERROR_MESSAGE = None
SNOWFLAKE_CONNECTOR_AVAILABLE = False
SNOWFLAKE_CONNECTOR_ERROR_MESSAGE = None

# Try to import Snowpark (for remote comparison)
try:
    from snowflake.snowpark import Session
    import datacompy.snowflake as snowflake_compare_module
    SNOWPARK_AVAILABLE = True
    logger.info("Snowpark backend available")
except ImportError as e:
    SNOWPARK_ERROR_MESSAGE = str(e)
except Exception as e:
    SNOWPARK_ERROR_MESSAGE = str(e)

# Try to import snowflake-connector-python (for local pandas comparison)
try:
    import snowflake.connector
    from snowflake.connector import DictCursor
    SNOWFLAKE_CONNECTOR_AVAILABLE = True
    logger.info("Snowflake connector backend available")
except ImportError as e:
    SNOWFLAKE_CONNECTOR_ERROR_MESSAGE = str(e)
except Exception as e:
    SNOWFLAKE_CONNECTOR_ERROR_MESSAGE = str(e)


def get_availability_status() -> str:
    """Return a string describing available backends."""
    status_lines = [
        "=" * 60,
        "SNOWFLAKE COMPARISON BACKENDS STATUS",
        "=" * 60,
        f"Python version: {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "",
    ]

    if SNOWPARK_AVAILABLE:
        status_lines.append("[OK] Snowpark (datacompy.snowflake) - AVAILABLE")
        status_lines.append("     -> Use SnowparkComparer for remote comparison")
    else:
        status_lines.append(f"[X] Snowpark - NOT AVAILABLE: {SNOWPARK_ERROR_MESSAGE}")
        status_lines.append("     -> Install: pip install snowflake-snowpark-python")
        status_lines.append("     -> Requires Python 3.9-3.13")

    status_lines.append("")

    if SNOWFLAKE_CONNECTOR_AVAILABLE:
        status_lines.append("[OK] Snowflake Connector - AVAILABLE")
        status_lines.append("     -> Use SnowflakeTableComparer for pandas-based comparison")
    else:
        status_lines.append(f"[X] Snowflake Connector - NOT AVAILABLE: {SNOWFLAKE_CONNECTOR_ERROR_MESSAGE}")
        status_lines.append("     -> Install: pip install snowflake-connector-python")

    status_lines.append("")
    status_lines.append("[OK] Local file comparison - ALWAYS AVAILABLE")
    status_lines.append("     -> Use LocalFileComparer for CSV/Excel files")
    status_lines.append("=" * 60)

    return "\n".join(status_lines)


def check_snowpark():
    """Check if Snowpark is available and provide helpful error message."""
    if not SNOWPARK_AVAILABLE:
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        error_msg = f"""
================================================================================
SNOWPARK NOT AVAILABLE
================================================================================

Error: {SNOWPARK_ERROR_MESSAGE}

Python version: {python_version}

Snowpark requires Python 3.9-3.13 and snowflake-snowpark-python package.

SOLUTIONS:
----------

1. Install Snowpark (Python 3.9-3.13 required):
   pip install snowflake-snowpark-python

2. If using Python 3.14+, downgrade to Python 3.11-3.13:
   python3.11 -m venv venv
   venv\\Scripts\\activate  # Windows
   pip install -r requirements.txt

3. Use LocalFileComparer for local file comparison (always available)

================================================================================
"""
        raise RuntimeError(error_msg)


# ============================================================================
# Data Classes
# ============================================================================

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
    comparison_mode: str = "unknown"  # 'snowpark', 'pandas', 'local'

    # Detailed diff data (optional, for export)
    diff_details: Optional[pd.DataFrame] = None
    df1_unq_rows: Optional[pd.DataFrame] = None
    df2_unq_rows: Optional[pd.DataFrame] = None

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
            "comparison_mode": self.comparison_mode,
        }

    def __str__(self) -> str:
        """Return a formatted string representation."""
        status = "IDENTICAL" if self.is_identical else "DIFFERENT"
        pk_info = ", ".join(self.primary_key_columns) if self.primary_key_columns else "None (hash)"

        lines = [
            "+" + "=" * 77 + "+",
            "|" + f"COMPARISON REPORT ({self.comparison_mode})".center(77) + "|",
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


# ============================================================================
# SnowparkComparer - Remote comparison using datacompy.snowflake
# ============================================================================

class SnowparkComparer:
    """
    Compare Snowflake tables using datacompy.snowflake.SnowflakeCompare.

    This class performs comparison directly on Snowflake servers using Snowpark,
    which is much more efficient for large tables as data doesn't need to be
    downloaded locally.

    Requires Python 3.9-3.13 and snowflake-snowpark-python package.
    """

    def __init__(
        self,
        snowflake_config: Optional[SnowflakeConfig] = None,
        comparison_config: Optional[ComparisonConfig] = None,
        credentials_file: Optional[str] = None,
    ):
        """
        Initialize the Snowpark comparer.

        Args:
            snowflake_config: Snowflake connection configuration (from .env if None)
            comparison_config: Comparison settings
            credentials_file: Path to JSON credentials file (alternative to config)
        """
        check_snowpark()

        self.sf_config = snowflake_config or SnowflakeConfig.from_env()
        self.cmp_config = comparison_config or ComparisonConfig()
        self.credentials_file = credentials_file
        self._session = None

    def _get_session_parameters(self) -> dict:
        """Build Snowpark session parameters from config."""
        # If credentials file is provided, load from it
        if self.credentials_file and os.path.exists(self.credentials_file):
            with open(self.credentials_file, 'r') as f:
                return json.load(f)

        # Otherwise, build from config
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

    def connect(self) -> "Session":
        """Create Snowpark session."""
        if self._session is None:
            logger.info(f"Creating Snowpark session for account: {self.sf_config.account}")
            connection_params = self._get_session_parameters()
            self._session = Session.builder.configs(connection_params).create()
            logger.info("Snowpark session established")
        return self._session

    def close(self) -> None:
        """Close Snowpark session."""
        if self._session is not None:
            try:
                self._session.close()
            except Exception as e:
                logger.warning(f"Error closing session: {e}")
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

    def use_database_schema(self, database: str, schema: str) -> None:
        """Set the active database and schema."""
        session = self.connect()
        session.sql(f"USE DATABASE {database}").collect()
        session.sql(f"USE SCHEMA {schema}").collect()
        logger.info(f"Using {database}.{schema}")

    def compare(
        self,
        table1: str,
        table2: str,
        join_columns: Optional[Union[str, List[str], set]] = None,
        abs_tol: float = 1e-4,
        rel_tol: float = 1e-3,
        ignore_spaces: bool = True,
        ignore_case: bool = False,
        database: Optional[str] = None,
        schema1: Optional[str] = None,
        schema2: Optional[str] = None,
        **kwargs,
    ) -> ComparisonResult:
        """
        Compare two Snowflake tables using datacompy.snowflake.SnowflakeCompare.

        Comparison runs directly on Snowflake - no data download required.

        Args:
            table1: First table name (can be fully qualified: DB.SCHEMA.TABLE)
            table2: Second table name
            join_columns: Column(s) to join on (required for SnowflakeCompare)
            abs_tol: Absolute tolerance for numeric comparisons
            rel_tol: Relative tolerance for numeric comparisons
            ignore_spaces: Ignore leading/trailing spaces in strings
            ignore_case: Ignore case in string comparisons
            database: Database name (uses config default if not specified)
            schema1: Schema for table1 (uses config default if not specified)
            schema2: Schema for table2 (uses schema1 if not specified)

        Returns:
            ComparisonResult object with detailed comparison results
        """
        start_time = datetime.now()
        comparison_id = self._generate_comparison_id()

        # Parse join_columns
        if isinstance(join_columns, str):
            join_columns = [c.strip().upper() for c in join_columns.split(",")]
        elif isinstance(join_columns, set):
            join_columns = list(join_columns)
        elif join_columns:
            join_columns = [c.upper() for c in join_columns]

        if not join_columns:
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
                execution_time_seconds=0.0,
                error_message="join_columns is required for Snowpark comparison. Use LocalFileComparer for hash-based comparison.",
                comparison_mode="snowpark",
            )

        try:
            session = self.connect()

            # Build fully qualified table names
            db = database or self.sf_config.database
            sch1 = schema1 or self.sf_config.schema
            sch2 = schema2 or sch1

            # If table names already contain dots, use them as-is
            if "." in table1:
                full_table1 = table1
            else:
                full_table1 = f"{db}.{sch1}.{table1}"

            if "." in table2:
                full_table2 = table2
            else:
                full_table2 = f"{db}.{sch2}.{table2}"

            logger.info(f"Comparing tables using Snowpark (remote):")
            logger.info(f"  Table 1: {full_table1}")
            logger.info(f"  Table 2: {full_table2}")
            logger.info(f"  Join columns: {join_columns}")

            # Create SnowflakeCompare instance
            compare = snowflake_compare_module.SnowflakeCompare(
                session,
                full_table1,
                full_table2,
                join_columns=join_columns,
                abs_tol=abs_tol,
                rel_tol=rel_tol,
                ignore_spaces=ignore_spaces,
            )

            # Check if tables match
            is_identical = compare.matches()
            all_columns_match = compare.all_columns_match()

            # Get unique rows as pandas DataFrames
            df1_unq = compare.df1_unq_rows.to_pandas() if hasattr(compare.df1_unq_rows, 'to_pandas') else pd.DataFrame()
            df2_unq = compare.df2_unq_rows.to_pandas() if hasattr(compare.df2_unq_rows, 'to_pandas') else pd.DataFrame()

            rows_only_t1 = len(df1_unq)
            rows_only_t2 = len(df2_unq)

            # Get row counts from the tables
            try:
                table1_count = session.sql(f"SELECT COUNT(*) FROM {full_table1}").collect()[0][0]
                table2_count = session.sql(f"SELECT COUNT(*) FROM {full_table2}").collect()[0][0]
            except Exception as e:
                logger.warning(f"Could not get row counts: {e}")
                table1_count = 0
                table2_count = 0

            # Calculate matched rows
            matched_rows = min(table1_count, table2_count) - max(rows_only_t1, rows_only_t2)
            if matched_rows < 0:
                matched_rows = 0

            # Calculate match percentage
            total_rows = table1_count + table2_count
            if total_rows > 0:
                match_pct = (2 * matched_rows / total_rows) * 100
            else:
                match_pct = 100.0 if is_identical else 0.0

            # Determine final status
            final_is_identical = is_identical and df1_unq.empty and df2_unq.empty

            exec_time = (datetime.now() - start_time).total_seconds()

            # Build report
            report_lines = [
                "",
                "=" * 70,
                "SNOWPARK REMOTE COMPARISON REPORT",
                "=" * 70,
                "",
                f"Table 1: {full_table1}",
                f"Table 2: {full_table2}",
                f"Join Columns: {join_columns}",
                "",
                f"Table 1 rows: {table1_count:,}",
                f"Table 2 rows: {table2_count:,}",
                f"Rows only in Table 1: {rows_only_t1:,}",
                f"Rows only in Table 2: {rows_only_t2:,}",
                f"Matched rows: {matched_rows:,}",
                "",
                f"All columns match: {all_columns_match}",
                f"Tables identical: {final_is_identical}",
                f"Match percentage: {match_pct:.2f}%",
                "",
                "=" * 70,
            ]

            return ComparisonResult(
                comparison_id=comparison_id,
                table1_name=full_table1,
                table2_name=full_table2,
                comparison_time=start_time,
                table1_row_count=table1_count,
                table2_row_count=table2_count,
                matched_rows=int(matched_rows),
                rows_only_in_table1=rows_only_t1,
                rows_only_in_table2=rows_only_t2,
                rows_with_diff_values=0,
                match_percentage=match_pct,
                is_identical=final_is_identical,
                has_primary_key=True,
                primary_key_columns=join_columns,
                execution_time_seconds=exec_time,
                comparison_mode="snowpark",
                df1_unq_rows=df1_unq if not df1_unq.empty else None,
                df2_unq_rows=df2_unq if not df2_unq.empty else None,
                datacompy_report="\n".join(report_lines),
            )

        except Exception as e:
            exec_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Snowpark comparison failed: {e}")
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
                has_primary_key=bool(join_columns),
                primary_key_columns=join_columns or [],
                execution_time_seconds=exec_time,
                error_message=str(e),
                comparison_mode="snowpark",
            )

    def export_results(
        self,
        results: List[ComparisonResult],
        output_path: str,
        format: str = "csv",
        include_details: bool = True,
    ) -> str:
        """Export comparison results to file."""
        records = [r.to_dict() for r in results]
        df = pd.DataFrame(records)

        if format == "csv":
            filepath = f"{output_path}.csv"
            df.to_csv(filepath, index=False)
        elif format == "excel":
            filepath = f"{output_path}.xlsx"
            with pd.ExcelWriter(filepath, engine="xlsxwriter") as writer:
                df.to_excel(writer, sheet_name="Summary", index=False)
                if include_details:
                    for result in results:
                        if result.df1_unq_rows is not None and len(result.df1_unq_rows) > 0:
                            sheet_name = f"t1_unq_{result.comparison_id[:20]}"
                            result.df1_unq_rows.to_excel(writer, sheet_name=sheet_name, index=False)
                        if result.df2_unq_rows is not None and len(result.df2_unq_rows) > 0:
                            sheet_name = f"t2_unq_{result.comparison_id[:20]}"
                            result.df2_unq_rows.to_excel(writer, sheet_name=sheet_name, index=False)
        elif format == "json":
            filepath = f"{output_path}.json"
            output_data = []
            for r in results:
                data = r.to_dict()
                if include_details:
                    if r.df1_unq_rows is not None:
                        data["df1_unique_rows"] = r.df1_unq_rows.to_dict(orient="records")
                    if r.df2_unq_rows is not None:
                        data["df2_unique_rows"] = r.df2_unq_rows.to_dict(orient="records")
                output_data.append(data)
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(output_data, f, indent=2, default=str, ensure_ascii=False)
        else:
            raise ValueError(f"Unknown format: {format}")

        logger.info(f"Results exported to: {filepath}")
        return filepath


# ============================================================================
# LocalFileComparer - Compare local CSV/Excel files
# ============================================================================

class LocalFileComparer:
    """
    Compare local CSV/Excel files using datacompy.

    Always available, no Snowflake connection required.
    """

    def __init__(self, comparison_config: Optional[ComparisonConfig] = None):
        self.cmp_config = comparison_config or ComparisonConfig()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def _generate_comparison_id(self) -> str:
        timestamp = datetime.now().isoformat()
        return hashlib.md5(timestamp.encode()).hexdigest()[:16]

    def _load_file(self, file_path: str) -> pd.DataFrame:
        """Load a file into a pandas DataFrame."""
        file_path_lower = file_path.lower()
        if file_path_lower.endswith('.csv'):
            return pd.read_csv(file_path)
        elif file_path_lower.endswith(('.xlsx', '.xls')):
            return pd.read_excel(file_path)
        elif file_path_lower.endswith('.parquet'):
            return pd.read_parquet(file_path)
        elif file_path_lower.endswith('.json'):
            return pd.read_json(file_path)
        else:
            return pd.read_csv(file_path)

    def compare(
        self,
        table1: str,
        table2: str,
        join_columns: Optional[Union[str, List[str]]] = None,
        abs_tol: Optional[float] = None,
        rel_tol: float = 0,
        ignore_spaces: bool = True,
        ignore_case: bool = True,
        **kwargs,
    ) -> ComparisonResult:
        """Compare two local files using datacompy.Compare."""
        start_time = datetime.now()
        comparison_id = self._generate_comparison_id()

        if abs_tol is None:
            abs_tol = self.cmp_config.numeric_tolerance

        if isinstance(join_columns, str):
            join_columns = [c.strip() for c in join_columns.split(",")]

        try:
            logger.info(f"Comparing local files:")
            logger.info(f"  File 1: {table1}")
            logger.info(f"  File 2: {table2}")

            df1 = self._load_file(table1)
            df2 = self._load_file(table2)

            table1_count = len(df1)
            table2_count = len(df2)

            # Normalize column names
            df1.columns = [c.upper() for c in df1.columns]
            df2.columns = [c.upper() for c in df2.columns]

            if join_columns:
                join_columns = [c.upper() for c in join_columns]

            # Hash-based comparison if no join columns
            if not join_columns:
                return self._compare_hash_local(df1, df2, table1, table2, comparison_id, start_time)

            comparison = Compare(
                df1=df1,
                df2=df2,
                join_columns=join_columns,
                abs_tol=abs_tol,
                rel_tol=rel_tol,
                df1_name=os.path.basename(table1),
                df2_name=os.path.basename(table2),
                ignore_spaces=ignore_spaces,
                ignore_case=ignore_case,
            )

            is_identical = comparison.matches()
            rows_only_t1 = len(comparison.df1_unq_rows)
            rows_only_t2 = len(comparison.df2_unq_rows)
            intersect_count = comparison.intersect_rows.shape[0]

            mismatch_columns = [c for c in comparison.intersect_rows.columns if c.endswith('_match')]
            if mismatch_columns:
                all_match = comparison.intersect_rows[mismatch_columns].all(axis=1)
                matched_rows = all_match.sum()
                rows_with_diff = intersect_count - matched_rows
            else:
                matched_rows = intersect_count
                rows_with_diff = 0

            total_rows = table1_count + table2_count
            match_pct = (2 * matched_rows / total_rows) * 100 if total_rows > 0 else 100.0

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
                execution_time_seconds=exec_time,
                comparison_mode="local",
                datacompy_report=comparison.report(),
            )

        except Exception as e:
            exec_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"Local comparison failed: {e}")
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
                has_primary_key=bool(join_columns),
                primary_key_columns=join_columns or [],
                execution_time_seconds=exec_time,
                error_message=str(e),
                comparison_mode="local",
            )

    def _compare_hash_local(
        self, df1: pd.DataFrame, df2: pd.DataFrame,
        table1: str, table2: str, comparison_id: str, start_time: datetime
    ) -> ComparisonResult:
        """Hash-based comparison for local files."""
        try:
            table1_count = len(df1)
            table2_count = len(df2)

            def row_hash(row):
                return hashlib.sha256(str(row.to_dict()).encode()).hexdigest()

            df1_copy = df1.copy()
            df2_copy = df2.copy()
            df1_copy['_hash'] = df1.apply(row_hash, axis=1)
            df2_copy['_hash'] = df2.apply(row_hash, axis=1)

            hash_counts1 = df1_copy['_hash'].value_counts()
            hash_counts2 = df2_copy['_hash'].value_counts()

            common_hashes = set(hash_counts1.index) & set(hash_counts2.index)
            matched_rows = sum(min(hash_counts1[h], hash_counts2[h]) for h in common_hashes)

            rows_only_t1 = table1_count - sum(min(hash_counts1.get(h, 0), hash_counts2.get(h, 0)) for h in hash_counts1.index)
            rows_only_t2 = table2_count - sum(min(hash_counts1.get(h, 0), hash_counts2.get(h, 0)) for h in hash_counts2.index)

            total_rows = table1_count + table2_count
            match_pct = (2 * matched_rows / total_rows) * 100 if total_rows > 0 else 100.0
            is_identical = table1_count == table2_count and rows_only_t1 == 0 and rows_only_t2 == 0

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
                execution_time_seconds=exec_time,
                comparison_mode="local_hash",
            )

        except Exception as e:
            exec_time = (datetime.now() - start_time).total_seconds()
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
                comparison_mode="local_hash",
            )

    def export_results(self, results: List[ComparisonResult], output_path: str, format: str = "csv", **kwargs) -> str:
        records = [r.to_dict() for r in results]
        df = pd.DataFrame(records)
        if format == "csv":
            filepath = f"{output_path}.csv"
            df.to_csv(filepath, index=False)
        elif format == "excel":
            filepath = f"{output_path}.xlsx"
            df.to_excel(filepath, index=False)
        elif format == "json":
            filepath = f"{output_path}.json"
            with open(filepath, "w") as f:
                json.dump(records, f, indent=2, default=str)
        else:
            raise ValueError(f"Unknown format: {format}")
        return filepath


# ============================================================================
# Aliases and convenience functions
# ============================================================================

# Main recommended comparer (Snowpark if available, otherwise local)
SnowflakeDataComparer = SnowparkComparer if SNOWPARK_AVAILABLE else LocalFileComparer


def quick_compare(
    table1: str,
    table2: str,
    join_columns: Optional[Union[str, List[str]]] = None,
    use_snowpark: bool = True,
    **kwargs,
) -> ComparisonResult:
    """
    Quick comparison function.

    Uses Snowpark for remote comparison if available and use_snowpark=True,
    otherwise falls back to local file comparison.
    """
    # Determine if inputs are local files
    is_file1 = os.path.exists(table1) or any(table1.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.json', '.parquet'])
    is_file2 = os.path.exists(table2) or any(table2.lower().endswith(ext) for ext in ['.csv', '.xlsx', '.json', '.parquet'])

    if is_file1 or is_file2:
        with LocalFileComparer() as comparer:
            return comparer.compare(table1, table2, join_columns=join_columns, **kwargs)
    elif use_snowpark and SNOWPARK_AVAILABLE:
        with SnowparkComparer() as comparer:
            return comparer.compare(table1, table2, join_columns=join_columns, **kwargs)
    else:
        raise RuntimeError(
            "No suitable comparer available. "
            "For Snowflake tables, install snowflake-snowpark-python (Python 3.9-3.13). "
            "For local files, provide file paths with extensions (.csv, .xlsx, etc.)"
        )


compare_tables = quick_compare


if __name__ == "__main__":
    print(get_availability_status())
    print("""
Usage:
    from snowflake_compare import SnowparkComparer, LocalFileComparer, quick_compare

    # Remote Snowflake comparison (Snowpark)
    with SnowparkComparer() as comparer:
        result = comparer.compare("TABLE1", "TABLE2", join_columns=["ID"])
        print(result)

    # Local file comparison
    with LocalFileComparer() as comparer:
        result = comparer.compare("file1.csv", "file2.csv", join_columns=["ID"])
        print(result)

CLI:
    python run_comparison.py compare TABLE1 TABLE2 --pk ID --mode snowpark
    python run_comparison.py compare file1.csv file2.csv --pk ID
""")
