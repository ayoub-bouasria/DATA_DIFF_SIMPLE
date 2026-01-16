# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DATA DIFF SIMPLE is a native Snowflake SQL tool for comparing data between SAS exports (CSV files) and Snowflake tables after migration. It validates data integrity during SAS-to-Snowflake migrations.

**Target Database**: Snowflake
**Language**: SQL with JavaScript stored procedures

## Architecture

### Two Deployment Options

1. **Simple Deployment** (`SQL/main.sql`): Single-file installation to `TEAM_DB.EXTERNAL` - recommended for quick setup
2. **Advanced Deployment** (`SQL/00_setup.sql` + `SQL/01_procedures.sql`): Multi-project architecture with dedicated `DATA_DIFF` database containing CONFIG, STAGING, and RESULTS schemas

### Core Tables

- `DIFF_RESULTS` - Stores comparison metadata (row counts, match percentage, execution time)
- `DIFF_DETAILS` - Stores individual differences (ONLY_TABLE1, ONLY_TABLE2, VALUE_DIFF)
- `TEMP_TABLE_COLUMNS` - Helper table for column metadata

### Main Stored Procedures

- `SP_COMPARE(P_TABLE1, P_TABLE2, P_PRIMARY_KEY, P_COLUMNS_TO_COMPARE, P_NUMERIC_TOLERANCE, P_CASE_SENSITIVE, P_SHOW_REPORT)` - Main comparison entry point (JavaScript)
- `SP_SHOW_COMPARISON_REPORT(P_COMPARISON_ID)` - Generates formatted ASCII report
- `SP_SHOW_DIFF_DETAILS(P_COMPARISON_ID, P_DIFF_TYPE, P_LIMIT)` - Shows detailed differences
- `SP_CLEANUP_OLD_COMPARISONS(P_DAYS_TO_KEEP)` - Maintenance cleanup

### Comparison Strategies

**With Primary Key**: Uses JOIN on PK columns, identifies rows only in one table, detects column-level value differences

**Without Primary Key** (pass `NULL` or `'NO'` as P_PRIMARY_KEY): Uses SHA-256 hash of entire row for comparison, identifies mismatched rows but no column-level detail

## Installation

```sql
-- Simple deployment (recommended)
-- Execute SQL/main.sql in Snowflake

-- Or advanced multi-project deployment:
-- 1. Execute SQL/00_setup.sql
-- 2. Execute SQL/01_procedures.sql
```

## Usage Examples

```sql
-- Compare two tables with primary key
CALL SP_COMPARE(
    'DB1.SCHEMA1.MY_TABLE',  -- Table 1 (SAS data)
    'DB2.SCHEMA2.MY_TABLE',  -- Table 2 (Snowflake data)
    'ID',                     -- Primary key column(s)
    'ALL',                    -- Compare all columns
    0.01,                     -- Numeric tolerance
    TRUE,                     -- Case sensitive
    TRUE                      -- Show report
);

-- Compare without primary key (hash-based)
CALL SP_COMPARE('TABLE1', 'TABLE2', NULL, 'ALL', 0, TRUE, TRUE);

-- View recent comparisons
SELECT * FROM V_RECENT_COMPARISONS;

-- View difference details
CALL SP_SHOW_DIFF_DETAILS('comparison-uuid-here');
```

## Key Technical Details

- Uses `OBJECT_CONSTRUCT(*)` for capturing full row data
- Uses `SHA2(..., 256)` for hash-based comparison
- Results stored as `VARIANT` type for flexible schema handling
- NULL normalization: handles `''`, `'.'`, `'NULL'` as NULL values
- Composite primary keys: comma-separated (e.g., `'COL1,COL2'`)

## File Structure

```
SQL/
├── main.sql                 # Complete single-file installation (TEAM_DB.EXTERNAL)
├── 00_setup.sql             # Advanced setup (DATA_DIFF database)
├── 00_setup_team_db.sql     # Alternative setup with helper procedures
├── 01_procedures.sql        # Stored procedures for advanced deployment
├── 02_config_proyecto0.sql  # Example configuration
├── 03_usage_examples.sql    # Usage examples
├── usage_examples_simple.sql
└── tests/
    ├── 00_test_setup.sql    # Create test tables
    └── 01_run_tests.sql     # Execute validation tests

PYTHON/
├── config.py                # Snowflake connection config
├── snowflake_compare.py     # Main comparison module (datacompy)
├── run_comparison.py        # CLI tool
├── examples.py              # Usage examples
├── requirements.txt         # Dependencies
└── README.md                # Python documentation
```

## Running SQL Tests

```sql
-- 1. Create test tables
-- Execute SQL/tests/00_test_setup.sql

-- 2. Run tests
-- Execute SQL/tests/01_run_tests.sql
```

## Python Alternative (datacompy)

For client-side comparison with detailed analysis:

```bash
cd PYTHON
pip install -r requirements.txt
cp .env.template .env  # Configure Snowflake credentials

# CLI usage
python run_comparison.py compare TABLE1 TABLE2 --pk ID
python run_comparison.py batch config.yaml --export results --format excel
```

## Querying Results

```sql
-- Summary by difference type
SELECT TABLE1, TABLE2, DIFF_TYPE, COUNT(*)
FROM DIFF_DETAILS d JOIN DIFF_RESULTS r ON d.COMPARISON_ID = r.COMPARISON_ID
GROUP BY 1, 2, 3;

-- Rows only in first table
SELECT * FROM DIFF_DETAILS WHERE DIFF_TYPE = 'ONLY_TABLE1' AND COMPARISON_ID = '...';
```
