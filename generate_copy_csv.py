"""
Script to generate CSV files from TABLE_COPY_FREEZE_22.xlsx for COPY automation.
Generates:
- CSVs per UC and PROJECTO
- CSVs for inputs (IN) and outputs (OUT)
- Handles dynamic name patterns (&aniomes, &hoy, &dia_sig, etc.)
- Handles wildcard patterns (SIBS*.txt, Impago_exp_venta_*, etc.)

Usage:
    python generate_copy_csv.py                    # Use current date
    python generate_copy_csv.py 20260115           # Use specific date YYYYMMDD
    python generate_copy_csv.py 2026-01-15         # Use specific date YYYY-MM-DD
"""

import pandas as pd
import os
import re
import sys
from datetime import datetime, timedelta

# ============================================
# PARSE DATE ARGUMENT
# ============================================
def parse_date_argument(args):
    """Parse date from command line arguments"""
    if len(args) > 1:
        date_str = args[1]
        # Try YYYYMMDD format
        try:
            return datetime.strptime(date_str, '%Y%m%d')
        except ValueError:
            pass
        # Try YYYY-MM-DD format
        try:
            return datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            pass
        # Try DD/MM/YYYY format
        try:
            return datetime.strptime(date_str, '%d/%m/%Y')
        except ValueError:
            pass
        print(f"WARNING: Could not parse date '{date_str}'. Using current date.")
    return datetime.now()

# Get reference date from argument or use current date
REFERENCE_DATE = parse_date_argument(sys.argv)
print(f"Reference date: {REFERENCE_DATE.strftime('%Y-%m-%d')} ({REFERENCE_DATE.strftime('%A')})")
print("=" * 60)

# ============================================
# WILDCARD PATTERN HANDLING
# ============================================
def has_wildcard(name):
    """Check if name contains wildcard patterns (* or ?)"""
    if pd.isna(name) or name == 'nan':
        return False
    return '*' in str(name) or '?' in str(name)

def extract_wildcard_pattern(name):
    """Extract the wildcard pattern from name"""
    if pd.isna(name) or name == 'nan':
        return ''
    name_str = str(name)
    if '*' in name_str or '?' in name_str:
        return name_str
    return ''

def wildcard_to_regex(pattern):
    """Convert wildcard pattern to regex pattern"""
    # Escape special regex characters except * and ?
    regex = re.escape(pattern)
    # Convert * to .* and ? to .
    regex = regex.replace(r'\*', '.*').replace(r'\?', '.')
    return f'^{regex}$'

# ============================================
# DYNAMIC PATTERNS CONFIGURATION
# ============================================
# Define the dynamic patterns and their replacement logic
DYNAMIC_PATTERNS = {
    '&aniomes': lambda d: d.strftime('%Y%m'),           # YYYYMM (current month)
    '&aniomes.': lambda d: d.strftime('%Y%m'),          # YYYYMM with trailing dot
    '&hoy': lambda d: d.strftime('%Y%m%d'),             # Today YYYYMMDD
    '&hoy.': lambda d: d.strftime('%Y%m%d'),            # Today with trailing dot
    '&ayer': lambda d: (d - timedelta(days=1)).strftime('%Y%m%d'),  # Yesterday
    '&ayer.': lambda d: (d - timedelta(days=1)).strftime('%Y%m%d'),
    '&dia_sig': lambda d: (d + timedelta(days=1)).strftime('%Y%m%d'),  # Tomorrow
    '&dia_sig.': lambda d: (d + timedelta(days=1)).strftime('%Y%m%d'),
    '&dia': lambda d: d.strftime('%Y%m%d'),             # Current day
    '&dia.': lambda d: d.strftime('%Y%m%d'),
    '&fec_mes': lambda d: d.strftime('%Y%m'),           # Current month YYYYMM
    '&fec_mes.': lambda d: d.strftime('%Y%m'),
    '&fec_mes1.': lambda d: (d.replace(day=1) - timedelta(days=1)).strftime('%Y%m'),  # Month -1
    '&fec_mes2.': lambda d: (d.replace(day=1) - timedelta(days=32)).strftime('%Y%m'), # Month -2
    '&fec_mes3.': lambda d: (d.replace(day=1) - timedelta(days=63)).strftime('%Y%m'), # Month -3
    '&fec_mes4.': lambda d: (d.replace(day=1) - timedelta(days=93)).strftime('%Y%m'), # Month -4
    '&fec_mes5.': lambda d: (d.replace(day=1) - timedelta(days=124)).strftime('%Y%m'), # Month -5
    '&fec_mesant': lambda d: (d.replace(day=1) - timedelta(days=1)).strftime('%Y%m'), # Previous month
    '&fec_mesant.': lambda d: (d.replace(day=1) - timedelta(days=1)).strftime('%Y%m'),
    '&hhmmss': lambda d: d.strftime('%H%M%S'),          # Time HHMMSS
    '&hhmmss.': lambda d: d.strftime('%H%M%S'),
    '&laborable': lambda d: d.strftime('%Y%m%d'),       # Business day (simplified)
    '&laborable.': lambda d: d.strftime('%Y%m%d'),
}

def extract_dynamic_patterns(name):
    """Extract all dynamic patterns from a name"""
    if pd.isna(name) or name == 'nan':
        return []
    patterns = re.findall(r'&[a-zA-Z_0-9]+\.?', str(name))
    return patterns

def resolve_dynamic_name(name, reference_date=None):
    """Replace dynamic patterns with actual values based on reference date"""
    if reference_date is None:
        reference_date = datetime.now()

    if pd.isna(name) or name == 'nan':
        return name

    resolved = str(name)
    # Sort patterns by length (longest first) to avoid partial matches
    # e.g., &fec_mesant should be replaced before &fec_mes
    sorted_patterns = sorted(DYNAMIC_PATTERNS.items(), key=lambda x: len(x[0]), reverse=True)
    for pattern, resolver in sorted_patterns:
        if pattern in resolved:
            resolved = resolved.replace(pattern, resolver(reference_date))

    return resolved

def is_dynamic_name(name):
    """Check if a name contains dynamic patterns"""
    patterns = extract_dynamic_patterns(name)
    return len(patterns) > 0

# Read the Excel file
df = pd.read_excel('TABLE_COPY_FREEZE_22.xlsx', skiprows=2, header=None)
df.columns = ['Col0', 'Col1', 'UC', 'Source_Dest', 'PROJECTO', 'TYPE', 'NAME', 'STATUT', 'Comments', 'Colonne1', 'Oney', 'STATUS2', 'Comments_2']

# Remove header row if present
df = df[df['UC'] != 'UC']

# Clean data
df['UC'] = df['UC'].astype(str).str.strip()
df = df[df['UC'] != 'nan']
df['Source_Dest'] = df['Source_Dest'].astype(str).str.strip()
df['PROJECTO'] = df['PROJECTO'].fillna('').astype(str).str.strip()
df['TYPE'] = df['TYPE'].astype(str).str.strip()
df['NAME'] = df['NAME'].astype(str).str.strip()
df['STATUT'] = df['STATUT'].fillna('').astype(str).str.strip()
df['Oney'] = df['Oney'].fillna('').astype(str).str.strip()
df['Comments'] = df['Comments'].fillna('').astype(str).str.strip()

# Normalize TYPE: Fichier -> File (both are flat files)
df['TYPE_NORMALIZED'] = df['TYPE'].replace({'Fichier': 'File', 'nan': ''})

# ============================================
# ADD DYNAMIC NAME COLUMNS
# ============================================
# Check if name is dynamic
df['IS_DYNAMIC'] = df['NAME'].apply(is_dynamic_name)

# Extract the dynamic patterns used
df['DYNAMIC_PATTERNS'] = df['NAME'].apply(lambda x: '|'.join(extract_dynamic_patterns(x)) if extract_dynamic_patterns(x) else '')

# Resolve names with reference date
df['NAME_RESOLVED'] = df['NAME'].apply(lambda x: resolve_dynamic_name(x, REFERENCE_DATE))

# ============================================
# ADD WILDCARD COLUMNS
# ============================================
# Check if name has wildcard
df['HAS_WILDCARD'] = df['NAME'].apply(has_wildcard)

# Also check if resolved name still has wildcard (for patterns like Impago_exp_venta_*)
df['NAME_RESOLVED_HAS_WILDCARD'] = df['NAME_RESOLVED'].apply(has_wildcard)

# Extract wildcard pattern
df['WILDCARD_PATTERN'] = df['NAME'].apply(extract_wildcard_pattern)

# Generate regex pattern for wildcard matching
df['REGEX_PATTERN'] = df['NAME_RESOLVED'].apply(lambda x: wildcard_to_regex(x) if has_wildcard(x) else '')

# Filter out invalid rows
df = df[df['Source_Dest'].isin(['IN', 'OUT'])]
df = df[df['TYPE'].isin(['Table', 'File', 'Fichier'])]

# Create output directory
output_dir = 'COPY'
os.makedirs(output_dir, exist_ok=True)

# Select relevant columns for COPY CSV
copy_columns = ['UC', 'Source_Dest', 'PROJECTO', 'TYPE', 'TYPE_NORMALIZED', 'NAME', 'NAME_RESOLVED', 'IS_DYNAMIC', 'DYNAMIC_PATTERNS', 'HAS_WILDCARD', 'REGEX_PATTERN', 'STATUT', 'Oney', 'Comments']

def clean_projecto_name(projecto):
    """Clean projecto name for file naming"""
    if not projecto or projecto == 'nan':
        return 'NO_PROJECTO'
    return projecto.replace(' ', '_').replace('/', '_')

# ============================================
# 1. Generate CSVs per UC and PROJECTO
# ============================================
print("=" * 60)
print("1. Generating CSVs per UC and PROJECTO")
print("=" * 60)

for uc in sorted(df['UC'].unique()):
    for projecto in df['PROJECTO'].unique():
        subset = df[(df['UC'] == uc) & (df['PROJECTO'] == projecto)]
        if len(subset) > 0:
            projecto_clean = clean_projecto_name(projecto)
            filename = f"UC{uc}_{projecto_clean}.csv"
            filepath = os.path.join(output_dir, filename)
            subset[copy_columns].to_csv(filepath, index=False, encoding='utf-8')
            print(f"  Created: {filename} ({len(subset)} rows)")

# ============================================
# 2. Generate CSVs for IN (inputs) and OUT (outputs)
# ============================================
print("\n" + "=" * 60)
print("2. Generating CSVs for IN and OUT")
print("=" * 60)

# All inputs
inputs_df = df[df['Source_Dest'] == 'IN']
inputs_df[copy_columns].to_csv(os.path.join(output_dir, 'ALL_INPUTS.csv'), index=False, encoding='utf-8')
print(f"  Created: ALL_INPUTS.csv ({len(inputs_df)} rows)")

# All outputs
outputs_df = df[df['Source_Dest'] == 'OUT']
outputs_df[copy_columns].to_csv(os.path.join(output_dir, 'ALL_OUTPUTS.csv'), index=False, encoding='utf-8')
print(f"  Created: ALL_OUTPUTS.csv ({len(outputs_df)} rows)")

# ============================================
# 3. Generate CSVs per UC, PROJECTO, and Source_Dest
# ============================================
print("\n" + "=" * 60)
print("3. Generating CSVs per UC, PROJECTO, and Source_Dest")
print("=" * 60)

for uc in sorted(df['UC'].unique()):
    for projecto in df['PROJECTO'].unique():
        for source_dest in ['IN', 'OUT']:
            subset = df[(df['UC'] == uc) & (df['PROJECTO'] == projecto) & (df['Source_Dest'] == source_dest)]
            if len(subset) > 0:
                projecto_clean = clean_projecto_name(projecto)
                filename = f"UC{uc}_{projecto_clean}_{source_dest}.csv"
                filepath = os.path.join(output_dir, filename)
                subset[copy_columns].to_csv(filepath, index=False, encoding='utf-8')
                print(f"  Created: {filename} ({len(subset)} rows)")

# ============================================
# 4. Generate CSVs by TYPE (Table vs File)
# ============================================
print("\n" + "=" * 60)
print("4. Generating CSVs by TYPE (Tables vs Files)")
print("=" * 60)

# All Tables
tables_df = df[df['TYPE'] == 'Table']
tables_df[copy_columns].to_csv(os.path.join(output_dir, 'ALL_TABLES.csv'), index=False, encoding='utf-8')
print(f"  Created: ALL_TABLES.csv ({len(tables_df)} rows)")

# All Files (File + Fichier)
files_df = df[df['TYPE'].isin(['File', 'Fichier'])]
files_df[copy_columns].to_csv(os.path.join(output_dir, 'ALL_FILES.csv'), index=False, encoding='utf-8')
print(f"  Created: ALL_FILES.csv ({len(files_df)} rows)")

# Tables INPUT
tables_in_df = df[(df['TYPE'] == 'Table') & (df['Source_Dest'] == 'IN')]
tables_in_df[copy_columns].to_csv(os.path.join(output_dir, 'TABLES_INPUT.csv'), index=False, encoding='utf-8')
print(f"  Created: TABLES_INPUT.csv ({len(tables_in_df)} rows)")

# Tables OUTPUT
tables_out_df = df[(df['TYPE'] == 'Table') & (df['Source_Dest'] == 'OUT')]
tables_out_df[copy_columns].to_csv(os.path.join(output_dir, 'TABLES_OUTPUT.csv'), index=False, encoding='utf-8')
print(f"  Created: TABLES_OUTPUT.csv ({len(tables_out_df)} rows)")

# Files INPUT
files_in_df = df[(df['TYPE'].isin(['File', 'Fichier'])) & (df['Source_Dest'] == 'IN')]
files_in_df[copy_columns].to_csv(os.path.join(output_dir, 'FILES_INPUT.csv'), index=False, encoding='utf-8')
print(f"  Created: FILES_INPUT.csv ({len(files_in_df)} rows)")

# Files OUTPUT
files_out_df = df[(df['TYPE'].isin(['File', 'Fichier'])) & (df['Source_Dest'] == 'OUT')]
files_out_df[copy_columns].to_csv(os.path.join(output_dir, 'FILES_OUTPUT.csv'), index=False, encoding='utf-8')
print(f"  Created: FILES_OUTPUT.csv ({len(files_out_df)} rows)")

# ============================================
# 5. Generate Dynamic Patterns Reference
# ============================================
print("\n" + "=" * 60)
print("5. Generating Dynamic Patterns Reference")
print("=" * 60)

# Create a reference CSV for dynamic patterns
patterns_data = []
for pattern, resolver in DYNAMIC_PATTERNS.items():
    example_value = resolver(REFERENCE_DATE)
    description = {
        '&aniomes': 'Year + Month (YYYYMM)',
        '&aniomes.': 'Year + Month (YYYYMM)',
        '&hoy': 'Today (YYYYMMDD)',
        '&hoy.': 'Today (YYYYMMDD)',
        '&ayer': 'Yesterday (YYYYMMDD)',
        '&ayer.': 'Yesterday (YYYYMMDD)',
        '&dia_sig': 'Tomorrow (YYYYMMDD)',
        '&dia_sig.': 'Tomorrow (YYYYMMDD)',
        '&dia': 'Current day (YYYYMMDD)',
        '&dia.': 'Current day (YYYYMMDD)',
        '&fec_mes': 'Current month (YYYYMM)',
        '&fec_mes.': 'Current month (YYYYMM)',
        '&fec_mes1.': 'Month -1 (YYYYMM)',
        '&fec_mes2.': 'Month -2 (YYYYMM)',
        '&fec_mes3.': 'Month -3 (YYYYMM)',
        '&fec_mes4.': 'Month -4 (YYYYMM)',
        '&fec_mes5.': 'Month -5 (YYYYMM)',
        '&fec_mesant': 'Previous month (YYYYMM)',
        '&fec_mesant.': 'Previous month (YYYYMM)',
        '&hhmmss': 'Time (HHMMSS)',
        '&hhmmss.': 'Time (HHMMSS)',
        '&laborable': 'Business day (YYYYMMDD)',
        '&laborable.': 'Business day (YYYYMMDD)',
    }.get(pattern, 'Unknown')

    # Count occurrences in data
    count = df['DYNAMIC_PATTERNS'].str.contains(re.escape(pattern), na=False).sum()

    patterns_data.append({
        'PATTERN': pattern,
        'DESCRIPTION': description,
        'EXAMPLE_VALUE': example_value,
        'OCCURRENCES': count
    })

patterns_df = pd.DataFrame(patterns_data)
patterns_df = patterns_df[patterns_df['OCCURRENCES'] > 0]  # Only patterns that are used
patterns_df.to_csv(os.path.join(output_dir, 'DYNAMIC_PATTERNS_REFERENCE.csv'), index=False, encoding='utf-8')
print(f"  Created: DYNAMIC_PATTERNS_REFERENCE.csv ({len(patterns_df)} patterns)")

# ============================================
# 6. Generate list of dynamic names only
# ============================================
print("\n" + "=" * 60)
print("6. Generating Dynamic Names List")
print("=" * 60)

dynamic_df = df[df['IS_DYNAMIC'] == True]
dynamic_df[copy_columns].to_csv(os.path.join(output_dir, 'DYNAMIC_NAMES_ONLY.csv'), index=False, encoding='utf-8')
print(f"  Created: DYNAMIC_NAMES_ONLY.csv ({len(dynamic_df)} rows)")

static_df = df[df['IS_DYNAMIC'] == False]
static_df[copy_columns].to_csv(os.path.join(output_dir, 'STATIC_NAMES_ONLY.csv'), index=False, encoding='utf-8')
print(f"  Created: STATIC_NAMES_ONLY.csv ({len(static_df)} rows)")

# ============================================
# 7. Generate Wildcard Names List
# ============================================
print("\n" + "=" * 60)
print("7. Generating Wildcard Names List")
print("=" * 60)

wildcard_df = df[df['HAS_WILDCARD'] == True]
wildcard_df[copy_columns].to_csv(os.path.join(output_dir, 'WILDCARD_NAMES.csv'), index=False, encoding='utf-8')
print(f"  Created: WILDCARD_NAMES.csv ({len(wildcard_df)} rows)")

# ============================================
# 8. Summary
# ============================================
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Total records processed: {len(df)}")
print(f"Reference date used: {REFERENCE_DATE.strftime('%Y-%m-%d')} ({REFERENCE_DATE.strftime('%A')})")
print(f"\nBy Source/Dest:")
print(df.groupby('Source_Dest').size().to_string())
print(f"\nBy TYPE:")
print(df.groupby('TYPE').size().to_string())
print(f"\nBy TYPE_NORMALIZED:")
print(df.groupby('TYPE_NORMALIZED').size().to_string())
print(f"\nBy PROJECTO:")
print(df.groupby('PROJECTO').size().to_string())
print(f"\nDynamic vs Static names:")
print(f"  Dynamic: {len(dynamic_df)}")
print(f"  Static:  {len(static_df)}")
print(f"\nWildcard patterns:")
print(f"  With wildcard (*/?): {len(wildcard_df)}")
if len(wildcard_df) > 0:
    print(f"  Wildcard entries:")
    for _, row in wildcard_df[['NAME', 'NAME_RESOLVED', 'REGEX_PATTERN']].iterrows():
        print(f"    {row['NAME']:40} -> {row['REGEX_PATTERN']}")
print(f"\nDynamic patterns used:")
for _, row in patterns_df.iterrows():
    print(f"  {row['PATTERN']:20} -> {row['EXAMPLE_VALUE']:12} ({row['OCCURRENCES']} uses)")
print(f"\nAll CSV files saved to: {os.path.abspath(output_dir)}")
