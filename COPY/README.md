# COPY Automation - Documentation

Ce dossier contient les fichiers CSV générés pour automatiser la copie des tables et fichiers entre SAS et Snowflake.

## Table des matières

- [Quick Start](#quick-start)
- [Génération des CSV](#génération-des-csv)
- [Fichiers CSV générés](#fichiers-csv-générés)
- [Structure des colonnes](#structure-des-colonnes)
- [Patterns dynamiques](#patterns-dynamiques)
- [Patterns Wildcard](#patterns-wildcard)
- [Exemples d'utilisation](#exemples-dutilisation)

---

## Quick Start

```bash
# 1. Générer les CSV avec la date courante
python generate_copy_csv.py

# 2. Générer les CSV avec une date spécifique
python generate_copy_csv.py 20260115

# 3. Utiliser les CSV pour la copie
python COPY/copy_tables.py --csv COPY/TABLES_INPUT.csv
```

---

## Génération des CSV

### Script principal

```bash
python generate_copy_csv.py [DATE]
```

### Formats de date acceptés

| Format | Exemple |
|--------|---------|
| YYYYMMDD | `20260115` |
| YYYY-MM-DD | `2026-01-15` |
| DD/MM/YYYY | `15/01/2026` |
| *(aucun)* | Date courante |

### Fichier source

Le script lit le fichier Excel `TABLE_COPY_FREEZE_22.xlsx` situé à la racine du projet.

---

## Fichiers CSV générés

### Par UC et PROJECTO

| Fichier | Description | Rows |
|---------|-------------|------|
| `UC1_NO_PROJECTO.csv` | UC1 sans projet spécifique | 18 |
| `UC1_ALTI_2.csv` | UC1 - ALTI 2 | 70 |
| `UC1_ALTI_3.csv` | UC1 - ALTI 3 | 2 |
| `UC1_ALTI_4.csv` | UC1 - ALTI 4 | 28 |

### Par UC, PROJECTO et Direction (IN/OUT)

| Fichier | Description |
|---------|-------------|
| `UC1_NO_PROJECTO_IN.csv` | UC1 sans projet - Entrées |
| `UC1_NO_PROJECTO_OUT.csv` | UC1 sans projet - Sorties |
| `UC1_ALTI_2_IN.csv` | UC1 ALTI 2 - Entrées |
| `UC1_ALTI_2_OUT.csv` | UC1 ALTI 2 - Sorties |
| `UC1_ALTI_3_OUT.csv` | UC1 ALTI 3 - Sorties |
| `UC1_ALTI_4_IN.csv` | UC1 ALTI 4 - Entrées |
| `UC1_ALTI_4_OUT.csv` | UC1 ALTI 4 - Sorties |

### Par Direction globale

| Fichier | Description | Usage recommandé |
|---------|-------------|------------------|
| `ALL_INPUTS.csv` | Toutes les entrées (IN) | Copie des données sources |
| `ALL_OUTPUTS.csv` | Toutes les sorties (OUT) | Validation des résultats |

### Par Type (Table vs Fichier)

| Fichier | Description | Usage recommandé |
|---------|-------------|------------------|
| `ALL_TABLES.csv` | Toutes les tables Snowflake | Comparaison SQL |
| `ALL_FILES.csv` | Tous les fichiers plats | Copie fichiers Azure/SFTP |
| `TABLES_INPUT.csv` | Tables en entrée | **Principal pour DATA_DIFF** |
| `TABLES_OUTPUT.csv` | Tables en sortie | Validation résultats |
| `FILES_INPUT.csv` | Fichiers plats en entrée | Import fichiers |
| `FILES_OUTPUT.csv` | Fichiers plats en sortie | Export fichiers |

### Fichiers spéciaux

| Fichier | Description |
|---------|-------------|
| `DYNAMIC_PATTERNS_REFERENCE.csv` | Référence des patterns de date |
| `DYNAMIC_NAMES_ONLY.csv` | Entrées avec noms dynamiques |
| `STATIC_NAMES_ONLY.csv` | Entrées avec noms statiques |
| `WILDCARD_NAMES.csv` | Entrées avec wildcards (*) |

---

## Structure des colonnes

### Colonnes principales

| Colonne | Type | Description | Exemple |
|---------|------|-------------|---------|
| `UC` | String | Numéro du Use Case | `1` |
| `Source_Dest` | String | Direction du flux | `IN` ou `OUT` |
| `PROJECTO` | String | Nom du projet | `ALTI 2`, `ALTI 3`, `ALTI 4`, ou vide |
| `TYPE` | String | Type original | `Table`, `File`, `Fichier` |
| `TYPE_NORMALIZED` | String | Type normalisé | `Table` ou `File` |
| `NAME` | String | Nom original avec variables | `stock_recobro_&fec_mes.` |
| `NAME_RESOLVED` | String | Nom avec dates résolues | `stock_recobro_202601.` |
| `STATUT` | String | Statut de validation | `OK`, `NO`, `~` |
| `Oney` | String | Chemin/table Snowflake alternatif | `shared_db.esd_altitude.xxx` |
| `Comments` | String | Commentaires | Notes diverses |

### Colonnes dynamiques

| Colonne | Type | Description | Exemple |
|---------|------|-------------|---------|
| `IS_DYNAMIC` | Boolean | Contient des patterns de date | `True` / `False` |
| `DYNAMIC_PATTERNS` | String | Patterns trouvés (séparés par `\|`) | `&fec_mes.\|&dia_sig.` |

### Colonnes wildcard

| Colonne | Type | Description | Exemple |
|---------|------|-------------|---------|
| `HAS_WILDCARD` | Boolean | Contient `*` ou `?` | `True` / `False` |
| `REGEX_PATTERN` | String | Pattern regex pour matching | `^SIBS.*\.txt\(E002\)$` |

---

## Patterns dynamiques

### Liste des patterns supportés

| Pattern | Description | Format | Exemple (2026-01-15) |
|---------|-------------|--------|----------------------|
| `&hoy` | Aujourd'hui | YYYYMMDD | `20260115` |
| `&ayer` | Hier | YYYYMMDD | `20260114` |
| `&dia_sig` | Demain | YYYYMMDD | `20260116` |
| `&dia` | Jour courant | YYYYMMDD | `20260115` |
| `&aniomes` | Année + Mois | YYYYMM | `202601` |
| `&fec_mes` | Mois courant | YYYYMM | `202601` |
| `&fec_mesant` | Mois précédent | YYYYMM | `202512` |
| `&fec_mes1` | Mois -1 | YYYYMM | `202512` |
| `&fec_mes2` | Mois -2 | YYYYMM | `202511` |
| `&fec_mes3` | Mois -3 | YYYYMM | `202510` |
| `&fec_mes4` | Mois -4 | YYYYMM | `202509` |
| `&fec_mes5` | Mois -5 | YYYYMM | `202508` |
| `&hhmmss` | Heure courante | HHMMSS | `143052` |
| `&laborable` | Jour ouvrable | YYYYMMDD | `20260115` |

> **Note**: Les patterns peuvent avoir un `.` final (ex: `&fec_mes.`)

### Exemples de résolution

| NAME original | NAME_RESOLVED (2026-01-15) |
|---------------|----------------------------|
| `stock_recobro_&fec_mes.` | `stock_recobro_202601.` |
| `altitude_&dia_sig.` | `altitude_20260116.` |
| `ficheros_e002_&ayer.` | `ficheros_e002_20260114.` |
| `remesa_bbva_&hoy.` | `remesa_bbva_20260115.` |
| `pagos_recobro_&fec_mesant.` | `pagos_recobro_202512.` |

---

## Patterns Wildcard

### Wildcards détectés

| NAME | REGEX_PATTERN | Exemple fichier réel |
|------|---------------|----------------------|
| `SIBS*.txt(E002)` | `^SIBS.*\.txt\(E002\)$` | `SIBS_META-E002_REI-A1303055.txt` |
| `Impago_exp_venta_*` | `^Impago_exp_venta_.*$` | `IMPAGO_EXP_VENTA_202512` |

### Utilisation du REGEX_PATTERN en Python

```python
import re
import os

# Lire le CSV
df = pd.read_csv('COPY/WILDCARD_NAMES.csv')

# Pour chaque entrée avec wildcard
for _, row in df[df['HAS_WILDCARD'] == True].iterrows():
    pattern = row['REGEX_PATTERN']

    # Lister les fichiers dans un répertoire
    files = os.listdir('/path/to/files')

    # Matcher les fichiers
    matching_files = [f for f in files if re.match(pattern, f, re.IGNORECASE)]
    print(f"Files matching {row['NAME']}: {matching_files}")
```

---

## Exemples d'utilisation

### 1. Copier toutes les tables INPUT

```python
import pandas as pd

# Charger le CSV
df = pd.read_csv('COPY/TABLES_INPUT.csv')

# Filtrer par statut OK
df_ok = df[df['STATUT'] == 'OK']

# Itérer sur les tables
for _, row in df_ok.iterrows():
    table_name = row['NAME_RESOLVED']
    snowflake_path = row['Oney'] if row['Oney'] else None

    print(f"Processing: {table_name}")
    if snowflake_path:
        print(f"  -> Snowflake: {snowflake_path}")
```

### 2. Générer les commandes COPY pour Snowflake

```python
import pandas as pd

df = pd.read_csv('COPY/TABLES_INPUT.csv')

for _, row in df.iterrows():
    if row['TYPE_NORMALIZED'] == 'Table' and row['STATUT'] == 'OK':
        source = row['NAME_RESOLVED']
        target = row['Oney'] if row['Oney'] else f"TARGET_SCHEMA.{source}"

        print(f"""
-- Copy {source}
CREATE OR REPLACE TABLE {target} AS
SELECT * FROM SOURCE_SCHEMA.{source};
""")
```

### 3. Filtrer par PROJECTO

```python
import pandas as pd

# Charger uniquement ALTI 2
df = pd.read_csv('COPY/UC1_ALTI_2.csv')

# Séparer entrées et sorties
inputs = df[df['Source_Dest'] == 'IN']
outputs = df[df['Source_Dest'] == 'OUT']

print(f"ALTI 2 - Inputs: {len(inputs)}, Outputs: {len(outputs)}")
```

### 4. Traiter les fichiers avec wildcards

```python
import pandas as pd
import re
import os

df = pd.read_csv('COPY/WILDCARD_NAMES.csv')
source_dir = '/path/to/source/files'

for _, row in df.iterrows():
    if row['HAS_WILDCARD']:
        pattern = row['REGEX_PATTERN']
        files = [f for f in os.listdir(source_dir)
                 if re.match(pattern, f, re.IGNORECASE)]

        for file in files:
            print(f"Found: {file} (matches {row['NAME']})")
```

---

## Statuts

| Statut | Signification | Action |
|--------|---------------|--------|
| `OK` | Validé, prêt pour copie | Copier directement |
| `NO` | Non trouvé ou problème | Vérifier manuellement |
| `~` | À vérifier | Consulter la colonne `Oney` ou `Comments` |

---

## Arborescence des fichiers

```
DATA_DIFF_SIMPLE/
├── TABLE_COPY_FREEZE_22.xlsx    # Fichier source Excel
├── generate_copy_csv.py          # Script de génération
└── COPY/
    ├── README.md                 # Cette documentation
    │
    ├── # Par UC et PROJECTO
    ├── UC1_NO_PROJECTO.csv
    ├── UC1_ALTI_2.csv
    ├── UC1_ALTI_3.csv
    ├── UC1_ALTI_4.csv
    │
    ├── # Par UC, PROJECTO et Direction
    ├── UC1_NO_PROJECTO_IN.csv
    ├── UC1_NO_PROJECTO_OUT.csv
    ├── UC1_ALTI_2_IN.csv
    ├── UC1_ALTI_2_OUT.csv
    ├── UC1_ALTI_3_OUT.csv
    ├── UC1_ALTI_4_IN.csv
    ├── UC1_ALTI_4_OUT.csv
    │
    ├── # Globaux
    ├── ALL_INPUTS.csv
    ├── ALL_OUTPUTS.csv
    │
    ├── # Par Type
    ├── ALL_TABLES.csv
    ├── ALL_FILES.csv
    ├── TABLES_INPUT.csv          # ⭐ Principal pour DATA_DIFF
    ├── TABLES_OUTPUT.csv
    ├── FILES_INPUT.csv
    ├── FILES_OUTPUT.csv
    │
    ├── # Spéciaux
    ├── DYNAMIC_PATTERNS_REFERENCE.csv
    ├── DYNAMIC_NAMES_ONLY.csv
    ├── STATIC_NAMES_ONLY.csv
    └── WILDCARD_NAMES.csv
```

---

## Notes importantes

1. **Date de référence**: Toujours vérifier que la date passée en argument correspond à la date d'exécution SAS attendue.

2. **Colonne Oney**: Contient souvent le chemin Snowflake complet quand différent du nom SAS (ex: `shared_db.esd_altitude.xxx`).

3. **Fichiers vs Tables**:
   - `TYPE = Table` → Table Snowflake
   - `TYPE = File` ou `Fichier` → Fichier plat (CSV, TXT, XLSX)

4. **Wildcards**: Les entrées avec `HAS_WILDCARD = True` nécessitent un listing du répertoire source pour identifier les fichiers réels.

5. **Régénération**: Relancer `python generate_copy_csv.py [DATE]` après modification du fichier Excel source.
