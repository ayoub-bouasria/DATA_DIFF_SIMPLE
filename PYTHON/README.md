# DATA DIFF PYTHON - Snowflake Table Comparison

Outil Python pour comparer des tables Snowflake en utilisant **datacompy** avec l'intégration native **Snowpark**.

## Avantage: Comparaison côté serveur

Ce module utilise `datacompy.SnowflakeCompare` qui effectue les comparaisons **directement dans Snowflake** via Snowpark. Cela signifie:

- **Pas de transfert de données** vers le client
- **Performances optimales** pour les grandes tables
- **Moins de mémoire** utilisée localement
- Support de la **tolérance numérique** (`abs_tol`, `rel_tol`)

## Compatibilité Python

Ce projet est compatible avec **Python 3.10, 3.11, 3.12, et 3.13**.

| Python Version | Status | Notes |
|----------------|--------|-------|
| 3.10 | ✅ Supporté | Recommandé pour la stabilité |
| 3.11 | ✅ Supporté | Recommandé pour la stabilité |
| 3.12 | ✅ Supporté | Snowpark >= 1.20.0 requis |
| 3.13 | ✅ Supporté | Snowpark >= 1.23.0 requis |

## Installation

### Windows (PowerShell ou CMD)

```powershell
# 1. Naviguer vers le dossier PYTHON
cd PYTHON

# 2. Créer un environnement virtuel (si pas déjà fait)
python -m venv venv

# 3. IMPORTANT: Activer le venv AVANT d'installer les dépendances
.\venv\Scripts\activate

# 4. Vérifier que le venv est actif (vous devez voir "(venv)" au début du prompt)
# Si ce n'est pas le cas, le venv n'est pas activé !

# 5. Installer les dépendances
pip install -r requirements.txt

# 6. Vérifier l'installation
pip list | findstr datacompy
# Doit afficher: datacompy  x.x.x
```

### Linux/Mac

```bash
# 1. Naviguer vers le dossier PYTHON
cd PYTHON

# 2. Créer un environnement virtuel
python -m venv venv

# 3. Activer le venv
source venv/bin/activate

# 4. Installer les dépendances
pip install -r requirements.txt

# 5. Vérifier l'installation
pip list | grep datacompy
```

### Installation avec Conda (Recommandé pour Python 3.13)

Pour Python 3.13, conda-forge peut avoir des packages plus récents :

```bash
# Créer un environnement conda
conda create -n datadiff python=3.13

# Activer l'environnement
conda activate datadiff

# Installer snowflake-snowpark-python depuis conda-forge
conda install -c conda-forge snowflake-snowpark-python

# Installer le reste via pip
pip install datacompy pandas openpyxl xlsxwriter click pyyaml python-dotenv
```

### Installation sans venv (globale)

Si vous préférez installer les packages directement dans votre Python système (sans environnement virtuel) :

```powershell
# 1. Naviguer vers le dossier PYTHON
cd PYTHON

# 2. Installer directement les dépendances
pip install -r requirements.txt

# Ou installer les packages un par un :
pip install "datacompy[snowflake]>=0.14.0"
pip install "snowflake-snowpark-python>=1.23.0"
pip install "pandas>=2.2.0"
pip install "numpy>=2.1.0"
pip install "python-dotenv>=1.0.0"
pip install "openpyxl>=3.1.0"
pip install "xlsxwriter>=3.2.0"
pip install "click>=8.1.0"
pip install "pyyaml>=6.0.1"

# 3. Vérifier l'installation
pip list | findstr datacompy
python -c "from datacompy import SnowflakeCompare; print('Installation OK')"
```

**Avantages installation globale** :
- Pas besoin d'activer un venv à chaque fois
- Plus simple pour les scripts automatisés

**Inconvénients** :
- Peut créer des conflits avec d'autres projets Python
- Difficile à nettoyer si vous voulez désinstaller

### Dépannage

**Erreur `ModuleNotFoundError: No module named 'datacompy'`**

Cette erreur signifie que les dépendances ne sont pas installées dans votre environnement Python actuel.

1. **Vérifiez que le venv est activé** :
   ```powershell
   # Windows - le prompt doit commencer par (venv)
   # Si non, activez-le:
   .\venv\Scripts\activate
   ```

2. **Vérifiez les packages installés** :
   ```powershell
   pip list
   # Doit afficher datacompy, snowflake-snowpark-python, pandas, etc.
   ```

3. **Si seul pip est installé**, réinstallez les dépendances :
   ```powershell
   pip install -r requirements.txt
   ```

4. **Si l'installation échoue** avec des erreurs de compilation, installez les outils de build :
   ```powershell
   # Windows: installer Visual Studio Build Tools
   # ou utiliser les packages binaires:
   pip install --only-binary :all: -r requirements.txt
   ```

**Erreur avec snowflake-snowpark-python sur Python 3.13**

Si pip ne trouve pas de version compatible de snowflake-snowpark-python :

```bash
# Option 1: Utiliser conda-forge (recommandé)
conda install -c conda-forge snowflake-snowpark-python

# Option 2: Installer depuis la source (si disponible)
pip install --no-binary snowflake-snowpark-python snowflake-snowpark-python

# Option 3: Vérifier les versions disponibles
pip index versions snowflake-snowpark-python
```

## Configuration

1. Créer le fichier `.env` à partir du template:

```bash
cp .env.template .env
```

2. Configurer les variables:

```env
SNOWFLAKE_ACCOUNT=your_account.eu-west-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE
SNOWFLAKE_DATABASE=TEAM_DB
SNOWFLAKE_SCHEMA=EXTERNAL
```

Pour SSO (Single Sign-On):
```env
SNOWFLAKE_AUTHENTICATOR=externalbrowser
```

## Utilisation Python

### Comparaison rapide

```python
from snowflake_compare import quick_compare

# Avec clé primaire
result = quick_compare(
    "TEAM_DB.EXTERNAL.TABLE1",
    "TEAM_DB.EXTERNAL.TABLE2",
    join_columns="ID"
)
print(result)

# Voir le rapport datacompy complet
print(result.get_datacompy_report())
```

### Utilisation avancée

```python
from snowflake_compare import SnowflakeTableComparer

with SnowflakeTableComparer() as comparer:
    # Comparaison avec options
    result = comparer.compare(
        table1="TEAM_DB.EXTERNAL.SOURCE",
        table2="TEAM_DB.EXTERNAL.TARGET",
        join_columns=["COL1", "COL2"],  # Clé composite
        abs_tol=0.01,                    # Tolérance numérique absolue
        rel_tol=0.001,                   # Tolérance numérique relative
    )

    print(result)

    # Exporter en Excel
    comparer.export_results([result], "rapport", format="excel")
```

### Comparaison sans clé (hash)

```python
# Si pas de join_columns, utilise SHA256 hash de chaque ligne
result = quick_compare(
    "TEAM_DB.EXTERNAL.TABLE1",
    "TEAM_DB.EXTERNAL.TABLE2"
    # join_columns=None => comparaison par hash
)
```

### Comparaison en batch

```python
from snowflake_compare import SnowflakeTableComparer

table_pairs = [
    ("DB.SCHEMA.TABLE1", "DB.SCHEMA.TABLE1_SAS", "ID"),
    ("DB.SCHEMA.TABLE2", "DB.SCHEMA.TABLE2_SAS", ["COL1", "COL2"]),
    ("DB.SCHEMA.TABLE3", "DB.SCHEMA.TABLE3_SAS", None),  # Hash
]

with SnowflakeTableComparer() as comparer:
    results = comparer.compare_batch(table_pairs, abs_tol=0.01)
    comparer.export_results(results, "batch_report", format="excel")
```

## Utilisation CLI

```bash
# Comparaison simple
python run_comparison.py compare TABLE1 TABLE2 --pk ID

# Clé primaire composite
python run_comparison.py compare TABLE1 TABLE2 --pk "COL1,COL2"

# Sans clé primaire (hash)
python run_comparison.py compare TABLE1 TABLE2

# Avec tolérance numérique et export
python run_comparison.py compare TABLE1 TABLE2 --pk ID --tolerance 0.01 --export results -f excel

# Batch depuis fichier YAML
python run_comparison.py batch config.yaml --export rapport --format excel
```

## Rapport datacompy

Le rapport généré par `datacompy.SnowflakeCompare.report()` inclut:

- **DataFrame Summary**: Nombre de colonnes et lignes
- **Column Summary**: Colonnes communes et uniques
- **Row Summary**: Lignes correspondantes et différentes
- **Column Comparison**: Détails des différences par colonne
- **Sample Mismatch Rows**: Échantillon des lignes différentes

```python
result = quick_compare("TABLE1", "TABLE2", "ID")
print(result.get_datacompy_report())
```

## Structure des fichiers

```
PYTHON/
├── config.py              # Configuration Snowflake/Snowpark
├── snowflake_compare.py   # Module principal (datacompy.SnowflakeCompare)
├── run_comparison.py      # CLI
├── examples.py            # Exemples d'utilisation
├── requirements.txt       # Dépendances (datacompy[snowflake])
├── .env.template          # Template de configuration
└── README.md              # Cette documentation
```

## Différences avec l'outil SQL natif

| Aspect | SQL (SP_COMPARE) | Python (datacompy) |
|--------|------------------|-------------------|
| Exécution | Dans Snowflake | Snowpark (côté serveur) |
| Mémoire | Snowflake | Snowflake (via Snowpark) |
| Rapport | ASCII formaté | Rapport datacompy détaillé |
| Export | Limité | Excel/CSV/JSON |
| Tolérance | abs_tol | abs_tol + rel_tol |
| Analyse | Basique | Colonnes + stats détaillées |

## API datacompy.SnowflakeCompare

Le module utilise ces méthodes de datacompy:

```python
comparison = SnowflakeCompare(
    session,                    # Snowpark Session
    "TABLE1",                   # Nom table 1
    "TABLE2",                   # Nom table 2
    join_columns=["ID"],        # Colonnes de jointure
    abs_tol=0.01,              # Tolérance absolue
    rel_tol=0.001,             # Tolérance relative
)

# Méthodes disponibles
comparison.matches()           # True si tables identiques
comparison.report()            # Rapport textuel complet
comparison.df1_unq_rows        # Lignes uniques table 1
comparison.df2_unq_rows        # Lignes uniques table 2
comparison.intersect_rows      # Lignes communes
comparison.df1_unq_columns()   # Colonnes uniques table 1
comparison.df2_unq_columns()   # Colonnes uniques table 2
```

## Ressources

- [datacompy PyPI](https://pypi.org/project/datacompy/)
- [datacompy Documentation](https://capitalone.github.io/datacompy/)
- [Snowpark Python](https://docs.snowflake.com/en/developer-guide/snowpark/python/index)
