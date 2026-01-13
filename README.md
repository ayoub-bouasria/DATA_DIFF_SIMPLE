# DATA DIFF SIMPLE - Outil de comparaison SAS vs Snowflake

## Vue d'ensemble

DATA DIFF SIMPLE est un outil SQL Snowflake natif conçu pour comparer les données entre des exports SAS (CSV) et des tables Snowflake après une migration. L'outil gère automatiquement les tables avec ou sans clé primaire et fournit des rapports détaillés des différences.

## Caractéristiques principales

- **100% Snowflake SQL** : Aucune dépendance externe
- **Gestion intelligente des clés** : Fonctionne avec ou sans clé primaire
- **Comparaison flexible** : Compare toutes les colonnes ou seulement celles spécifiées
- **Tolérance numérique** : Support des différences acceptables pour les valeurs numériques
- **Rapports visuels** : Affichage formaté des résultats de comparaison
- **Traçabilité complète** : Historique des comparaisons et détail des différences

## Architecture

```
DATA_DIFF_SIMPLE/
├── SQL/
│   ├── 00_setup.sql              # Création des schémas et tables
│   ├── 01_procedures.sql         # Procédures stockées
│   ├── 02_config_proyecto0.sql   # Configuration exemple
│   └── 03_usage_examples.sql     # Exemples d'utilisation
└── README.md                     # Cette documentation
```

## Installation

### 1. Exécuter le script de setup

```sql
-- Exécuter 00_setup.sql pour créer la structure
-- Cela créera :
-- - Base de données DATA_DIFF
-- - Schémas CONFIG, STAGING, RESULTS
-- - Tables de configuration et résultats
-- - Stage pour le chargement des CSV
```

### 2. Créer les procédures stockées

```sql
-- Exécuter 01_procedures.sql
-- Cela créera toutes les procédures nécessaires
```

### 3. (Optionnel) Charger la configuration exemple

```sql
-- Exécuter 02_config_proyecto0.sql
-- Pour avoir un exemple de configuration complète
```

## Guide d'utilisation rapide

### Étape 1 : Enregistrer un projet

```sql
CALL SP_REGISTER_PROJECT(
    'MON_PROJET',          -- Identifiant unique
    'Mon Projet Test',     -- Nom descriptif
    'MA_DATABASE',         -- Base de données cible
    'MON_SCHEMA'           -- Schéma cible
);
```

### Étape 2 : Enregistrer les tables à comparer

#### Table avec clé primaire simple
```sql
CALL SP_REGISTER_TABLE(
    'MON_PROJET',
    'MA_TABLE',
    'MA_DATABASE.MON_SCHEMA.MA_TABLE',
    'ID',                              -- Clé primaire
    'COL1,COL2,COL3',                 -- Colonnes à comparer
    'HIGH'                            -- Priorité
);
```

#### Table avec clé primaire composite
```sql
CALL SP_REGISTER_TABLE(
    'MON_PROJET',
    'COMMANDES',
    'MA_DATABASE.MON_SCHEMA.COMMANDES',
    'CLIENT_ID,ORDER_ID',             -- Clés multiples
    'MONTANT,DATE_COMMANDE,STATUT',
    'HIGH',
    0.01                              -- Tolérance numérique
);
```

#### Table SANS clé primaire
```sql
CALL SP_REGISTER_TABLE(
    'MON_PROJET',
    'LOGS',
    'MA_DATABASE.MON_SCHEMA.LOGS',
    NULL,                             -- Pas de clé
    'ALL',                            -- Comparer tout
    'MEDIUM'
);
```

### Étape 3 : Charger les données SAS

```sql
-- 1. Uploader le CSV dans le stage
PUT file://C:/exports/ma_table.csv @DATA_DIFF.STAGING.SAS_EXPORTS;

-- 2. Charger dans la zone de staging
CALL SP_LOAD_SAS_CSV(
    'MON_PROJET',
    'MA_TABLE',
    '@DATA_DIFF.STAGING.SAS_EXPORTS/ma_table.csv'
);
```

### Étape 4 : Lancer la comparaison

```sql
-- Comparer toutes les tables du projet
CALL SP_RUN_COMPARISON('MON_PROJET');
```

### Étape 5 : Consulter les résultats

```sql
-- Afficher le rapport formaté
CALL SP_SHOW_REPORT();
```

Exemple de rapport :
```
╔═══════════════════════════════════════════════════════════════════════════╗
║                 RAPPORT DATA DIFF - MON_PROJET                             ║
║                 Date: 2025-01-09 14:30:00                                  ║
╠═══════════════════════════════════════════════════════════════════════════╣
║  RÉSUMÉ: 3 tables | ✓ 2 identiques | ✗ 1 différentes                      ║
╠═══════════════════════════════════════════════════════════════════════════╣
║  TABLE                    │ SAS     │ SNOW    │ MATCH  │ STATUS            ║
║  ─────────────────────────┼─────────┼─────────┼────────┼─────────────────  ║
║  MA_TABLE                 │   1,234 │   1,234 │ 100.0% │ ✓ IDENTICAL       ║
║  COMMANDES                │   5,678 │   5,679 │  99.9% │ ✗ DIFFERENT       ║
║  LOGS                     │     456 │     456 │ 100.0% │ ✓ IDENTICAL       ║
╠═══════════════════════════════════════════════════════════════════════════╣
║  STATUT FINAL: ✗ DIFFÉRENCES TROUVÉES                                      ║
╚═══════════════════════════════════════════════════════════════════════════╝
```

## Analyse des différences

### Types de différences

1. **ONLY_SAS** : Lignes présentes uniquement dans l'export SAS
2. **ONLY_SNOW** : Lignes présentes uniquement dans Snowflake
3. **VALUE_DIFF** : Lignes avec des valeurs différentes (tables avec PK uniquement)

### Requêtes d'analyse

```sql
-- Résumé des différences par type
SELECT 
    TABLE_NAME,
    DIFF_TYPE,
    COUNT(*) AS NB_DIFFERENCES
FROM RESULTS.COMPARISON_DIFF_DETAILS
WHERE RUN_ID = 'VOTRE_RUN_ID'
GROUP BY TABLE_NAME, DIFF_TYPE;

-- Voir les lignes manquantes dans Snowflake
SELECT * FROM RESULTS.COMPARISON_DIFF_DETAILS
WHERE RUN_ID = 'VOTRE_RUN_ID'
  AND DIFF_TYPE = 'ONLY_SAS'
  AND TABLE_NAME = 'MA_TABLE';

-- Exporter toutes les différences
CALL SP_EXPORT_DIFF_DETAILS('VOTRE_RUN_ID');
```

## Gestion des cas particuliers

### Valeurs NULL

L'outil normalise automatiquement les représentations NULL :
- SAS : `''`, `'.'`, `'NULL'` → NULL Snowflake
- Comparaison NULL-safe intégrée

### Tolérance numérique

Pour les colonnes numériques avec précision différente :
```sql
CALL SP_REGISTER_TABLE(
    'FINANCE',
    'TRANSACTIONS',
    'FINANCE_DB.PUBLIC.TRANSACTIONS',
    'ID',
    'MONTANT,TAXES',
    'HIGH',
    0.01  -- Tolérance de ±0.01
);
```

### Tables sans clé primaire

Pour les tables sans clé primaire, l'outil :
1. Calcule un hash SHA-256 de chaque ligne complète
2. Compare les hashes pour identifier les lignes identiques
3. Les différences sont reportées au niveau ligne (pas colonne par colonne)

## Maintenance

### Nettoyer les anciennes données

```sql
-- Supprimer les staging de plus de 7 jours
DELETE FROM STAGING.SAS_STAGING_DATA
WHERE LOADED_AT < DATEADD('day', -7, CURRENT_TIMESTAMP());

-- Supprimer les résultats de plus d'un mois
DELETE FROM RESULTS.COMPARISON_DIFF_DETAILS
WHERE CREATED_AT < DATEADD('month', -1, CURRENT_TIMESTAMP());
```

### Désactiver temporairement une table

```sql
UPDATE CONFIG.CONFIG_TABLES
SET IS_ACTIVE = FALSE
WHERE PROJECT_ID = 'MON_PROJET' 
  AND TABLE_NAME = 'TABLE_PROBLEMATIQUE';
```

## Monitoring et performance

### Temps d'exécution par table

```sql
SELECT 
    TABLE_NAME,
    AVG(EXECUTION_TIME_SEC) AS AVG_TIME,
    MAX(EXECUTION_TIME_SEC) AS MAX_TIME,
    AVG(SAS_ROW_COUNT) AS AVG_ROWS
FROM RESULTS.COMPARISON_RESULTS
WHERE PROJECT_ID = 'MON_PROJET'
GROUP BY TABLE_NAME
ORDER BY AVG_TIME DESC;
```

### Évolution du taux de correspondance

```sql
SELECT 
    DATE(RUN_TIMESTAMP) AS DATE,
    TABLE_NAME,
    AVG(MATCH_PERCENTAGE) AS AVG_MATCH
FROM RESULTS.COMPARISON_RESULTS
WHERE PROJECT_ID = 'MON_PROJET'
GROUP BY DATE, TABLE_NAME
ORDER BY DATE DESC;
```

## Limitations

- Les fichiers CSV doivent être UTF-8 avec header
- La comparaison VALUE_DIFF n'est disponible que pour les tables avec clé primaire
- Les performances peuvent être impactées pour les très grandes tables (>10M lignes)

## Support

Pour toute question ou problème :
1. Consulter les logs d'exécution dans `RESULTS.EXECUTION_LOGS`
2. Vérifier la configuration dans `CONFIG.CONFIG_TABLES`
3. S'assurer que les données sont bien chargées dans `STAGING.SAS_STAGING_DATA`