-- ============================================================================
-- DATA DIFF TOOL - EXEMPLES D'UTILISATION
-- ============================================================================
-- Description: Exemples d'utilisation de l'outil Data Diff
-- Version: 1.0
-- Date: 2025-01-09
-- ============================================================================

USE DATABASE DATA_DIFF;

-- ============================================================================
-- ÉTAPE 1: CONFIGURATION INITIALE
-- ============================================================================

-- 1.1 Enregistrer un nouveau projet
CALL SP_REGISTER_PROJECT(
    'MON_PROJET',           -- project_id
    'Mon Projet Test',      -- project_name
    'MA_DATABASE',          -- target_database
    'MON_SCHEMA',           -- target_schema
    'Description de mon projet de migration'
);

-- 1.2 Enregistrer des tables avec clé primaire
CALL SP_REGISTER_TABLE(
    'MON_PROJET',                          -- project_id
    'MA_TABLE_1',                          -- table_name
    'MA_DATABASE.MON_SCHEMA.MA_TABLE_1',   -- snow_table
    'ID',                                  -- primary_key_cols (simple)
    'COL1,COL2,COL3',                      -- compare_cols
    'HIGH',                                -- priority
    0                                      -- numeric_tolerance
);

-- 1.3 Enregistrer des tables avec clé primaire composite
CALL SP_REGISTER_TABLE(
    'MON_PROJET',
    'MA_TABLE_2',
    'MA_DATABASE.MON_SCHEMA.MA_TABLE_2',
    'ID_CLIENT,ID_PRODUIT',                -- Clé composite
    'QUANTITE,PRIX,DATE_COMMANDE',
    'HIGH',
    0.01                                   -- Tolérance de 0.01 pour les numériques
);

-- 1.4 Enregistrer des tables SANS clé primaire
CALL SP_REGISTER_TABLE(
    'MON_PROJET',
    'MA_TABLE_3',
    'MA_DATABASE.MON_SCHEMA.MA_TABLE_3',
    NULL,                                  -- Pas de clé primaire
    'ALL',                                 -- Comparer toutes les colonnes
    'MEDIUM',
    0
);

-- ============================================================================
-- ÉTAPE 2: CHARGER LES DONNÉES SAS
-- ============================================================================

-- 2.1 Uploader d'abord les fichiers CSV dans le stage
-- Via l'interface Snowflake ou commande PUT :
-- PUT file://C:/exports_sas/ma_table_1.csv @DATA_DIFF.STAGING.SAS_EXPORTS;

-- 2.2 Charger les données CSV dans la zone de staging
CALL SP_LOAD_SAS_CSV(
    'MON_PROJET',
    'MA_TABLE_1',
    '@DATA_DIFF.STAGING.SAS_EXPORTS/ma_table_1.csv'
);

CALL SP_LOAD_SAS_CSV(
    'MON_PROJET',
    'MA_TABLE_2',
    '@DATA_DIFF.STAGING.SAS_EXPORTS/ma_table_2.csv'
);

-- ============================================================================
-- ÉTAPE 3: LANCER LA COMPARAISON
-- ============================================================================

-- 3.1 Lancer une comparaison complète
CALL SP_RUN_COMPARISON('MON_PROJET');

-- 3.2 Voir le rapport formaté
CALL SP_SHOW_REPORT();

-- Ou pour un run spécifique :
-- CALL SP_SHOW_REPORT('UUID-du-run');

-- ============================================================================
-- ÉTAPE 4: ANALYSER LES RÉSULTATS
-- ============================================================================

-- 4.1 Vue d'ensemble des runs
SELECT * FROM RESULTS.V_COMPARISON_SUMMARY
WHERE PROJECT_ID = 'MON_PROJET'
ORDER BY START_TIME DESC;

-- 4.2 Détails par table du dernier run
SELECT * FROM RESULTS.V_TABLE_RESULTS
WHERE PROJECT_ID = 'MON_PROJET'
  AND RUN_ID = (SELECT RUN_ID FROM RESULTS.COMPARISON_RUNS 
                WHERE PROJECT_ID = 'MON_PROJET' 
                ORDER BY START_TIME DESC LIMIT 1);

-- 4.3 Voir les différences détaillées
SELECT 
    TABLE_NAME,
    DIFF_TYPE,
    COUNT(*) AS NB_DIFFERENCES
FROM RESULTS.COMPARISON_DIFF_DETAILS
WHERE RUN_ID = (SELECT RUN_ID FROM RESULTS.COMPARISON_RUNS 
                WHERE PROJECT_ID = 'MON_PROJET' 
                ORDER BY START_TIME DESC LIMIT 1)
GROUP BY TABLE_NAME, DIFF_TYPE
ORDER BY TABLE_NAME, DIFF_TYPE;

-- 4.4 Examiner des différences spécifiques
-- Lignes présentes uniquement dans SAS
SELECT 
    TABLE_NAME,
    PRIMARY_KEY_VALUE,
    SAS_ROW_DATA
FROM RESULTS.COMPARISON_DIFF_DETAILS
WHERE RUN_ID = (SELECT RUN_ID FROM RESULTS.COMPARISON_RUNS 
                WHERE PROJECT_ID = 'MON_PROJET' 
                ORDER BY START_TIME DESC LIMIT 1)
  AND DIFF_TYPE = 'ONLY_SAS'
  AND TABLE_NAME = 'MA_TABLE_1'
LIMIT 10;

-- 4.5 Exporter les différences pour analyse
CALL SP_EXPORT_DIFF_DETAILS(
    'RUN_ID_ICI',           -- Remplacer par l'ID du run
    'MA_TABLE_1',           -- Optionnel: filtrer par table
    'VALUE_DIFF'            -- Optionnel: filtrer par type de diff
);

-- ============================================================================
-- CAS D'USAGE SPÉCIFIQUES
-- ============================================================================

-- CAS 1: Comparer seulement certaines colonnes numériques avec tolérance
CALL SP_REGISTER_TABLE(
    'FINANCE_PROJECT',
    'TRANSACTIONS',
    'FINANCE_DB.PUBLIC.TRANSACTIONS',
    'TRANSACTION_ID',
    'AMOUNT,TAX_AMOUNT,TOTAL_AMOUNT',      -- Colonnes numériques uniquement
    'HIGH',
    0.001                                  -- Tolérance de 0.001
);

-- CAS 2: Table avec beaucoup de colonnes - comparer tout sauf certaines
-- (Nécessite de lister explicitement les colonnes à comparer)
CALL SP_REGISTER_TABLE(
    'BIG_PROJECT',
    'LARGE_TABLE',
    'BIG_DB.PUBLIC.LARGE_TABLE',
    'ID',
    'COL1,COL2,COL3,COL4,COL5,COL6,COL7,COL8,COL9,COL10',  -- Exclure COL11, COL12...
    'MEDIUM',
    0
);

-- CAS 3: Gérer les tables temporaires ou datées
CALL SP_REGISTER_TABLE(
    'DAILY_PROJECT',
    'SALES_20250109',                              -- Table du jour
    'DAILY_DB.PUBLIC.SALES_CURRENT',               -- Table courante dans Snowflake
    'SALE_ID',
    'ALL',
    'HIGH',
    0
);

-- ============================================================================
-- MAINTENANCE ET NETTOYAGE
-- ============================================================================

-- Désactiver temporairement une table
UPDATE CONFIG.CONFIG_TABLES
SET IS_ACTIVE = FALSE
WHERE PROJECT_ID = 'MON_PROJET' AND TABLE_NAME = 'MA_TABLE_PROBLEMATIQUE';

-- Nettoyer les anciennes données de staging
DELETE FROM STAGING.SAS_STAGING_DATA
WHERE LOADED_AT < DATEADD('day', -7, CURRENT_TIMESTAMP());

-- Nettoyer les anciens résultats
DELETE FROM RESULTS.COMPARISON_DIFF_DETAILS
WHERE RUN_ID IN (
    SELECT RUN_ID 
    FROM RESULTS.COMPARISON_RUNS
    WHERE START_TIME < DATEADD('month', -1, CURRENT_TIMESTAMP())
);

-- ============================================================================
-- REQUÊTES DE MONITORING
-- ============================================================================

-- Performance des comparaisons par table
SELECT 
    TABLE_NAME,
    AVG(EXECUTION_TIME_SEC) AS AVG_TIME_SEC,
    MAX(EXECUTION_TIME_SEC) AS MAX_TIME_SEC,
    AVG(SAS_ROW_COUNT) AS AVG_ROWS,
    COUNT(*) AS NB_RUNS
FROM RESULTS.COMPARISON_RESULTS
WHERE PROJECT_ID = 'MON_PROJET'
GROUP BY TABLE_NAME
ORDER BY AVG_TIME_SEC DESC;

-- Évolution du taux de correspondance
SELECT 
    DATE(RUN_TIMESTAMP) AS RUN_DATE,
    TABLE_NAME,
    AVG(MATCH_PERCENTAGE) AS AVG_MATCH_PCT
FROM RESULTS.COMPARISON_RESULTS
WHERE PROJECT_ID = 'MON_PROJET'
GROUP BY DATE(RUN_TIMESTAMP), TABLE_NAME
ORDER BY RUN_DATE DESC, TABLE_NAME;

-- Tables les plus problématiques
SELECT 
    TABLE_NAME,
    SUM(CASE WHEN STATUS = 'DIFFERENT' THEN 1 ELSE 0 END) AS NB_DIFF,
    SUM(CASE WHEN STATUS = 'ERROR' THEN 1 ELSE 0 END) AS NB_ERRORS,
    COUNT(*) AS TOTAL_RUNS,
    ROUND(100.0 * SUM(CASE WHEN STATUS = 'IDENTICAL' THEN 1 ELSE 0 END) / COUNT(*), 2) AS SUCCESS_RATE
FROM RESULTS.COMPARISON_RESULTS
WHERE PROJECT_ID = 'MON_PROJET'
GROUP BY TABLE_NAME
HAVING SUCCESS_RATE < 100
ORDER BY SUCCESS_RATE ASC;

-- ============================================================================
-- TROUBLESHOOTING
-- ============================================================================

-- Voir les logs d'exécution pour débugger
SELECT * FROM RESULTS.EXECUTION_LOGS
WHERE RUN_ID = 'RUN_ID_ICI'
ORDER BY LOG_TIMESTAMP;

-- Vérifier les données staging chargées
SELECT 
    TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MIN(LOADED_AT) AS FIRST_LOAD,
    MAX(LOADED_AT) AS LAST_LOAD
FROM STAGING.SAS_STAGING_DATA
WHERE PROJECT_ID = 'MON_PROJET'
GROUP BY TABLE_NAME;

-- Exemple de données staging (premières lignes)
SELECT * FROM STAGING.SAS_STAGING_DATA
WHERE PROJECT_ID = 'MON_PROJET' 
  AND TABLE_NAME = 'MA_TABLE_1'
LIMIT 5;