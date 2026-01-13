-- ============================================================================
-- DATA DIFF TOOL - EXEMPLES D'UTILISATION SIMPLE
-- ============================================================================
-- Description: Exemples d'utilisation avec la syntaxe simplifiée SP_COMPARE
-- Version: 2.0
-- Date: 2025-01-09
-- ============================================================================

USE DATABASE TEAM_DB;
USE SCHEMA EXTERNAL;

-- ============================================================================
-- EXEMPLES BASIQUES
-- ============================================================================

-- Exemple 1: Comparaison simple sans clé primaire
-- Les tables seront comparées ligne par ligne via hash
CALL SP_COMPARE(
    'HISTORICO_REMESAS',           -- Table 1
    'HISTORICO_REMESAS_NEW'        -- Table 2
);

-- Exemple 2: Comparaison avec clé primaire simple
CALL SP_COMPARE(
    'OUTPUT_ENVIO',                -- Table 1
    'OUTPUT_ENVIO_V2',             -- Table 2
    'CODNUM'                       -- Clé primaire
);

-- Exemple 3: Comparaison avec clé primaire composite
CALL SP_COMPARE(
    'OUTPUT_DOMICILIACIONES_NO_NEGATIVAS',     -- Table 1
    'OUTPUT_DOMICILIACIONES_NO_NEGATIVAS_NEW', -- Table 2
    'CONTRATO,CODNUM'                          -- Clés multiples (séparées par virgule)
);

-- Exemple 4: Sans afficher le rapport automatiquement
CALL SP_COMPARE(
    'EXCLUSIONES_REMESAS',
    'EXCLUSIONES_REMESAS_TEST',
    NULL,                          -- Pas de clé primaire
    'ALL',                         -- Comparer toutes les colonnes
    0,                             -- Pas de tolérance numérique
    TRUE,                          -- Case sensitive
    FALSE                          -- Ne pas afficher le rapport
);

-- ============================================================================
-- EXEMPLES AVANCÉS
-- ============================================================================

-- Exemple 5: Comparaison avec tolérance numérique
CALL SP_COMPARE(
    'REMESA_BBVA',
    'REMESA_BBVA_MIGRATED',
    NULL,                          -- Pas de clé primaire
    'ALL',                         -- Toutes les colonnes
    0.01                           -- Tolérance de 0.01 pour les numériques
);

-- Exemple 6: Comparaison non sensible à la casse
CALL SP_COMPARE(
    'FICHEROS_E002',
    'FICHEROS_E002_UPPER',
    'FICHERO',                     -- Clé primaire
    'ALL',                         -- Toutes les colonnes
    0,                             -- Pas de tolérance
    FALSE                          -- Case insensitive
);

-- Exemple 7: Comparaison de colonnes spécifiques uniquement
CALL SP_COMPARE(
    'EVOLUCION_RP',
    'EVOLUCION_RP_V2',
    'FICHERO_SIBS',                -- Clé primaire
    'RP1,RP5,RP10,RP15,RP20'       -- Colonnes spécifiques à comparer
);

-- ============================================================================
-- CONSULTATION DES RÉSULTATS
-- ============================================================================

-- Voir les comparaisons récentes
SELECT * FROM V_RECENT_COMPARISONS;

-- Afficher le rapport de la dernière comparaison
CALL SP_SHOW_COMPARISON_REPORT();

-- Afficher le rapport d'une comparaison spécifique
CALL SP_SHOW_COMPARISON_REPORT('COMPARISON_ID_ICI');

-- Voir le résumé des différences
SELECT * FROM V_DIFF_SUMMARY
WHERE COMPARISON_ID = (
    SELECT COMPARISON_ID 
    FROM DIFF_RESULTS 
    ORDER BY COMPARISON_TIME DESC 
    LIMIT 1
);

-- Voir les détails des différences (limité à 50 lignes)
CALL SP_SHOW_DIFF_DETAILS(
    (SELECT COMPARISON_ID FROM DIFF_RESULTS ORDER BY COMPARISON_TIME DESC LIMIT 1),
    NULL,    -- Tous les types
    50       -- Limite
);

-- Voir uniquement les lignes qui existent dans une seule table
CALL SP_SHOW_DIFF_DETAILS(
    'COMPARISON_ID_ICI',
    'ONLY_TABLE1',    -- ou 'ONLY_TABLE2'
    100
);

-- ============================================================================
-- REQUÊTES D'ANALYSE
-- ============================================================================

-- Statistiques des comparaisons par tables
SELECT 
    TABLE1,
    TABLE2,
    COUNT(*) AS NB_COMPARISONS,
    AVG(MATCH_PERCENTAGE) AS AVG_MATCH,
    SUM(CASE WHEN IS_IDENTICAL THEN 1 ELSE 0 END) AS NB_IDENTICAL,
    SUM(CASE WHEN NOT IS_IDENTICAL THEN 1 ELSE 0 END) AS NB_DIFFERENT
FROM DIFF_RESULTS
GROUP BY TABLE1, TABLE2
ORDER BY NB_COMPARISONS DESC;

-- Tables avec le plus de différences
SELECT 
    TABLE1,
    TABLE2,
    COMPARISON_TIME,
    TABLE1_ROW_COUNT,
    TABLE2_ROW_COUNT,
    ONLY_IN_TABLE1,
    ONLY_IN_TABLE2,
    DIFF_VALUES,
    MATCH_PERCENTAGE
FROM DIFF_RESULTS
WHERE NOT IS_IDENTICAL
ORDER BY MATCH_PERCENTAGE ASC
LIMIT 10;

-- Évolution du taux de correspondance pour une paire de tables
SELECT 
    DATE(COMPARISON_TIME) AS COMPARISON_DATE,
    TABLE1,
    TABLE2,
    AVG(MATCH_PERCENTAGE) AS AVG_MATCH_PCT,
    COUNT(*) AS NB_RUNS
FROM DIFF_RESULTS
WHERE TABLE1 = 'HISTORICO_REMESAS'
  AND TABLE2 = 'HISTORICO_REMESAS_NEW'
GROUP BY DATE(COMPARISON_TIME), TABLE1, TABLE2
ORDER BY COMPARISON_DATE DESC;

-- ============================================================================
-- CAS D'USAGE SPÉCIFIQUES
-- ============================================================================

-- Cas 1: Comparer des tables dans différents schémas
CALL SP_COMPARE(
    'TEAM_DB.EXTERNAL.HISTORICO_REMESAS',
    'TEAM_DB.STAGING.HISTORICO_REMESAS',
    'CODNUM'
);

-- Cas 2: Comparer avec une table temporaire
CREATE OR REPLACE TEMPORARY TABLE TEMP_DATA AS 
SELECT * FROM HISTORICO_REMESAS WHERE FECHA >= '2024-01-01';

CALL SP_COMPARE(
    'TEMP_DATA',
    'HISTORICO_REMESAS',
    'CODNUM'
);

-- Cas 3: Comparaison après transformation
CREATE OR REPLACE TEMPORARY TABLE TRANSFORMED_DATA AS
SELECT 
    CODNUM,
    UPPER(CONTRATO) AS CONTRATO,
    FECHA,
    ROUND(IMPORTE, 2) AS IMPORTE
FROM HISTORICO_REMESAS;

CALL SP_COMPARE(
    'HISTORICO_REMESAS',
    'TRANSFORMED_DATA',
    'CODNUM',
    'CONTRATO,FECHA,IMPORTE'
);

-- ============================================================================
-- MAINTENANCE
-- ============================================================================

-- Nettoyer les comparaisons de plus de 30 jours
CALL SP_CLEANUP_OLD_COMPARISONS(30);

-- Nettoyer les comparaisons de plus de 7 jours
CALL SP_CLEANUP_OLD_COMPARISONS(7);

-- Voir l'espace utilisé
SELECT 
    'DIFF_RESULTS' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT,
    MIN(COMPARISON_TIME) AS OLDEST_RECORD,
    MAX(COMPARISON_TIME) AS NEWEST_RECORD
FROM DIFF_RESULTS
UNION ALL
SELECT 
    'DIFF_DETAILS',
    COUNT(*),
    MIN(CREATED_AT),
    MAX(CREATED_AT)
FROM (
    SELECT D.*, R.COMPARISON_TIME AS CREATED_AT
    FROM DIFF_DETAILS D
    JOIN DIFF_RESULTS R ON D.COMPARISON_ID = R.COMPARISON_ID
);

-- ============================================================================
-- EXEMPLES POUR PROYECTO 0
-- ============================================================================

-- Toutes les tables de PROYECTO 0 avec leurs clés primaires appropriées

-- 1. HISTORICO_REMESAS
CALL SP_COMPARE('HISTORICO_REMESAS', 'HISTORICO_REMESAS_SAS', 'CODNUM');

-- 2. OUTPUT_ENVIO
CALL SP_COMPARE('OUTPUT_ENVIO', 'OUTPUT_ENVIO_SAS', 'CODNUM');

-- 3. OUTPUT_ENVIO_BBVA
CALL SP_COMPARE('OUTPUT_ENVIO_BBVA', 'OUTPUT_ENVIO_BBVA_SAS', 'CODNUM');

-- 4. Tables sans clé primaire
CALL SP_COMPARE('EXCLUSIONES_REMESAS', 'EXCLUSIONES_REMESAS_SAS');
CALL SP_COMPARE('REMESA_BBVA', 'REMESA_BBVA_SAS');

-- 5. Table avec clé composite
CALL SP_COMPARE(
    'OUTPUT_DOMICILIACIONES_NO_NEGATIVAS', 
    'OUTPUT_DOMICILIACIONES_NO_NEGATIVAS_SAS', 
    'CONTRATO,CODNUM'
);

-- Script pour comparer toutes les tables d'un coup
DECLARE
    v_tables ARRAY := [
        ['HISTORICO_REMESAS', 'CODNUM'],
        ['OUTPUT_ENVIO', 'CODNUM'],
        ['OUTPUT_ENVIO_BBVA', 'CODNUM'],
        ['OUTPUT_IBAN_NO_OK', 'CODNUM'],
        ['OUTPUT_NOENVIO', 'CODNUM'],
        ['OUTPUT_KO_EXCLUSIONES', 'CODNUM'],
        ['OUTPUT_DOMICILIACIONES_NO_NEGATIVAS', 'CONTRATO,CODNUM'],
        ['FICHEROS_E002', 'FICHERO'],
        ['FICHEROS_REMESAS', 'FICHERO'],
        ['FICHEROS_DEVOLUCIONES', 'FICHERO'],
        ['EVOLUCION_RP', 'FICHERO_SIBS'],
        ['EXCLUSIONES_REMESAS', NULL],
        ['REMESA_BBVA', NULL]
    ];
    v_table VARCHAR;
    v_pk VARCHAR;
    i INTEGER;
BEGIN
    FOR i IN 0 TO ARRAY_SIZE(v_tables) - 1 DO
        v_table := v_tables[i][0];
        v_pk := v_tables[i][1];
        
        CALL SP_COMPARE(
            v_table,
            v_table || '_SAS',
            v_pk,
            'ALL',
            0.01,
            TRUE,
            FALSE  -- Pas de rapport individuel
        );
    END LOOP;
    
    -- Afficher un rapport consolidé
    SELECT 
        TABLE1,
        TABLE2,
        IS_IDENTICAL,
        MATCH_PERCENTAGE,
        TABLE1_ROW_COUNT,
        TABLE2_ROW_COUNT
    FROM DIFF_RESULTS
    WHERE COMPARISON_TIME >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
    ORDER BY TABLE1;
END;