-- ============================================================================
-- DATA DIFF TOOL - EXÉCUTION DES TESTS
-- ============================================================================
-- Description: Script d'exécution des tests de l'outil Data Diff
-- Prérequis: Exécuter d'abord 00_test_setup.sql puis main.sql
-- Version: 1.0
-- Date: 2025-01-13
-- ============================================================================

USE DATABASE TEAM_DB;
USE SCHEMA EXTERNAL;

-- ============================================================================
-- NETTOYAGE PRÉALABLE
-- ============================================================================

-- Supprimer les résultats de tests précédents
DELETE FROM DIFF_DETAILS WHERE COMPARISON_ID IN (
    SELECT COMPARISON_ID FROM DIFF_RESULTS
    WHERE TABLE1 LIKE 'TEST_TABLE%' OR TABLE2 LIKE 'TEST_TABLE%'
);

DELETE FROM DIFF_RESULTS
WHERE TABLE1 LIKE 'TEST_TABLE%' OR TABLE2 LIKE 'TEST_TABLE%';

SELECT 'Nettoyage des anciens résultats de test terminé' AS STATUS;

-- ============================================================================
-- TEST 1: Tables avec clé primaire simple - Différences attendues
-- ============================================================================
-- Attendu:
--   - 2 lignes identiques (ID 1 et 4)
--   - 1 ligne uniquement dans TABLE_A (ID 5)
--   - 1 ligne uniquement dans TABLE_B (ID 6)
--   - 2 lignes avec différences de valeurs (ID 2 et 3)
-- ============================================================================

SELECT '=== TEST 1: Clé primaire simple avec différences ===' AS TEST_NAME;

CALL SP_COMPARE(
    'TEST_TABLE_A',
    'TEST_TABLE_B',
    'ID',
    'ALL',
    0,
    TRUE,
    FALSE
);

-- Vérification
SELECT
    'TEST 1' AS TEST,
    CASE
        WHEN IS_IDENTICAL = FALSE
             AND ONLY_IN_TABLE1 = 1
             AND ONLY_IN_TABLE2 = 1
             AND DIFF_VALUES = 2
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RESULT,
    'Tables différentes attendues' AS EXPECTED,
    'ONLY_TABLE1=' || ONLY_IN_TABLE1 || ', ONLY_TABLE2=' || ONLY_IN_TABLE2 || ', DIFF=' || DIFF_VALUES AS ACTUAL
FROM DIFF_RESULTS
WHERE TABLE1 = 'TEST_TABLE_A' AND TABLE2 = 'TEST_TABLE_B'
ORDER BY COMPARISON_TIME DESC
LIMIT 1;

-- ============================================================================
-- TEST 2: Tables identiques
-- ============================================================================
-- Attendu: 100% de correspondance, IS_IDENTICAL = TRUE
-- ============================================================================

SELECT '=== TEST 2: Tables identiques ===' AS TEST_NAME;

CALL SP_COMPARE(
    'TEST_TABLE_IDENTICAL_A',
    'TEST_TABLE_IDENTICAL_B',
    'ID',
    'ALL',
    0,
    TRUE,
    FALSE
);

-- Vérification
SELECT
    'TEST 2' AS TEST,
    CASE
        WHEN IS_IDENTICAL = TRUE
             AND MATCH_PERCENTAGE = 100.00
             AND ONLY_IN_TABLE1 = 0
             AND ONLY_IN_TABLE2 = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RESULT,
    'Tables identiques (100%)' AS EXPECTED,
    'MATCH=' || MATCH_PERCENTAGE || '%, IDENTICAL=' || IS_IDENTICAL AS ACTUAL
FROM DIFF_RESULTS
WHERE TABLE1 = 'TEST_TABLE_IDENTICAL_A'
ORDER BY COMPARISON_TIME DESC
LIMIT 1;

-- ============================================================================
-- TEST 3: Tables sans clé primaire (hash)
-- ============================================================================
-- Attendu: Comparaison par hash, différences détectées
-- ============================================================================

SELECT '=== TEST 3: Tables sans clé primaire (hash) ===' AS TEST_NAME;

CALL SP_COMPARE(
    'TEST_TABLE_NO_PK_A',
    'TEST_TABLE_NO_PK_B',
    NULL,  -- Pas de clé primaire
    'ALL',
    0,
    TRUE,
    FALSE
);

-- Vérification
SELECT
    'TEST 3' AS TEST,
    CASE
        WHEN HAS_PRIMARY_KEY = FALSE
             AND IS_IDENTICAL = FALSE
             AND (ONLY_IN_TABLE1 > 0 OR ONLY_IN_TABLE2 > 0)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RESULT,
    'Comparaison hash avec différences' AS EXPECTED,
    'HAS_PK=' || HAS_PRIMARY_KEY || ', ONLY_T1=' || ONLY_IN_TABLE1 || ', ONLY_T2=' || ONLY_IN_TABLE2 AS ACTUAL
FROM DIFF_RESULTS
WHERE TABLE1 = 'TEST_TABLE_NO_PK_A'
ORDER BY COMPARISON_TIME DESC
LIMIT 1;

-- ============================================================================
-- TEST 4: Clé primaire composite
-- ============================================================================
-- Attendu: Différences détectées avec clé composite
-- ============================================================================

SELECT '=== TEST 4: Clé primaire composite ===' AS TEST_NAME;

CALL SP_COMPARE(
    'TEST_TABLE_COMPOSITE_PK_A',
    'TEST_TABLE_COMPOSITE_PK_B',
    'CLIENT_ID,ORDER_ID',  -- Clé composite
    'ALL',
    0,
    TRUE,
    FALSE
);

-- Vérification
SELECT
    'TEST 4' AS TEST,
    CASE
        WHEN HAS_PRIMARY_KEY = TRUE
             AND IS_IDENTICAL = FALSE
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RESULT,
    'Clé composite avec différences' AS EXPECTED,
    'PK=' || PRIMARY_KEY_COLS || ', IDENTICAL=' || IS_IDENTICAL AS ACTUAL
FROM DIFF_RESULTS
WHERE TABLE1 = 'TEST_TABLE_COMPOSITE_PK_A'
ORDER BY COMPARISON_TIME DESC
LIMIT 1;

-- ============================================================================
-- TEST 5: Tolérance numérique
-- ============================================================================
-- Attendu avec tolérance 0: différences détectées
-- Attendu avec tolérance 0.001: moins de différences
-- ============================================================================

SELECT '=== TEST 5: Tolérance numérique ===' AS TEST_NAME;

-- Sans tolérance
CALL SP_COMPARE(
    'TEST_TABLE_NUMERIC_A',
    'TEST_TABLE_NUMERIC_B',
    'ID',
    'ALL',
    0,      -- Pas de tolérance
    TRUE,
    FALSE
);

SELECT
    'TEST 5a' AS TEST,
    CASE
        WHEN DIFF_VALUES > 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RESULT,
    'Différences sans tolérance' AS EXPECTED,
    'DIFF_VALUES=' || DIFF_VALUES AS ACTUAL
FROM DIFF_RESULTS
WHERE TABLE1 = 'TEST_TABLE_NUMERIC_A'
ORDER BY COMPARISON_TIME DESC
LIMIT 1;

-- ============================================================================
-- TEST 6: Table vide vs table avec données
-- ============================================================================

SELECT '=== TEST 6: Table vide vs table avec données ===' AS TEST_NAME;

CALL SP_COMPARE(
    'TEST_TABLE_EMPTY',
    'TEST_TABLE_WITH_DATA',
    'ID',
    'ALL',
    0,
    TRUE,
    FALSE
);

-- Vérification
SELECT
    'TEST 6' AS TEST,
    CASE
        WHEN TABLE1_ROW_COUNT = 0
             AND TABLE2_ROW_COUNT = 1
             AND IS_IDENTICAL = FALSE
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RESULT,
    'Table vide vs 1 ligne' AS EXPECTED,
    'T1_COUNT=' || TABLE1_ROW_COUNT || ', T2_COUNT=' || TABLE2_ROW_COUNT AS ACTUAL
FROM DIFF_RESULTS
WHERE TABLE1 = 'TEST_TABLE_EMPTY'
ORDER BY COMPARISON_TIME DESC
LIMIT 1;

-- ============================================================================
-- TEST 7: Tables avec valeurs NULL
-- ============================================================================

SELECT '=== TEST 7: Tables avec valeurs NULL ===' AS TEST_NAME;

CALL SP_COMPARE(
    'TEST_TABLE_NULLS_A',
    'TEST_TABLE_NULLS_B',
    'ID',
    'ALL',
    0,
    TRUE,
    FALSE
);

-- Vérification - Les tables devraient être identiques malgré les NULLs
SELECT
    'TEST 7' AS TEST,
    CASE
        WHEN IS_IDENTICAL = TRUE
        THEN 'PASS'
        ELSE 'FAIL'
    END AS RESULT,
    'Tables avec NULL identiques' AS EXPECTED,
    'IDENTICAL=' || IS_IDENTICAL || ', DIFF=' || DIFF_VALUES AS ACTUAL
FROM DIFF_RESULTS
WHERE TABLE1 = 'TEST_TABLE_NULLS_A'
ORDER BY COMPARISON_TIME DESC
LIMIT 1;

-- ============================================================================
-- RAPPORT FINAL DES TESTS
-- ============================================================================

SELECT '=== RAPPORT FINAL DES TESTS ===' AS SECTION;

SELECT
    ROW_NUMBER() OVER (ORDER BY COMPARISON_TIME) AS TEST_NUM,
    TABLE1,
    TABLE2,
    CASE WHEN HAS_PRIMARY_KEY THEN 'Avec PK' ELSE 'Sans PK' END AS METHOD,
    TABLE1_ROW_COUNT AS T1_ROWS,
    TABLE2_ROW_COUNT AS T2_ROWS,
    MATCHED_ROWS,
    ONLY_IN_TABLE1 AS ONLY_T1,
    ONLY_IN_TABLE2 AS ONLY_T2,
    DIFF_VALUES,
    MATCH_PERCENTAGE AS MATCH_PCT,
    CASE WHEN IS_IDENTICAL THEN 'IDENTICAL' ELSE 'DIFFERENT' END AS STATUS,
    EXECUTION_TIME_SEC AS EXEC_SEC
FROM DIFF_RESULTS
WHERE (TABLE1 LIKE 'TEST_TABLE%' OR TABLE2 LIKE 'TEST_TABLE%')
ORDER BY COMPARISON_TIME;

-- Résumé des tests
SELECT
    COUNT(*) AS TOTAL_TESTS,
    SUM(CASE WHEN IS_IDENTICAL THEN 1 ELSE 0 END) AS IDENTICAL_TABLES,
    SUM(CASE WHEN NOT IS_IDENTICAL THEN 1 ELSE 0 END) AS DIFFERENT_TABLES,
    AVG(EXECUTION_TIME_SEC) AS AVG_EXEC_TIME_SEC
FROM DIFF_RESULTS
WHERE (TABLE1 LIKE 'TEST_TABLE%' OR TABLE2 LIKE 'TEST_TABLE%');

SELECT 'Tests terminés!' AS MESSAGE;
