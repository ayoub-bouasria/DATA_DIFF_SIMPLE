-- ============================================================================
-- DATA DIFF TOOL - TESTS DE VALIDATION
-- ============================================================================
-- Description: Script de tests pour valider le fonctionnement de l'outil
-- Version: 1.0
-- Date: 2025-01-13
-- ============================================================================

USE DATABASE TEAM_DB;
USE SCHEMA EXTERNAL;

-- ============================================================================
-- PARTIE 1 : CRÉATION DES TABLES DE TEST
-- ============================================================================

-- Nettoyer les tables de test existantes
DROP TABLE IF EXISTS TEST_TABLE_A;
DROP TABLE IF EXISTS TEST_TABLE_B;
DROP TABLE IF EXISTS TEST_TABLE_IDENTICAL_A;
DROP TABLE IF EXISTS TEST_TABLE_IDENTICAL_B;
DROP TABLE IF EXISTS TEST_TABLE_NO_PK_A;
DROP TABLE IF EXISTS TEST_TABLE_NO_PK_B;
DROP TABLE IF EXISTS TEST_TABLE_COMPOSITE_PK_A;
DROP TABLE IF EXISTS TEST_TABLE_COMPOSITE_PK_B;
DROP TABLE IF EXISTS TEST_TABLE_NUMERIC_A;
DROP TABLE IF EXISTS TEST_TABLE_NUMERIC_B;

-- ============================================================================
-- TEST 1: Tables avec clé primaire simple - Différences détectées
-- ============================================================================

CREATE OR REPLACE TABLE TEST_TABLE_A (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100),
    VALUE NUMBER(10,2),
    CREATED_DATE DATE
);

CREATE OR REPLACE TABLE TEST_TABLE_B (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100),
    VALUE NUMBER(10,2),
    CREATED_DATE DATE
);

-- Insérer des données dans TABLE_A
INSERT INTO TEST_TABLE_A VALUES
    (1, 'Item 1', 100.00, '2024-01-01'),
    (2, 'Item 2', 200.00, '2024-01-02'),
    (3, 'Item 3', 300.00, '2024-01-03'),
    (4, 'Item 4', 400.00, '2024-01-04'),
    (5, 'Item 5', 500.00, '2024-01-05');

-- Insérer des données différentes dans TABLE_B
INSERT INTO TEST_TABLE_B VALUES
    (1, 'Item 1', 100.00, '2024-01-01'),        -- Identique
    (2, 'Item 2 Modified', 200.00, '2024-01-02'), -- NAME différent
    (3, 'Item 3', 350.00, '2024-01-03'),        -- VALUE différent
    (4, 'Item 4', 400.00, '2024-01-04'),        -- Identique
    (6, 'Item 6', 600.00, '2024-01-06');        -- Nouvelle ligne (ID=5 manquant)

-- ============================================================================
-- TEST 2: Tables identiques
-- ============================================================================

CREATE OR REPLACE TABLE TEST_TABLE_IDENTICAL_A (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100),
    VALUE NUMBER(10,2)
);

CREATE OR REPLACE TABLE TEST_TABLE_IDENTICAL_B (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100),
    VALUE NUMBER(10,2)
);

INSERT INTO TEST_TABLE_IDENTICAL_A VALUES
    (1, 'Alpha', 10.50),
    (2, 'Beta', 20.75),
    (3, 'Gamma', 30.25);

INSERT INTO TEST_TABLE_IDENTICAL_B VALUES
    (1, 'Alpha', 10.50),
    (2, 'Beta', 20.75),
    (3, 'Gamma', 30.25);

-- ============================================================================
-- TEST 3: Tables sans clé primaire
-- ============================================================================

CREATE OR REPLACE TABLE TEST_TABLE_NO_PK_A (
    COL1 VARCHAR(50),
    COL2 NUMBER,
    COL3 DATE
);

CREATE OR REPLACE TABLE TEST_TABLE_NO_PK_B (
    COL1 VARCHAR(50),
    COL2 NUMBER,
    COL3 DATE
);

INSERT INTO TEST_TABLE_NO_PK_A VALUES
    ('Row A', 100, '2024-01-01'),
    ('Row B', 200, '2024-01-02'),
    ('Row C', 300, '2024-01-03'),
    ('Row D', 400, '2024-01-04');

INSERT INTO TEST_TABLE_NO_PK_B VALUES
    ('Row A', 100, '2024-01-01'),  -- Identique
    ('Row B', 250, '2024-01-02'),  -- Différent
    ('Row C', 300, '2024-01-03'),  -- Identique
    ('Row E', 500, '2024-01-05');  -- Nouveau

-- ============================================================================
-- TEST 4: Tables avec clé primaire composite
-- ============================================================================

CREATE OR REPLACE TABLE TEST_TABLE_COMPOSITE_PK_A (
    CLIENT_ID NUMBER,
    ORDER_ID NUMBER,
    PRODUCT VARCHAR(100),
    QUANTITY NUMBER,
    PRICE NUMBER(10,2),
    PRIMARY KEY (CLIENT_ID, ORDER_ID)
);

CREATE OR REPLACE TABLE TEST_TABLE_COMPOSITE_PK_B (
    CLIENT_ID NUMBER,
    ORDER_ID NUMBER,
    PRODUCT VARCHAR(100),
    QUANTITY NUMBER,
    PRICE NUMBER(10,2),
    PRIMARY KEY (CLIENT_ID, ORDER_ID)
);

INSERT INTO TEST_TABLE_COMPOSITE_PK_A VALUES
    (1, 101, 'Product A', 5, 99.99),
    (1, 102, 'Product B', 3, 49.99),
    (2, 101, 'Product C', 10, 29.99),
    (2, 102, 'Product D', 2, 199.99);

INSERT INTO TEST_TABLE_COMPOSITE_PK_B VALUES
    (1, 101, 'Product A', 5, 99.99),      -- Identique
    (1, 102, 'Product B', 5, 49.99),      -- QUANTITY différente
    (2, 101, 'Product C Modified', 10, 29.99),  -- PRODUCT différent
    (3, 101, 'Product E', 1, 299.99);     -- Nouveau client

-- ============================================================================
-- TEST 5: Tables avec tolérance numérique
-- ============================================================================

CREATE OR REPLACE TABLE TEST_TABLE_NUMERIC_A (
    ID NUMBER PRIMARY KEY,
    AMOUNT NUMBER(15,6),
    PERCENTAGE NUMBER(5,4)
);

CREATE OR REPLACE TABLE TEST_TABLE_NUMERIC_B (
    ID NUMBER PRIMARY KEY,
    AMOUNT NUMBER(15,6),
    PERCENTAGE NUMBER(5,4)
);

INSERT INTO TEST_TABLE_NUMERIC_A VALUES
    (1, 1000.123456, 0.1234),
    (2, 2000.654321, 0.5678),
    (3, 3000.111111, 0.9999);

-- Petites différences de précision
INSERT INTO TEST_TABLE_NUMERIC_B VALUES
    (1, 1000.123457, 0.1234),  -- Diff de 0.000001
    (2, 2000.654322, 0.5679),  -- Diff de 0.000001 et 0.0001
    (3, 3000.111111, 0.9999);  -- Identique

-- ============================================================================
-- TEST 6: Table vide vs table avec données
-- ============================================================================

CREATE OR REPLACE TABLE TEST_TABLE_EMPTY (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100)
);

CREATE OR REPLACE TABLE TEST_TABLE_WITH_DATA (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100)
);

INSERT INTO TEST_TABLE_WITH_DATA VALUES (1, 'Only Entry');

-- ============================================================================
-- TEST 7: Tables avec valeurs NULL
-- ============================================================================

CREATE OR REPLACE TABLE TEST_TABLE_NULLS_A (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100),
    OPTIONAL_VALUE NUMBER
);

CREATE OR REPLACE TABLE TEST_TABLE_NULLS_B (
    ID NUMBER PRIMARY KEY,
    NAME VARCHAR(100),
    OPTIONAL_VALUE NUMBER
);

INSERT INTO TEST_TABLE_NULLS_A VALUES
    (1, 'With Value', 100),
    (2, 'With NULL', NULL),
    (3, NULL, 300);

INSERT INTO TEST_TABLE_NULLS_B VALUES
    (1, 'With Value', 100),
    (2, 'With NULL', NULL),
    (3, NULL, 300);

-- ============================================================================
-- Confirmation de la création des tables de test
-- ============================================================================

SELECT 'Tables de test créées avec succès!' AS MESSAGE;

SELECT
    TABLE_NAME,
    ROW_COUNT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'EXTERNAL'
  AND TABLE_NAME LIKE 'TEST_TABLE%'
ORDER BY TABLE_NAME;
