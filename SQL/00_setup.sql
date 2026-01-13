-- ============================================================================
-- DATA DIFF TOOL - SETUP SCRIPT
-- ============================================================================
-- Description: Crée le schéma et les tables nécessaires pour l'outil Data Diff
-- Version: 1.0
-- Date: 2025-01-09
-- ============================================================================

-- Créer la base de données et le schéma
CREATE DATABASE IF NOT EXISTS DATA_DIFF;
CREATE SCHEMA IF NOT EXISTS DATA_DIFF.CONFIG;
CREATE SCHEMA IF NOT EXISTS DATA_DIFF.STAGING;
CREATE SCHEMA IF NOT EXISTS DATA_DIFF.RESULTS;

USE DATABASE DATA_DIFF;
USE SCHEMA CONFIG;

-- ============================================================================
-- TABLES DE CONFIGURATION
-- ============================================================================

-- Table des projets
CREATE OR REPLACE TABLE CONFIG_PROJECTS (
    PROJECT_ID          VARCHAR(50) PRIMARY KEY,
    PROJECT_NAME        VARCHAR(200) NOT NULL,
    TARGET_DATABASE     VARCHAR(100) NOT NULL,
    TARGET_SCHEMA       VARCHAR(100) NOT NULL,
    DESCRIPTION         VARCHAR(1000),
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY          VARCHAR(100) DEFAULT CURRENT_USER(),
    IS_ACTIVE           BOOLEAN DEFAULT TRUE
);

-- Table des tables à comparer
CREATE OR REPLACE TABLE CONFIG_TABLES (
    CONFIG_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    PROJECT_ID          VARCHAR(50) NOT NULL,
    TABLE_NAME          VARCHAR(200) NOT NULL,
    SAS_TABLE_NAME      VARCHAR(200),
    SNOW_TABLE          VARCHAR(500) NOT NULL,  -- Format: DB.SCHEMA.TABLE
    PRIMARY_KEY_COLS    VARCHAR(2000),          -- CSV list ou NULL si pas de PK
    COMPARE_COLS        VARCHAR(4000),          -- CSV list ou 'ALL'
    NUMERIC_TOLERANCE   NUMBER(10,6) DEFAULT 0, -- Tolérance pour colonnes numériques
    PRIORITY            VARCHAR(10) DEFAULT 'MEDIUM', -- HIGH, MEDIUM, LOW
    IS_ACTIVE           BOOLEAN DEFAULT TRUE,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CREATED_BY          VARCHAR(100) DEFAULT CURRENT_USER(),
    CONSTRAINT FK_PROJECT FOREIGN KEY (PROJECT_ID) REFERENCES CONFIG_PROJECTS(PROJECT_ID),
    CONSTRAINT UK_PROJECT_TABLE UNIQUE (PROJECT_ID, TABLE_NAME)
);

-- Table de mapping des colonnes (optionnel)
CREATE OR REPLACE TABLE CONFIG_COLUMN_MAPPING (
    MAPPING_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    PROJECT_ID          VARCHAR(50) NOT NULL,
    TABLE_NAME          VARCHAR(200) NOT NULL,
    SAS_COLUMN          VARCHAR(200) NOT NULL,
    SNOW_COLUMN         VARCHAR(200) NOT NULL,
    DATA_TYPE           VARCHAR(50),
    IS_NUMERIC          BOOLEAN DEFAULT FALSE,
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT FK_CONFIG FOREIGN KEY (PROJECT_ID, TABLE_NAME) 
        REFERENCES CONFIG_TABLES(PROJECT_ID, TABLE_NAME)
);

-- ============================================================================
-- TABLES DE STAGING
-- ============================================================================

USE SCHEMA STAGING;

-- Table générique pour stocker les données SAS
CREATE OR REPLACE TABLE SAS_STAGING_DATA (
    LOAD_ID             VARCHAR(50) NOT NULL,
    PROJECT_ID          VARCHAR(50) NOT NULL,
    TABLE_NAME          VARCHAR(200) NOT NULL,
    ROW_DATA            VARIANT NOT NULL,       -- Données JSON de la ligne
    ROW_HASH            VARCHAR(64),            -- Hash pour comparaison rapide
    LOADED_AT           TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Index pour améliorer les performances
CREATE OR REPLACE INDEX IDX_SAS_STAGING ON SAS_STAGING_DATA(PROJECT_ID, TABLE_NAME, LOAD_ID);

-- ============================================================================
-- TABLES DE RÉSULTATS
-- ============================================================================

USE SCHEMA RESULTS;

-- Résumé des runs de comparaison
CREATE OR REPLACE TABLE COMPARISON_RUNS (
    RUN_ID              VARCHAR(50) PRIMARY KEY DEFAULT UUID_STRING(),
    PROJECT_ID          VARCHAR(50) NOT NULL,
    RUN_TYPE            VARCHAR(20) DEFAULT 'FULL', -- FULL, INCREMENTAL
    START_TIME          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    END_TIME            TIMESTAMP_NTZ,
    TOTAL_TABLES        NUMBER DEFAULT 0,
    TABLES_MATCHED      NUMBER DEFAULT 0,
    TABLES_DIFFERENT    NUMBER DEFAULT 0,
    TABLES_ERROR        NUMBER DEFAULT 0,
    STATUS              VARCHAR(20) DEFAULT 'RUNNING', -- RUNNING, SUCCESS, ERROR
    ERROR_MESSAGE       VARCHAR(4000),
    CREATED_BY          VARCHAR(100) DEFAULT CURRENT_USER()
);

-- Résultats détaillés par table
CREATE OR REPLACE TABLE COMPARISON_RESULTS (
    RESULT_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID              VARCHAR(50) NOT NULL,
    PROJECT_ID          VARCHAR(50) NOT NULL,
    TABLE_NAME          VARCHAR(200) NOT NULL,
    RUN_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Métriques de comparaison
    SAS_ROW_COUNT       NUMBER DEFAULT 0,
    SNOW_ROW_COUNT      NUMBER DEFAULT 0,
    MATCHED_ROWS        NUMBER DEFAULT 0,
    ONLY_IN_SAS         NUMBER DEFAULT 0,
    ONLY_IN_SNOW        NUMBER DEFAULT 0,
    DIFF_VALUES         NUMBER DEFAULT 0,
    
    -- Résultats
    MATCH_PERCENTAGE    NUMBER(5,2),
    STATUS              VARCHAR(20), -- IDENTICAL, DIFFERENT, ERROR
    ERROR_MESSAGE       VARCHAR(4000),
    
    -- Métadonnées
    EXECUTION_TIME_SEC  NUMBER,
    HAS_PRIMARY_KEY     BOOLEAN,
    
    CONSTRAINT FK_RUN FOREIGN KEY (RUN_ID) REFERENCES COMPARISON_RUNS(RUN_ID)
);

-- Détail des différences trouvées
CREATE OR REPLACE TABLE COMPARISON_DIFF_DETAILS (
    DETAIL_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID              VARCHAR(50) NOT NULL,
    PROJECT_ID          VARCHAR(50) NOT NULL,
    TABLE_NAME          VARCHAR(200) NOT NULL,
    
    -- Information sur la différence
    DIFF_TYPE           VARCHAR(20) NOT NULL, -- ONLY_SAS, ONLY_SNOW, VALUE_DIFF
    PRIMARY_KEY_VALUE   VARCHAR(4000),        -- NULL si pas de PK
    ROW_IDENTIFIER      VARCHAR(4000),        -- Hash ou autre identifiant unique
    
    -- Détails de la différence (pour VALUE_DIFF)
    COLUMN_NAME         VARCHAR(200),
    SAS_VALUE           VARCHAR(4000),
    SNOW_VALUE          VARCHAR(4000),
    
    -- Données complètes (JSON)
    SAS_ROW_DATA        VARIANT,
    SNOW_ROW_DATA       VARIANT,
    
    CREATED_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    CONSTRAINT FK_DIFF_RUN FOREIGN KEY (RUN_ID) REFERENCES COMPARISON_RUNS(RUN_ID)
);

-- Table de logging pour debug
CREATE OR REPLACE TABLE EXECUTION_LOGS (
    LOG_ID              NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_ID              VARCHAR(50),
    PROCEDURE_NAME      VARCHAR(100),
    LOG_LEVEL           VARCHAR(10), -- INFO, WARN, ERROR, DEBUG
    LOG_MESSAGE         VARCHAR(4000),
    LOG_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    EXECUTION_TIME_MS   NUMBER
);

-- ============================================================================
-- VUES UTILITAIRES
-- ============================================================================

-- Vue pour le dashboard de résumé
CREATE OR REPLACE VIEW V_COMPARISON_SUMMARY AS
SELECT 
    cr.RUN_ID,
    cr.PROJECT_ID,
    cp.PROJECT_NAME,
    cr.START_TIME,
    cr.END_TIME,
    DATEDIFF('minute', cr.START_TIME, COALESCE(cr.END_TIME, CURRENT_TIMESTAMP())) AS DURATION_MIN,
    cr.TOTAL_TABLES,
    cr.TABLES_MATCHED,
    cr.TABLES_DIFFERENT,
    cr.TABLES_ERROR,
    CASE 
        WHEN cr.TOTAL_TABLES = 0 THEN 0
        ELSE ROUND(100.0 * cr.TABLES_MATCHED / cr.TOTAL_TABLES, 2)
    END AS SUCCESS_RATE,
    cr.STATUS,
    cr.CREATED_BY
FROM RESULTS.COMPARISON_RUNS cr
JOIN CONFIG.CONFIG_PROJECTS cp ON cr.PROJECT_ID = cp.PROJECT_ID
ORDER BY cr.START_TIME DESC;

-- Vue détaillée des résultats par table
CREATE OR REPLACE VIEW V_TABLE_RESULTS AS
SELECT 
    r.RUN_ID,
    r.PROJECT_ID,
    r.TABLE_NAME,
    r.SAS_ROW_COUNT,
    r.SNOW_ROW_COUNT,
    r.MATCHED_ROWS,
    r.ONLY_IN_SAS,
    r.ONLY_IN_SNOW,
    r.DIFF_VALUES,
    r.MATCH_PERCENTAGE,
    r.STATUS,
    CASE 
        WHEN r.STATUS = 'IDENTICAL' THEN '✓'
        WHEN r.STATUS = 'DIFFERENT' THEN '✗'
        ELSE '⚠'
    END AS STATUS_ICON,
    r.EXECUTION_TIME_SEC,
    r.HAS_PRIMARY_KEY
FROM RESULTS.COMPARISON_RESULTS r
ORDER BY r.RUN_TIMESTAMP DESC;

-- ============================================================================
-- STAGES POUR CHARGEMENT DES FICHIERS
-- ============================================================================

-- Créer le stage pour les exports SAS
CREATE OR REPLACE STAGE DATA_DIFF.STAGING.SAS_EXPORTS
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        NULL_IF = ('', 'NULL', '.', 'null')
        EMPTY_FIELD_AS_NULL = TRUE
        ENCODING = 'UTF8'
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    );

-- ============================================================================
-- PERMISSIONS
-- ============================================================================

-- Créer un rôle pour l'utilisation de l'outil
CREATE ROLE IF NOT EXISTS DATA_DIFF_USER;

-- Accorder les permissions nécessaires
GRANT USAGE ON DATABASE DATA_DIFF TO ROLE DATA_DIFF_USER;
GRANT USAGE ON ALL SCHEMAS IN DATABASE DATA_DIFF TO ROLE DATA_DIFF_USER;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN DATABASE DATA_DIFF TO ROLE DATA_DIFF_USER;
GRANT USAGE ON ALL STAGES IN DATABASE DATA_DIFF TO ROLE DATA_DIFF_USER;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DATA_DIFF_USER;

-- Message de confirmation
SELECT 'Setup completed successfully!' AS MESSAGE;