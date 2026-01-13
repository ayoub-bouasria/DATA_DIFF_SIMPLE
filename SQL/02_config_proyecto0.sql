-- ============================================================================
-- DATA DIFF TOOL - CONFIGURATION PROYECTO 0
-- ============================================================================
-- Description: Configuration exemple pour le projet PROYECTO 0
-- Version: 1.0
-- Date: 2025-01-09
-- ============================================================================

USE DATABASE TEAM_DB;
USE SCHEMA EXTERNAL;

-- ============================================================================
-- ENREGISTRER LE PROJET PROYECTO 0
-- ============================================================================

CALL SP_REGISTER_PROJECT(
    'PROYECTO0',                                    -- project_id
    'Remesas Bancarias - Traitement des remises',  -- project_name
    'TEAM_DB',                                      -- target_database
    'EXTERNAL',                                     -- target_schema
    'Migration SAS vers Snowflake du traitement des remises bancaires'
);

-- ============================================================================
-- ENREGISTRER LES TABLES À COMPARER
-- ============================================================================

-- Table principale : HISTORICO_REMESAS
CALL SP_REGISTER_TABLE(
    'PROYECTO0',                                    -- project_id
    'HISTORICO_REMESAS',                           -- table_name
    'TEAM_DB.EXTERNAL.HISTORICO_REMESAS',         -- snow_table
    'CODNUM',                                      -- primary_key_cols
    'CONTRATO,FECHA,IMPORTE,DEVUELTA,DIAS',       -- compare_cols
    'HIGH',                                        -- priority
    0.01                                           -- numeric_tolerance
);

-- Table OUTPUT_ENVIO
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'OUTPUT_ENVIO',
    'TEAM_DB.EXTERNAL.OUTPUT_ENVIO',
    'CODNUM',
    'CONTRATO,FECHA,CUOTA,CHECK_FINAL',
    'HIGH',
    0.01
);

-- Table OUTPUT_ENVIO_BBVA
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'OUTPUT_ENVIO_BBVA',
    'TEAM_DB.EXTERNAL.OUTPUT_ENVIO_BBVA',
    'CODNUM',
    'ALL',                                         -- Comparer toutes les colonnes
    'HIGH',
    0.01
);

-- Table OUTPUT_IBAN_NO_OK
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'OUTPUT_IBAN_NO_OK',
    'TEAM_DB.EXTERNAL.OUTPUT_IBAN_NO_OK',
    'CODNUM',
    'ALL',
    'MEDIUM',
    0
);

-- Table OUTPUT_NOENVIO
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'OUTPUT_NOENVIO',
    'TEAM_DB.EXTERNAL.OUTPUT_NOENVIO',
    'CODNUM',
    'ALL',
    'MEDIUM',
    0
);

-- Table OUTPUT_KO_EXCLUSIONES
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'OUTPUT_KO_EXCLUSIONES',
    'TEAM_DB.EXTERNAL.OUTPUT_KO_EXCLUSIONES',
    'CODNUM',
    'ALL',
    'MEDIUM',
    0
);

-- Table OUTPUT_DOMICILIACIONES_NO_NEGATIVAS (clé primaire composite)
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'OUTPUT_DOMICILIACIONES_NO_NEGATIVAS',
    'TEAM_DB.EXTERNAL.OUTPUT_DOMICILIACIONES_NO_NEGATIVAS',
    'CONTRATO,CODNUM',                             -- Clé primaire composite
    'FECHA,CUOTA',
    'HIGH',
    0.01
);

-- Table FICHEROS_E002
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'FICHEROS_E002',
    'TEAM_DB.EXTERNAL.FICHEROS_E002',
    'FICHERO',
    'FECHA_FICHERO,FECHA_GESTION',
    'MEDIUM',
    0
);

-- Table FICHEROS_REMESAS
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'FICHEROS_REMESAS',
    'TEAM_DB.EXTERNAL.FICHEROS_REMESAS',
    'FICHERO',
    'ALL',
    'LOW',
    0
);

-- Table FICHEROS_DEVOLUCIONES
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'FICHEROS_DEVOLUCIONES',
    'TEAM_DB.EXTERNAL.FICHEROS_DEVOLUCIONES',
    'FICHERO',
    'ALL',
    'LOW',
    0
);

-- Table EVOLUCION_RP
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'EVOLUCION_RP',
    'TEAM_DB.EXTERNAL.EVOLUCION_RP',
    'FICHERO_SIBS',
    'RP1,RP5,RP10,RP15,RP20',
    'MEDIUM',
    0
);

-- Table EXCLUSIONES_REMESAS (pas de clé primaire explicite)
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'EXCLUSIONES_REMESAS',
    'TEAM_DB.EXTERNAL.EXCLUSIONES_REMESAS',
    NULL,                                          -- Pas de clé primaire
    'ALL',
    'MEDIUM',
    0
);

-- Table REMESA_BBVA (pas de clé primaire explicite)
CALL SP_REGISTER_TABLE(
    'PROYECTO0',
    'REMESA_BBVA',
    'TEAM_DB.EXTERNAL.REMESA_BBVA',
    NULL,                                          -- Pas de clé primaire
    'ALL',
    'MEDIUM',
    0.01
);

-- ============================================================================
-- VÉRIFIER LA CONFIGURATION
-- ============================================================================

-- Afficher le projet configuré
SELECT * FROM CONFIG_PROJECTS WHERE PROJECT_ID = 'PROYECTO0';

-- Afficher toutes les tables configurées
SELECT 
    TABLE_NAME,
    SNOW_TABLE,
    PRIMARY_KEY_COLS,
    CASE 
        WHEN COMPARE_COLS = 'ALL' THEN 'Toutes les colonnes'
        ELSE COMPARE_COLS 
    END AS COLUMNS_TO_COMPARE,
    PRIORITY,
    NUMERIC_TOLERANCE,
    CASE 
        WHEN PRIMARY_KEY_COLS IS NULL THEN 'Sans PK (hash complet)'
        ELSE 'Avec PK'
    END AS COMPARISON_METHOD
FROM CONFIG_TABLES
WHERE PROJECT_ID = 'PROYECTO0'
ORDER BY 
    CASE PRIORITY 
        WHEN 'HIGH' THEN 1 
        WHEN 'MEDIUM' THEN 2 
        ELSE 3 
    END,
    TABLE_NAME;

-- Message de confirmation
SELECT 'Configuration PROYECTO0 completed!' AS MESSAGE,
       COUNT(*) AS TABLES_CONFIGURED
FROM CONFIG_TABLES
WHERE PROJECT_ID = 'PROYECTO0';