-- ============================================================================
-- DATA DIFF TOOL - SETUP POUR TEAM_DB.EXTERNAL
-- ============================================================================
-- Description: Crée les objets nécessaires dans TEAM_DB.EXTERNAL
-- Version: 2.0
-- Date: 2025-01-09
-- ============================================================================

-- Utiliser la base et le schéma existants
USE DATABASE TEAM_DB;
USE SCHEMA EXTERNAL;

-- ============================================================================
-- TABLES DE RÉSULTATS DE COMPARAISON
-- ============================================================================

-- Table pour stocker les résultats de comparaison
CREATE OR REPLACE TABLE DIFF_RESULTS (
    COMPARISON_ID       VARCHAR(50) DEFAULT UUID_STRING() PRIMARY KEY,
    TABLE1              VARCHAR(200) NOT NULL,
    TABLE2              VARCHAR(200) NOT NULL,
    COMPARISON_TIME     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Métriques
    TABLE1_ROW_COUNT    NUMBER DEFAULT 0,
    TABLE2_ROW_COUNT    NUMBER DEFAULT 0,
    MATCHED_ROWS        NUMBER DEFAULT 0,
    ONLY_IN_TABLE1      NUMBER DEFAULT 0,
    ONLY_IN_TABLE2      NUMBER DEFAULT 0,
    DIFF_VALUES         NUMBER DEFAULT 0,
    
    -- Résultats
    MATCH_PERCENTAGE    NUMBER(5,2),
    IS_IDENTICAL        BOOLEAN,
    HAS_PRIMARY_KEY     BOOLEAN,
    PRIMARY_KEY_COLS    VARCHAR(1000),
    
    -- Métadonnées
    EXECUTION_TIME_SEC  NUMBER,
    EXECUTED_BY         VARCHAR(100) DEFAULT CURRENT_USER()
);

-- Table pour stocker le détail des différences
CREATE OR REPLACE TABLE DIFF_DETAILS (
    DETAIL_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    COMPARISON_ID       VARCHAR(50) NOT NULL,
    
    -- Type de différence
    DIFF_TYPE           VARCHAR(20) NOT NULL, -- ONLY_TABLE1, ONLY_TABLE2, VALUE_DIFF
    
    -- Identification de la ligne
    PRIMARY_KEY_VALUE   VARCHAR(4000),        -- Si PK existe
    ROW_HASH            VARCHAR(64),          -- Si pas de PK
    
    -- Détails (pour VALUE_DIFF uniquement)
    COLUMN_NAME         VARCHAR(200),
    VALUE_TABLE1        VARCHAR(4000),
    VALUE_TABLE2        VARCHAR(4000),
    
    -- Données complètes
    ROW_DATA_TABLE1     VARIANT,
    ROW_DATA_TABLE2     VARIANT,
    
    CONSTRAINT FK_COMPARISON FOREIGN KEY (COMPARISON_ID) 
        REFERENCES DIFF_RESULTS(COMPARISON_ID)
);

-- Table temporaire pour stocker les colonnes d'une table
CREATE OR REPLACE TABLE TEMP_TABLE_COLUMNS (
    TABLE_NAME          VARCHAR(200),
    COLUMN_NAME         VARCHAR(200),
    DATA_TYPE           VARCHAR(100),
    ORDINAL_POSITION    NUMBER
);

-- ============================================================================
-- VUES UTILITAIRES
-- ============================================================================

-- Vue résumée des comparaisons récentes
CREATE OR REPLACE VIEW V_RECENT_COMPARISONS AS
SELECT 
    COMPARISON_ID,
    TABLE1,
    TABLE2,
    COMPARISON_TIME,
    TABLE1_ROW_COUNT,
    TABLE2_ROW_COUNT,
    MATCH_PERCENTAGE,
    CASE 
        WHEN IS_IDENTICAL THEN '✓ IDENTIQUE'
        ELSE '✗ DIFFÉRENT'
    END AS STATUS,
    EXECUTION_TIME_SEC
FROM DIFF_RESULTS
ORDER BY COMPARISON_TIME DESC
LIMIT 100;

-- Vue des différences par type
CREATE OR REPLACE VIEW V_DIFF_SUMMARY AS
SELECT 
    d.COMPARISON_ID,
    r.TABLE1,
    r.TABLE2,
    d.DIFF_TYPE,
    COUNT(*) AS COUNT_DIFFERENCES
FROM DIFF_DETAILS d
JOIN DIFF_RESULTS r ON d.COMPARISON_ID = r.COMPARISON_ID
GROUP BY d.COMPARISON_ID, r.TABLE1, r.TABLE2, d.DIFF_TYPE
ORDER BY d.COMPARISON_ID DESC, d.DIFF_TYPE;

-- ============================================================================
-- PROCÉDURE UTILITAIRE : GET_TABLE_COLUMNS
-- ============================================================================
-- CREATE OR REPLACE PROCEDURE GET_TABLE_COLUMNS(
--     P_TABLE_NAME VARCHAR
-- )
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     v_db VARCHAR;
--     v_schema VARCHAR;
--     v_table VARCHAR;
--     v_parts ARRAY;
-- BEGIN
--     -- Parser le nom complet de la table
--     v_parts := SPLIT(P_TABLE_NAME, '.');
    
--     IF ARRAY_SIZE(v_parts) = 3 THEN
--         v_db := v_parts[0];
--         v_schema := v_parts[1];
--         v_table := v_parts[2];
--     ELSIF ARRAY_SIZE(v_parts) = 2 THEN
--         v_db := CURRENT_DATABASE();
--         v_schema := v_parts[0];
--         v_table := v_parts[1];
--     ELSIF ARRAY_SIZE(v_parts) = 1 THEN
--         v_db := CURRENT_DATABASE();
--         v_schema := CURRENT_SCHEMA();
--         v_table := v_parts[0];
--     ELSE
--         RETURN 'Error: Invalid table name format';
--     END IF;
    
--     -- Nettoyer la table temporaire
--     TRUNCATE TABLE TEMP_TABLE_COLUMNS;
    
--     -- Récupérer les colonnes
--     INSERT INTO TEMP_TABLE_COLUMNS
--     SELECT 
--         TABLE_NAME,
--         COLUMN_NAME,
--         DATA_TYPE,
--         ORDINAL_POSITION
--     FROM INFORMATION_SCHEMA.COLUMNS
--     WHERE TABLE_CATALOG = UPPER(v_db)
--       AND TABLE_SCHEMA = UPPER(v_schema)
--       AND TABLE_NAME = UPPER(v_table)
--     ORDER BY ORDINAL_POSITION;
    
--     RETURN 'Columns retrieved for ' || P_TABLE_NAME;
-- END;
-- $$;





CREATE OR REPLACE PROCEDURE GET_TABLE_COLUMNS(P_TABLE_NAME STRING)
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_db STRING;
    v_schema STRING;
    v_table STRING;
    v_parts_count NUMBER;
BEGIN
    -- parts = nombre de points + 1
    v_parts_count := LENGTH(P_TABLE_NAME) - LENGTH(REPLACE(P_TABLE_NAME, '.', '')) + 1;

    -- Déterminer DB / SCHÉMA / TABLE selon le format fourni
    IF (v_parts_count = 3) THEN
        v_db     := SPLIT_PART(P_TABLE_NAME, '.', 1);
        v_schema := SPLIT_PART(P_TABLE_NAME, '.', 2);
        v_table  := SPLIT_PART(P_TABLE_NAME, '.', 3);

    ELSEIF (v_parts_count = 2) THEN
        v_db     := CURRENT_DATABASE();
        v_schema := SPLIT_PART(P_TABLE_NAME, '.', 1);
        v_table  := SPLIT_PART(P_TABLE_NAME, '.', 2);

    ELSEIF (v_parts_count = 1) THEN
        v_db     := CURRENT_DATABASE();
        v_schema := CURRENT_SCHEMA();
        v_table  := P_TABLE_NAME;

    ELSE
        RETURN 'Error: Invalid table name format';
    END IF;

    -- Nettoyer la table cible (doit exister)
    TRUNCATE TABLE TEMP_TABLE_COLUMNS;

    -- Insérer les colonnes de la table demandée
    INSERT INTO TEMP_TABLE_COLUMNS
    SELECT 
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        ORDINAL_POSITION
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_CATALOG = UPPER(v_db)
      AND TABLE_SCHEMA  = UPPER(v_schema)
      AND TABLE_NAME    = UPPER(v_table)
    ORDER BY ORDINAL_POSITION;

    RETURN 'Columns retrieved for ' || v_db || '.' || v_schema || '.' || v_table;
END;
$$;


-- ============================================================================
-- PROCÉDURE PRINCIPALE : SP_COMPARE
-- ============================================================================
-- CREATE OR REPLACE PROCEDURE SP_COMPARE(
--     P_TABLE1 VARCHAR,
--     P_TABLE2 VARCHAR,
--     P_PRIMARY_KEY VARCHAR DEFAULT NULL,
--     P_COLUMNS_TO_COMPARE VARCHAR DEFAULT 'ALL',
--     P_NUMERIC_TOLERANCE NUMBER DEFAULT 0,
--     P_CASE_SENSITIVE BOOLEAN DEFAULT TRUE,
--     P_SHOW_REPORT BOOLEAN DEFAULT TRUE
-- )
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     v_comparison_id VARCHAR;
--     v_has_pk BOOLEAN;
--     v_pk_cols ARRAY;
--     v_col_list ARRAY;
--     v_start_time TIMESTAMP_NTZ;
--     v_exec_time NUMBER;
    
--     v_table1_count NUMBER;
--     v_table2_count NUMBER;
--     v_matched_count NUMBER := 0;
--     v_only_table1 NUMBER := 0;
--     v_only_table2 NUMBER := 0;
--     v_diff_values NUMBER := 0;
--     v_match_pct NUMBER(5,2);
--     v_is_identical BOOLEAN;
    
--     v_sql VARCHAR;
--     v_result VARCHAR;
-- BEGIN
--     v_start_time := CURRENT_TIMESTAMP();
--     v_comparison_id := UUID_STRING();
    
--     -- Déterminer si on a une clé primaire
--     v_has_pk := (P_PRIMARY_KEY IS NOT NULL AND P_PRIMARY_KEY != '' AND UPPER(P_PRIMARY_KEY) != 'NO');
    
--     -- Parser les colonnes de la clé primaire si fournie
--     IF v_has_pk THEN
--         v_pk_cols := SPLIT(P_PRIMARY_KEY, ',');
--     END IF;
    
--     -- Compter les lignes dans chaque table
--     EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || P_TABLE1 INTO v_table1_count;
--     EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || P_TABLE2 INTO v_table2_count;
    
--     -- Appeler la procédure appropriée selon le cas
--     IF v_has_pk THEN
--         CALL SP_COMPARE_WITH_PRIMARY_KEY(
--             v_comparison_id, P_TABLE1, P_TABLE2, P_PRIMARY_KEY, 
--             P_COLUMNS_TO_COMPARE, P_NUMERIC_TOLERANCE, P_CASE_SENSITIVE,
--             v_matched_count, v_only_table1, v_only_table2, v_diff_values
--         );
--     ELSE
--         CALL SP_COMPARE_WITHOUT_PRIMARY_KEY(
--             v_comparison_id, P_TABLE1, P_TABLE2, 
--             P_COLUMNS_TO_COMPARE, P_CASE_SENSITIVE,
--             v_matched_count, v_only_table1, v_only_table2
--         );
--         v_diff_values := 0; -- Pas de diff values sans PK
--     END IF;
    
--     -- Calculer le pourcentage de correspondance
--     IF v_table1_count + v_table2_count > 0 THEN
--         v_match_pct := ROUND(200.0 * v_matched_count / (v_table1_count + v_table2_count), 2);
--     ELSE
--         v_match_pct := 100.0;
--     END IF;
    
--     -- Déterminer si les tables sont identiques
--     v_is_identical := (v_table1_count = v_table2_count AND 
--                       v_only_table1 = 0 AND v_only_table2 = 0 AND v_diff_values = 0);
    
--     -- Calculer le temps d'exécution
--     v_exec_time := DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP());
    
--     -- Entrer les résultats
--     INSERT INTO DIFF_RESULTS (
--         COMPARISON_ID, TABLE1, TABLE2,
--         TABLE1_ROW_COUNT, TABLE2_ROW_COUNT,
--         MATCHED_ROWS, ONLY_IN_TABLE1, ONLY_IN_TABLE2, DIFF_VALUES,
--         MATCH_PERCENTAGE, IS_IDENTICAL, HAS_PRIMARY_KEY, PRIMARY_KEY_COLS,
--         EXECUTION_TIME_SEC
--     ) VALUES (
--         v_comparison_id, P_TABLE1, P_TABLE2,
--         v_table1_count, v_table2_count,
--         v_matched_count, v_only_table1, v_only_table2, v_diff_values,
--         v_match_pct, v_is_identical, v_has_pk, P_PRIMARY_KEY,
--         v_exec_time
--     );
    
--     -- Construire le message de résultat
--     v_result := 'Comparaison terminée. ID: ' || v_comparison_id || CHR(10) ||
--                 'Tables ' || CASE WHEN v_is_identical THEN 'IDENTIQUES' ELSE 'DIFFÉRENTES' END || CHR(10) ||
--                 'Correspondance: ' || v_match_pct || '%' || CHR(10) ||
--                 'Lignes: ' || v_table1_count || ' vs ' || v_table2_count;
    
--     -- Afficher le rapport si demandé
--     IF P_SHOW_REPORT THEN
--         CALL SP_SHOW_COMPARISON_REPORT(v_comparison_id);
--     END IF;
    
--     RETURN v_result;
-- EXCEPTION
--     WHEN OTHER THEN
--         RETURN 'Erreur: ' || SQLERRM;
-- END;
-- $$;

CREATE OR REPLACE PROCEDURE SP_COMPARE(
    P_TABLE1 VARCHAR,
    P_TABLE2 VARCHAR,
    P_PRIMARY_KEY VARCHAR DEFAULT NULL,
    P_COLUMNS_TO_COMPARE VARCHAR DEFAULT 'ALL',
    P_NUMERIC_TOLERANCE NUMBER DEFAULT 0,
    P_CASE_SENSITIVE BOOLEAN DEFAULT TRUE,
    P_SHOW_REPORT BOOLEAN DEFAULT TRUE
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_comparison_id VARCHAR;
    v_has_pk BOOLEAN;
    v_pk_cols ARRAY;
    v_col_list ARRAY;
    v_start_time TIMESTAMP_NTZ;
    v_exec_time NUMBER;

    v_table1_count NUMBER;
    v_table2_count NUMBER;
    v_matched_count NUMBER := 0;
    v_only_table1 NUMBER := 0;
    v_only_table2 NUMBER := 0;
    v_diff_values NUMBER := 0;
    v_match_pct NUMBER(5,2);
    v_is_identical BOOLEAN;

    v_sql VARCHAR;
    v_result VARCHAR;
    v_pk_result VARIANT;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();
    v_comparison_id := UUID_STRING();

    -- Déterminer si on a une clé primaire
    v_has_pk := (P_PRIMARY_KEY IS NOT NULL AND P_PRIMARY_KEY != '' AND UPPER(P_PRIMARY_KEY) != 'NO');

    -- Parser les colonnes de la clé primaire si fournie
    IF (v_has_pk) THEN
        v_pk_cols := SPLIT(P_PRIMARY_KEY, ',');
    END IF;

    -- Compter les lignes dans chaque table (table dynamique => IDENTIFIER)
    SELECT COUNT(*) INTO :v_table1_count
    FROM IDENTIFIER(:P_TABLE1);

    SELECT COUNT(*) INTO :v_table2_count
    FROM IDENTIFIER(:P_TABLE2);

    -- Appeler la procédure appropriée selon le cas
    IF (v_has_pk) THEN
        -- SP_COMPARE_WITH_PRIMARY_KEY retourne un VARIANT
        CALL SP_COMPARE_WITH_PRIMARY_KEY(
            :v_comparison_id, :P_TABLE1, :P_TABLE2, :P_PRIMARY_KEY,
            :P_COLUMNS_TO_COMPARE, :P_NUMERIC_TOLERANCE, :P_CASE_SENSITIVE
        ) INTO :v_pk_result;

        -- Extraire les valeurs du résultat VARIANT
        v_matched_count := v_pk_result['matched_count']::NUMBER;
        v_only_table1 := v_pk_result['only_table1']::NUMBER;
        v_only_table2 := v_pk_result['only_table2']::NUMBER;
        v_diff_values := v_pk_result['diff_values']::NUMBER;
    ELSE
        -- SP_COMPARE_WITHOUT_PRIMARY_KEY utilise des paramètres OUT
        CALL SP_COMPARE_WITHOUT_PRIMARY_KEY(
            :v_comparison_id, :P_TABLE1, :P_TABLE2,
            :P_COLUMNS_TO_COMPARE, :P_CASE_SENSITIVE,
            :v_matched_count, :v_only_table1, :v_only_table2
        );
        v_diff_values := 0; -- Pas de diff values sans PK
    END IF;

    -- Calculer le pourcentage de correspondance
    IF (v_table1_count + v_table2_count > 0) THEN
        v_match_pct := ROUND(200.0 * v_matched_count / (v_table1_count + v_table2_count), 2);
    ELSE
        v_match_pct := 100.0;
    END IF;

    -- Déterminer si les tables sont identiques
    v_is_identical := (
        v_table1_count = v_table2_count
        AND v_only_table1 = 0
        AND v_only_table2 = 0
        AND v_diff_values = 0
    );

    -- Calculer le temps d'exécution
    v_exec_time := DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP());

    -- Enregistrer les résultats
    INSERT INTO DIFF_RESULTS (
        COMPARISON_ID, TABLE1, TABLE2,
        TABLE1_ROW_COUNT, TABLE2_ROW_COUNT,
        MATCHED_ROWS, ONLY_IN_TABLE1, ONLY_IN_TABLE2, DIFF_VALUES,
        MATCH_PERCENTAGE, IS_IDENTICAL, HAS_PRIMARY_KEY, PRIMARY_KEY_COLS,
        EXECUTION_TIME_SEC
    ) VALUES (
        v_comparison_id, P_TABLE1, P_TABLE2,
        v_table1_count, v_table2_count,
        v_matched_count, v_only_table1, v_only_table2, v_diff_values,
        v_match_pct, v_is_identical, v_has_pk, P_PRIMARY_KEY,
        v_exec_time
    );

    -- Message de résultat
    v_result := 'Comparaison terminée. ID: ' || v_comparison_id || CHR(10) ||
                'Tables ' || CASE WHEN v_is_identical THEN 'IDENTIQUES' ELSE 'DIFFÉRENTES' END || CHR(10) ||
                'Correspondance: ' || v_match_pct || '%' || CHR(10) ||
                'Lignes: ' || v_table1_count || ' vs ' || v_table2_count;

    -- Afficher le rapport si demandé
    IF (P_SHOW_REPORT) THEN
        CALL SP_SHOW_COMPARISON_REPORT(:v_comparison_id);
    END IF;

    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        RETURN 'Erreur: ' || SQLERRM;
END;
$$;

-- ============================================================================
-- PROCÉDURE : SP_COMPARE_WITH_PRIMARY_KEY
-- ============================================================================
-- CREATE OR REPLACE PROCEDURE SP_COMPARE_WITH_PRIMARY_KEY(
--     P_COMPARISON_ID VARCHAR,
--     P_TABLE1 VARCHAR,
--     P_TABLE2 VARCHAR,
--     P_PRIMARY_KEY VARCHAR,
--     P_COLUMNS_TO_COMPARE VARCHAR,
--     P_NUMERIC_TOLERANCE NUMBER,
--     P_CASE_SENSITIVE BOOLEAN,
--     P_MATCHED_COUNT NUMBER OUTPUT,
--     P_ONLY_TABLE1 NUMBER OUTPUT,
--     P_ONLY_TABLE2 NUMBER OUTPUT,
--     P_DIFF_VALUES NUMBER OUTPUT
-- )
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     v_pk_cols ARRAY;
--     v_col_list ARRAY;
--     v_sql VARCHAR;
--     v_pk_join VARCHAR := '';
--     v_pk_select VARCHAR := '';
--     v_col_compare VARCHAR := '';
--     i INTEGER;
--     v_col VARCHAR;
--     v_expr1 VARCHAR;
--     v_expr2 VARCHAR;
-- BEGIN
--     -- Parser les colonnes de la clé primaire
--     v_pk_cols := SPLIT(P_PRIMARY_KEY, ',');
    
--     -- Construire la jointure et sélection sur PK
--     FOR i IN 0 TO ARRAY_SIZE(v_pk_cols) - 1 DO
--         v_col := TRIM(v_pk_cols[i]);
--         IF i > 0 THEN
--             v_pk_join := v_pk_join || ' AND ';
--             v_pk_select := v_pk_select || ' || ''|'' || ';
--         END IF;
--         v_pk_join := v_pk_join || 't1.' || v_col || ' = t2.' || v_col;
--         v_pk_select := v_pk_select || 'COALESCE(t1.' || v_col || '::VARCHAR, '''')';
--     END LOOP;
    
--     -- 1. Trouver les lignes uniquement dans TABLE1
--     v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, PRIMARY_KEY_VALUE, ROW_DATA_TABLE1) ' ||
--              'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE1'', ' ||
--              v_pk_select || ', OBJECT_CONSTRUCT(*) ' ||
--              'FROM ' || P_TABLE1 || ' t1 ' ||
--              'WHERE NOT EXISTS (SELECT 1 FROM ' || P_TABLE2 || ' t2 WHERE ' || v_pk_join || ')';
--     EXECUTE IMMEDIATE v_sql;
--     GET DIAGNOSTICS P_ONLY_TABLE1 = ROW_COUNT;
    
--     -- 2. Trouver les lignes uniquement dans TABLE2
--     v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, PRIMARY_KEY_VALUE, ROW_DATA_TABLE2) ' ||
--              'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE2'', ' ||
--              REPLACE(v_pk_select, 't1.', 't2.') || ', OBJECT_CONSTRUCT(*) ' ||
--              'FROM ' || P_TABLE2 || ' t2 ' ||
--              'WHERE NOT EXISTS (SELECT 1 FROM ' || P_TABLE1 || ' t1 WHERE ' || v_pk_join || ')';
--     EXECUTE IMMEDIATE v_sql;
--     GET DIAGNOSTICS P_ONLY_TABLE2 = ROW_COUNT;
    
--     -- 3. Comparer les valeurs pour les lignes qui existent des deux côtés
--     IF P_COLUMNS_TO_COMPARE = 'ALL' THEN
--         -- Obtenir toutes les colonnes
--         CALL GET_TABLE_COLUMNS(P_TABLE1);
        
--         -- Construire la comparaison pour toutes les colonnes
--         v_sql := 'SELECT COLUMN_NAME FROM TEMP_TABLE_COLUMNS ORDER BY ORDINAL_POSITION';
--         LET c1 CURSOR FOR EXECUTE IMMEDIATE v_sql;
--         OPEN c1;
--         i := 0;
--         LOOP
--             FETCH c1 INTO v_col;
--             IF NOT FOUND THEN BREAK; END IF;
            
--             IF i > 0 THEN v_col_compare := v_col_compare || ' OR '; END IF;
            
--             IF P_CASE_SENSITIVE THEN
--                 v_expr1 := 'COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__'')';
--                 v_expr2 := 'COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__'')';
--             ELSE
--                 v_expr1 := 'UPPER(COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__''))';
--                 v_expr2 := 'UPPER(COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__''))';
--             END IF;
            
--             -- Ajouter la tolérance numérique si applicable
--             IF P_NUMERIC_TOLERANCE > 0 THEN
--                 v_col_compare := v_col_compare || 
--                     '(TRY_CAST(t1.' || v_col || ' AS NUMBER) IS NOT NULL AND ' ||
--                     'ABS(COALESCE(t1.' || v_col || ', 0) - COALESCE(t2.' || v_col || ', 0)) > ' || 
--                     P_NUMERIC_TOLERANCE || ') OR ' ||
--                     '(TRY_CAST(t1.' || v_col || ' AS NUMBER) IS NULL AND ' || 
--                     v_expr1 || ' != ' || v_expr2 || ')';
--             ELSE
--                 v_col_compare := v_col_compare || '(' || v_expr1 || ' != ' || v_expr2 || ')';
--             END IF;
            
--             i := i + 1;
--         END LOOP;
--         CLOSE c1;
--     ELSE
--         -- Comparer seulement les colonnes spécifiées
--         v_col_list := SPLIT(P_COLUMNS_TO_COMPARE, ',');
--         FOR i IN 0 TO ARRAY_SIZE(v_col_list) - 1 DO
--             v_col := TRIM(v_col_list[i]);
--             IF i > 0 THEN v_col_compare := v_col_compare || ' OR '; END IF;
            
--             IF P_CASE_SENSITIVE THEN
--                 v_expr1 := 'COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__'')';
--                 v_expr2 := 'COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__'')';
--             ELSE
--                 v_expr1 := 'UPPER(COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__''))';
--                 v_expr2 := 'UPPER(COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__''))';
--             END IF;
            
--             v_col_compare := v_col_compare || '(' || v_expr1 || ' != ' || v_expr2 || ')';
--         END LOOP;
--     END IF;
    
--     -- Compter les différences de valeurs
--     v_sql := 'SELECT COUNT(*) FROM ' || P_TABLE1 || ' t1 ' ||
--              'JOIN ' || P_TABLE2 || ' t2 ON ' || v_pk_join || ' ' ||
--              'WHERE ' || v_col_compare;
--     EXECUTE IMMEDIATE v_sql INTO P_DIFF_VALUES;
    
--     -- Calculer les lignes correspondantes
--     v_sql := 'SELECT COUNT(*) FROM ' || P_TABLE1 || ' t1 ' ||
--              'JOIN ' || P_TABLE2 || ' t2 ON ' || v_pk_join;
--     EXECUTE IMMEDIATE v_sql INTO P_MATCHED_COUNT;
--     P_MATCHED_COUNT := P_MATCHED_COUNT - P_DIFF_VALUES;
    
--     RETURN 'Comparison with PK completed';
-- END;
-- $$;


CREATE OR REPLACE PROCEDURE SP_COMPARE_WITH_PRIMARY_KEY(
    P_COMPARISON_ID       VARCHAR,
    P_TABLE1              VARCHAR,
    P_TABLE2              VARCHAR,
    P_PRIMARY_KEY         VARCHAR,
    P_COLUMNS_TO_COMPARE  VARCHAR,
    P_NUMERIC_TOLERANCE   NUMBER,
    P_CASE_SENSITIVE      BOOLEAN
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$

DECLARE
    v_pk_cols ARRAY;
    v_col_list ARRAY;

    v_sql VARCHAR;

    v_pk_join VARCHAR DEFAULT '';
    v_pk_select VARCHAR DEFAULT '';
    v_col_compare VARCHAR DEFAULT '';

    i INTEGER;
    v_col VARCHAR;
    v_expr1 VARCHAR;
    v_expr2 VARCHAR;

    -- métriques (ex-OUTPUT)
    v_matched_count NUMBER DEFAULT 0;
    v_only_table1   NUMBER DEFAULT 0;
    v_only_table2   NUMBER DEFAULT 0;
    v_diff_values   NUMBER DEFAULT 0;

    res  RESULTSET;
    -- SUPPRIMER ces deux lignes :
    -- cur  CURSOR;
    -- cur2 CURSOR;
BEGIN
    v_pk_cols := SPLIT(P_PRIMARY_KEY, ',');

    FOR i IN 0 TO ARRAY_SIZE(v_pk_cols) - 1 DO
        v_col := TRIM(v_pk_cols[i]::STRING);

        IF (i > 0) THEN
            v_pk_join := v_pk_join || ' AND ';
            v_pk_select := v_pk_select || ' || ''|'' || ';
        END IF;

        v_pk_join := v_pk_join || 't1.' || v_col || ' = t2.' || v_col;
        v_pk_select := v_pk_select || 'COALESCE(t1.' || v_col || '::VARCHAR, '''')';
    END FOR;

    -- 1) ONLY TABLE1
    v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, PRIMARY_KEY_VALUE, ROW_DATA_TABLE1) ' ||
             'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE1'', ' ||
             v_pk_select || ', OBJECT_CONSTRUCT(*) ' ||
             'FROM ' || P_TABLE1 || ' t1 ' ||
             'WHERE NOT EXISTS (SELECT 1 FROM ' || P_TABLE2 || ' t2 WHERE ' || v_pk_join || ')';

    EXECUTE IMMEDIATE v_sql;
    v_only_table1 := SQLROWCOUNT;

    -- 2) ONLY TABLE2
    v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, PRIMARY_KEY_VALUE, ROW_DATA_TABLE2) ' ||
             'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE2'', ' ||
             REPLACE(v_pk_select, 't1.', 't2.') || ', OBJECT_CONSTRUCT(*) ' ||
             'FROM ' || P_TABLE2 || ' t2 ' ||
             'WHERE NOT EXISTS (SELECT 1 FROM ' || P_TABLE1 || ' t1 WHERE ' || v_pk_join || ')';

    EXECUTE IMMEDIATE v_sql;
    v_only_table2 := SQLROWCOUNT;

    -- 3) Construire la condition de diff colonnes
    IF (UPPER(P_COLUMNS_TO_COMPARE) = 'ALL') THEN
        CALL GET_TABLE_COLUMNS(:P_TABLE1);

        LET c_cols CURSOR FOR
            SELECT COLUMN_NAME FROM TEMP_TABLE_COLUMNS ORDER BY ORDINAL_POSITION;

        i := 0;
        FOR rec IN c_cols DO
            v_col := rec.COLUMN_NAME;

            IF (i > 0) THEN
                v_col_compare := v_col_compare || ' OR ';
            END IF;

            IF (P_CASE_SENSITIVE) THEN
                v_expr1 := 'COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__'')';
                v_expr2 := 'COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__'')';
            ELSE
                v_expr1 := 'UPPER(COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__''))';
                v_expr2 := 'UPPER(COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__''))';
            END IF;

            IF (P_NUMERIC_TOLERANCE > 0) THEN
                v_col_compare := v_col_compare ||
                    '(' ||
                      '(' ||
                        'TRY_CAST(t1.' || v_col || ' AS NUMBER) IS NOT NULL AND ' ||
                        'TRY_CAST(t2.' || v_col || ' AS NUMBER) IS NOT NULL AND ' ||
                        'ABS(COALESCE(TRY_CAST(t1.' || v_col || ' AS NUMBER), 0) - COALESCE(TRY_CAST(t2.' || v_col || ' AS NUMBER), 0)) > ' ||
                        P_NUMERIC_TOLERANCE ||
                      ')' ||
                      ' OR ' ||
                      '(' ||
                        '(TRY_CAST(t1.' || v_col || ' AS NUMBER) IS NULL OR TRY_CAST(t2.' || v_col || ' AS NUMBER) IS NULL) AND ' ||
                        v_expr1 || ' != ' || v_expr2 ||
                      ')' ||
                    ')';
            ELSE
                v_col_compare := v_col_compare || '(' || v_expr1 || ' != ' || v_expr2 || ')';
            END IF;

            i := i + 1;
        END FOR;

    ELSE
        v_col_list := SPLIT(P_COLUMNS_TO_COMPARE, ',');

        FOR i IN 0 TO ARRAY_SIZE(v_col_list) - 1 DO
            v_col := TRIM(v_col_list[i]::STRING);

            IF (i > 0) THEN
                v_col_compare := v_col_compare || ' OR ';
            END IF;

            IF (P_CASE_SENSITIVE) THEN
                v_expr1 := 'COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__'')';
                v_expr2 := 'COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__'')';
            ELSE
                v_expr1 := 'UPPER(COALESCE(t1.' || v_col || '::VARCHAR, ''__NULL__''))';
                v_expr2 := 'UPPER(COALESCE(t2.' || v_col || '::VARCHAR, ''__NULL__''))';
            END IF;

            v_col_compare := v_col_compare || '(' || v_expr1 || ' != ' || v_expr2 || ')';
        END FOR;
    END IF;

    -- Diff values
    v_sql := 'SELECT COUNT(*) AS CNT FROM ' || P_TABLE1 || ' t1 ' ||
             'JOIN ' || P_TABLE2 || ' t2 ON ' || v_pk_join || ' ' ||
             'WHERE ' || v_col_compare;

    res := (EXECUTE IMMEDIATE :v_sql);
    LET cur CURSOR FOR res;
    OPEN cur;
    FETCH cur INTO v_diff_values;
    CLOSE cur;

    -- Matched rows
    v_sql := 'SELECT COUNT(*) AS CNT FROM ' || P_TABLE1 || ' t1 ' ||
             'JOIN ' || P_TABLE2 || ' t2 ON ' || v_pk_join;

    res := (EXECUTE IMMEDIATE :v_sql);
    LET cur2 CURSOR FOR res;
    OPEN cur2;
    FETCH cur2 INTO v_matched_count;
    CLOSE cur2;

    v_matched_count := v_matched_count - v_diff_values;

    RETURN OBJECT_CONSTRUCT(
        'status', 'OK',
        'comparison_id', P_COMPARISON_ID,
        'matched_count', v_matched_count,
        'only_table1', v_only_table1,
        'only_table2', v_only_table2,
        'diff_values', v_diff_values
    );

EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT(
            'status', 'ERROR',
            'comparison_id', P_COMPARISON_ID,
            'message', SQLERRM
        );
END;
$$;

-- ============================================================================
-- PROCÉDURE : SP_COMPARE_WITHOUT_PRIMARY_KEY
-- ============================================================================
-- CREATE OR REPLACE PROCEDURE SP_COMPARE_WITHOUT_PRIMARY_KEY(
--     P_COMPARISON_ID VARCHAR,
--     P_TABLE1 VARCHAR,
--     P_TABLE2 VARCHAR,
--     P_COLUMNS_TO_COMPARE VARCHAR,
--     P_CASE_SENSITIVE BOOLEAN,
--     P_MATCHED_COUNT NUMBER OUTPUT,
--     P_ONLY_TABLE1 NUMBER OUTPUT,
--     P_ONLY_TABLE2 NUMBER OUTPUT
-- )
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     v_sql VARCHAR;
--     v_hash_expr VARCHAR;
-- BEGIN
--     -- Construire l'expression de hash selon les colonnes à comparer
--     IF P_COLUMNS_TO_COMPARE = 'ALL' THEN
--         v_hash_expr := 'SHA2(OBJECT_CONSTRUCT(*), 256)';
--     ELSE
--         -- Hash seulement les colonnes spécifiées
--         LET v_cols ARRAY := SPLIT(P_COLUMNS_TO_COMPARE, ',');
--         v_hash_expr := 'SHA2(OBJECT_CONSTRUCT(';
--         FOR i IN 0 TO ARRAY_SIZE(v_cols) - 1 DO
--             IF i > 0 THEN v_hash_expr := v_hash_expr || ', '; END IF;
--             v_hash_expr := v_hash_expr || '''' || TRIM(v_cols[i]) || ''', ' || TRIM(v_cols[i]);
--         END LOOP;
--         v_hash_expr := v_hash_expr || '), 256)';
--     END IF;
    
--     -- Créer des tables temporaires avec les hashes
--     v_sql := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_T1_HASHES AS ' ||
--              'SELECT ' || v_hash_expr || ' AS ROW_HASH, OBJECT_CONSTRUCT(*) AS ROW_DATA ' ||
--              'FROM ' || P_TABLE1;
--     EXECUTE IMMEDIATE v_sql;
    
--     v_sql := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_T2_HASHES AS ' ||
--              'SELECT ' || v_hash_expr || ' AS ROW_HASH, OBJECT_CONSTRUCT(*) AS ROW_DATA ' ||
--              'FROM ' || P_TABLE2;
--     EXECUTE IMMEDIATE v_sql;
    
--     -- 1. Compter les lignes identiques
--     v_sql := 'SELECT COUNT(*) FROM TEMP_T1_HASHES t1 ' ||
--              'WHERE EXISTS (SELECT 1 FROM TEMP_T2_HASHES t2 WHERE t1.ROW_HASH = t2.ROW_HASH)';
--     EXECUTE IMMEDIATE v_sql INTO P_MATCHED_COUNT;
    
--     -- 2. Lignes uniquement dans TABLE1
--     v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, ROW_HASH, ROW_DATA_TABLE1) ' ||
--              'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE1'', ROW_HASH, ROW_DATA ' ||
--              'FROM TEMP_T1_HASHES t1 ' ||
--              'WHERE NOT EXISTS (SELECT 1 FROM TEMP_T2_HASHES t2 WHERE t1.ROW_HASH = t2.ROW_HASH)';
--     EXECUTE IMMEDIATE v_sql;
--     GET DIAGNOSTICS P_ONLY_TABLE1 = ROW_COUNT;
    
--     -- 3. Lignes uniquement dans TABLE2
--     v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, ROW_HASH, ROW_DATA_TABLE2) ' ||
--              'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE2'', ROW_HASH, ROW_DATA ' ||
--              'FROM TEMP_T2_HASHES t2 ' ||
--              'WHERE NOT EXISTS (SELECT 1 FROM TEMP_T1_HASHES t1 WHERE t1.ROW_HASH = t2.ROW_HASH)';
--     EXECUTE IMMEDIATE v_sql;
--     GET DIAGNOSTICS P_ONLY_TABLE2 = ROW_COUNT;
    
--     -- Nettoyer les tables temporaires
--     DROP TABLE IF EXISTS TEMP_T1_HASHES;
--     DROP TABLE IF EXISTS TEMP_T2_HASHES;
    
--     RETURN 'Comparison without PK completed';
-- END;
-- $$;



CREATE OR REPLACE PROCEDURE SP_COMPARE_WITHOUT_PRIMARY_KEY(
    P_COMPARISON_ID       VARCHAR,
    P_TABLE1              VARCHAR,
    P_TABLE2              VARCHAR,
    P_COLUMNS_TO_COMPARE  VARCHAR,
    P_CASE_SENSITIVE      BOOLEAN,
    P_MATCHED_COUNT       OUT NUMBER,
    P_ONLY_TABLE1         OUT NUMBER,
    P_ONLY_TABLE2         OUT NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_sql       VARCHAR;
    v_hash_expr VARCHAR;
BEGIN
    -- Construire l'expression de hash selon les colonnes à comparer
    IF (P_COLUMNS_TO_COMPARE = 'ALL') THEN
        v_hash_expr := 'SHA2(OBJECT_CONSTRUCT(*), 256)';
    ELSE
        -- Hash seulement les colonnes spécifiées
        LET v_cols ARRAY := SPLIT(P_COLUMNS_TO_COMPARE, ',');              -- SPLIT retourne un ARRAY
        v_hash_expr := 'SHA2(OBJECT_CONSTRUCT(';
        FOR i IN 0 TO ARRAY_SIZE(v_cols) - 1 DO                            -- FOR ... DO ... END FOR
            IF (i > 0) THEN
                v_hash_expr := v_hash_expr || ', ';
            END IF;
            v_hash_expr := v_hash_expr ||
                           '''' || TRIM(v_cols[i]::STRING) || ''', ' || TRIM(v_cols[i]::STRING);
        END FOR;
        v_hash_expr := v_hash_expr || '), 256)';
    END IF;
    
    -- Créer des tables temporaires avec les hashes
    v_sql := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_T1_HASHES AS ' ||
             'SELECT ' || v_hash_expr || ' AS ROW_HASH, OBJECT_CONSTRUCT(*) AS ROW_DATA ' ||
             'FROM ' || P_TABLE1;
    EXECUTE IMMEDIATE v_sql;
    
    v_sql := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_T2_HASHES AS ' ||
             'SELECT ' || v_hash_expr || ' AS ROW_HASH, OBJECT_CONSTRUCT(*) AS ROW_DATA ' ||
             'FROM ' || P_TABLE2;
    EXECUTE IMMEDIATE v_sql;
    
    -- 1. Compter les lignes identiques (SELECT ... INTO, sans EXECUTE IMMEDIATE)
    SELECT COUNT(*)
    INTO :P_MATCHED_COUNT
    FROM TEMP_T1_HASHES t1
    WHERE EXISTS (SELECT 1 FROM TEMP_T2_HASHES t2 WHERE t1.ROW_HASH = t2.ROW_HASH);
    
    -- 2. Lignes uniquement dans TABLE1
    v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, ROW_HASH, ROW_DATA_TABLE1) ' ||
             'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE1'', ROW_HASH, ROW_DATA ' ||
             'FROM TEMP_T1_HASHES t1 ' ||
             'WHERE NOT EXISTS (SELECT 1 FROM TEMP_T2_HASHES t2 WHERE t1.ROW_HASH = t2.ROW_HASH)';
    EXECUTE IMMEDIATE v_sql;
    P_ONLY_TABLE1 := SQLROWCOUNT;                                          -- lignes affectées
    
    -- 3. Lignes uniquement dans TABLE2
    v_sql := 'INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, ROW_HASH, ROW_DATA_TABLE2) ' ||
             'SELECT ''' || P_COMPARISON_ID || ''', ''ONLY_TABLE2'', ROW_HASH, ROW_DATA ' ||
             'FROM TEMP_T2_HASHES t2 ' ||
             'WHERE NOT EXISTS (SELECT 1 FROM TEMP_T1_HASHES t1 WHERE t1.ROW_HASH = t2.ROW_HASH)';
    EXECUTE IMMEDIATE v_sql;
    P_ONLY_TABLE2 := SQLROWCOUNT;                                          -- lignes affectées
    
    -- Nettoyer les tables temporaires
    DROP TABLE IF EXISTS TEMP_T1_HASHES;
    DROP TABLE IF EXISTS TEMP_T2_HASHES;
    
    RETURN 'Comparison without PK completed';
END;
$$;

-- ============================================================================
-- PROCÉDURE : SP_SHOW_COMPARISON_REPORT
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_SHOW_COMPARISON_REPORT(
    P_COMPARISON_ID VARCHAR DEFAULT NULL
)
RETURNS TABLE (REPORT_LINE VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_comparison_id VARCHAR;
    v_rec OBJECT;
BEGIN
    -- Si pas d'ID fourni, prendre le plus récent
    IF (P_COMPARISON_ID IS NULL) THEN
        SELECT COMPARISON_ID INTO v_comparison_id
        FROM DIFF_RESULTS
        ORDER BY COMPARISON_TIME DESC
        LIMIT 1;
    ELSE
        v_comparison_id := P_COMPARISON_ID;
    END IF;
    
    -- Récupérer les informations
    SELECT OBJECT_CONSTRUCT(
        'TABLE1', TABLE1,
        'TABLE2', TABLE2,
        'TIME', TO_VARCHAR(COMPARISON_TIME, 'YYYY-MM-DD HH24:MI:SS'),
        'COUNT1', TABLE1_ROW_COUNT,
        'COUNT2', TABLE2_ROW_COUNT,
        'MATCHED', MATCHED_ROWS,
        'ONLY1', ONLY_IN_TABLE1,
        'ONLY2', ONLY_IN_TABLE2,
        'DIFF', DIFF_VALUES,
        'PCT', MATCH_PERCENTAGE,
        'IDENTICAL', IS_IDENTICAL,
        'HAS_PK', HAS_PRIMARY_KEY
    ) INTO v_rec
    FROM DIFF_RESULTS
    WHERE COMPARISON_ID = v_comparison_id;
    
    -- Générer le rapport
    LET res RESULTSET := (
        SELECT * FROM (
            SELECT 1 AS SORT_ORDER, 
                   '╔═══════════════════════════════════════════════════════════════════════════╗' AS REPORT_LINE
            UNION ALL
            SELECT 2, '║                        RAPPORT DE COMPARAISON                              ║'
            UNION ALL
            SELECT 3, '║ ' || RPAD('ID: ' || v_comparison_id, 75) || ' ║'
            UNION ALL
            SELECT 4, '║ ' || RPAD('Date: ' || v_rec:TIME, 75) || ' ║'
            UNION ALL
            SELECT 5, '╠═══════════════════════════════════════════════════════════════════════════╣'
            UNION ALL
            SELECT 6, '║ TABLE 1: ' || RPAD(v_rec:TABLE1::VARCHAR, 66) || ' ║'
            UNION ALL
            SELECT 7, '║ TABLE 2: ' || RPAD(v_rec:TABLE2::VARCHAR, 66) || ' ║'
            UNION ALL
            SELECT 8, '╠═══════════════════════════════════════════════════════════════════════════╣'
            UNION ALL
            SELECT 9, '║ Méthode: ' || RPAD(CASE WHEN v_rec:HAS_PK THEN 'Comparaison avec clé primaire' 
                                             ELSE 'Comparaison par hash (sans clé primaire)' END, 66) || ' ║'
            UNION ALL
            SELECT 10, '╠═══════════════════════════════════════════════════════════════════════════╣'
            UNION ALL
            SELECT 11, '║ STATISTIQUES:                                                              ║'
            UNION ALL
            SELECT 12, '║   - Lignes Table 1:        ' || LPAD(TO_VARCHAR(v_rec:COUNT1, '999,999,999'), 12) || 
                       '                                     ║'
            UNION ALL
            SELECT 13, '║   - Lignes Table 2:        ' || LPAD(TO_VARCHAR(v_rec:COUNT2, '999,999,999'), 12) || 
                       '                                     ║'
            UNION ALL
            SELECT 14, '║   - Lignes identiques:     ' || LPAD(TO_VARCHAR(v_rec:MATCHED, '999,999,999'), 12) || 
                       '                                     ║'
            UNION ALL
            SELECT 15, '║   - Uniquement Table 1:    ' || LPAD(TO_VARCHAR(v_rec:ONLY1, '999,999,999'), 12) || 
                       '                                     ║'
            UNION ALL
            SELECT 16, '║   - Uniquement Table 2:    ' || LPAD(TO_VARCHAR(v_rec:ONLY2, '999,999,999'), 12) || 
                       '                                     ║'
            UNION ALL
            SELECT 17, '║   - Valeurs différentes:   ' || LPAD(TO_VARCHAR(v_rec:DIFF, '999,999,999'), 12) || 
                       '                                     ║'
            UNION ALL
            SELECT 18, '╠═══════════════════════════════════════════════════════════════════════════╣'
            UNION ALL
            SELECT 19, '║ RÉSULTAT: ' || RPAD(CASE WHEN v_rec:IDENTICAL 
                                               THEN '✓ TABLES IDENTIQUES (' || v_rec:PCT || '% match)' 
                                               ELSE '✗ TABLES DIFFÉRENTES (' || v_rec:PCT || '% match)' END, 65) || ' ║'
            UNION ALL
            SELECT 20, '╚═══════════════════════════════════════════════════════════════════════════╝'
        )
        ORDER BY SORT_ORDER
    );
    
    RETURN TABLE(res);
END;
$$;

-- Message de confirmation
SELECT 'Setup completed for TEAM_DB.EXTERNAL' AS MESSAGE;