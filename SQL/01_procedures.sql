-- ============================================================================
-- DATA DIFF TOOL - STORED PROCEDURES
-- ============================================================================
-- Description: Procédures stockées pour l'outil de comparaison Data Diff
-- Version: 1.0
-- Date: 2025-01-09
-- ============================================================================

USE DATABASE TEAM_DB;
USE SCHEMA EXTERNAL;

-- ============================================================================
-- PROCÉDURE: SP_REGISTER_PROJECT
-- Description: Enregistre un nouveau projet dans l'outil
-- ============================================================================

-- CREATE OR REPLACE PROCEDURE SP_REGISTER_PROJECT(
--     P_PROJECT_ID VARCHAR,
--     P_PROJECT_NAME VARCHAR,
--     P_TARGET_DATABASE VARCHAR,
--     P_TARGET_SCHEMA VARCHAR,
--     P_DESCRIPTION VARCHAR DEFAULT NULL
-- )
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     v_result VARCHAR;
-- BEGIN
--     -- Vérifier si le projet existe déjà
--     IF EXISTS (SELECT 1 FROM CONFIG_PROJECTS WHERE PROJECT_ID = :P_PROJECT_ID) THEN
--         -- Mettre à jour le projet existant
--         UPDATE CONFIG_PROJECTS
--         SET PROJECT_NAME = :P_PROJECT_NAME,
--             TARGET_DATABASE = :P_TARGET_DATABASE,
--             TARGET_SCHEMA = :P_TARGET_SCHEMA,
--             DESCRIPTION = COALESCE(:P_DESCRIPTION, DESCRIPTION),
--             IS_ACTIVE = TRUE
--         WHERE PROJECT_ID = :P_PROJECT_ID;
        
--         v_result := 'Project updated: ' || :P_PROJECT_ID;
--     ELSE
--         -- Créer un nouveau projet
--         INSERT INTO CONFIG_PROJECTS (
--             PROJECT_ID, PROJECT_NAME, TARGET_DATABASE, TARGET_SCHEMA, DESCRIPTION
--         ) VALUES (
--             :P_PROJECT_ID, :P_PROJECT_NAME, :P_TARGET_DATABASE, :P_TARGET_SCHEMA, :P_DESCRIPTION
--         );
        
--         v_result := 'Project created: ' || :P_PROJECT_ID;
--     END IF;
    
--     RETURN v_result;
-- END;
-- $$;

CREATE OR REPLACE PROCEDURE SP_REGISTER_PROJECT(
    P_PROJECT_ID VARCHAR,
    P_PROJECT_NAME VARCHAR,
    P_TARGET_DATABASE VARCHAR,
    P_TARGET_SCHEMA VARCHAR,
    P_DESCRIPTION VARCHAR DEFAULT NULL
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_exists INTEGER;
    v_result VARCHAR;
BEGIN
    -- Vérifier l'existence du projet
    SELECT COUNT(*) INTO v_exists
    FROM CONFIG_PROJECTS
    WHERE PROJECT_ID = :P_PROJECT_ID;

    IF (v_exists > 0) THEN
        -- Mettre à jour le projet existant
        UPDATE CONFIG_PROJECTS
        SET PROJECT_NAME   = :P_PROJECT_NAME,
            TARGET_DATABASE = :P_TARGET_DATABASE,
            TARGET_SCHEMA   = :P_TARGET_SCHEMA,
            DESCRIPTION     = COALESCE(:P_DESCRIPTION, DESCRIPTION),
            IS_ACTIVE       = TRUE
        WHERE PROJECT_ID = :P_PROJECT_ID;

        v_result := 'Project updated: ' || :P_PROJECT_ID;
    ELSE
        -- Créer un nouveau projet
        INSERT INTO CONFIG_PROJECTS (
            PROJECT_ID, PROJECT_NAME, TARGET_DATABASE, TARGET_SCHEMA, DESCRIPTION, IS_ACTIVE
        ) VALUES (
            :P_PROJECT_ID, :P_PROJECT_NAME, :P_TARGET_DATABASE, :P_TARGET_SCHEMA, :P_DESCRIPTION, TRUE
        );

        v_result := 'Project created: ' || :P_PROJECT_ID;
    END IF;

    RETURN v_result;
END;
$$;


-- ============================================================================
-- PROCÉDURE: SP_REGISTER_TABLE
-- Description: Enregistre une table à comparer pour un projet
-- ============================================================================

CREATE OR REPLACE PROCEDURE SP_REGISTER_TABLE(
    P_PROJECT_ID VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_SNOW_TABLE VARCHAR,
    P_PRIMARY_KEY_COLS VARCHAR DEFAULT NULL,
    P_COMPARE_COLS VARCHAR DEFAULT 'ALL',
    P_PRIORITY VARCHAR DEFAULT 'MEDIUM',
    P_NUMERIC_TOLERANCE NUMBER DEFAULT 0
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_result VARCHAR;
    v_sas_table_name VARCHAR;
BEGIN
    -- Vérifier que le projet existe
    IF NOT EXISTS (SELECT 1 FROM CONFIG_PROJECTS WHERE PROJECT_ID = :P_PROJECT_ID) THEN
        RETURN 'Error: Project ' || :P_PROJECT_ID || ' does not exist';
    END IF;
    
    -- Utiliser le nom de table pour SAS si non spécifié
    v_sas_table_name := :P_TABLE_NAME;
    
    -- Vérifier si la configuration existe déjà
    IF EXISTS (SELECT 1 FROM CONFIG_TABLES 
               WHERE PROJECT_ID = :P_PROJECT_ID AND TABLE_NAME = :P_TABLE_NAME) THEN
        -- Mettre à jour la configuration existante
        UPDATE CONFIG_TABLES
        SET SNOW_TABLE = :P_SNOW_TABLE,
            PRIMARY_KEY_COLS = :P_PRIMARY_KEY_COLS,
            COMPARE_COLS = :P_COMPARE_COLS,
            PRIORITY = :P_PRIORITY,
            NUMERIC_TOLERANCE = :P_NUMERIC_TOLERANCE,
            IS_ACTIVE = TRUE
        WHERE PROJECT_ID = :P_PROJECT_ID AND TABLE_NAME = :P_TABLE_NAME;
        
        v_result := 'Table configuration updated: ' || :P_TABLE_NAME;
    ELSE
        -- Créer une nouvelle configuration
        INSERT INTO CONFIG_TABLES (
            PROJECT_ID, TABLE_NAME, SAS_TABLE_NAME, SNOW_TABLE, 
            PRIMARY_KEY_COLS, COMPARE_COLS, PRIORITY, NUMERIC_TOLERANCE
        ) VALUES (
            :P_PROJECT_ID, :P_TABLE_NAME, v_sas_table_name, :P_SNOW_TABLE,
            :P_PRIMARY_KEY_COLS, :P_COMPARE_COLS, :P_PRIORITY, :P_NUMERIC_TOLERANCE
        );
        
        v_result := 'Table configuration created: ' || :P_TABLE_NAME;
    END IF;
    
    RETURN v_result;
END;
$$;

-- ============================================================================
-- PROCÉDURE: SP_LOAD_SAS_CSV
-- Description: Charge un fichier CSV SAS dans la zone de staging
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_LOAD_SAS_CSV(
    P_PROJECT_ID VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_STAGE_PATH VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_load_id VARCHAR;
    v_row_count NUMBER;
    v_sql VARCHAR;
BEGIN
    -- Générer un ID de chargement unique
    v_load_id := :P_PROJECT_ID || '_' || :P_TABLE_NAME || '_' || 
                 TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
    
    -- Nettoyer les données précédentes pour cette table
    DELETE FROM STAGING.SAS_STAGING_DATA 
    WHERE PROJECT_ID = :P_PROJECT_ID AND TABLE_NAME = :P_TABLE_NAME;
    
    -- Charger les données du CSV dans la table de staging
    v_sql := 'COPY INTO STAGING.SAS_STAGING_DATA (LOAD_ID, PROJECT_ID, TABLE_NAME, ROW_DATA, ROW_HASH) ' ||
             'FROM ( ' ||
             '  SELECT ''' || v_load_id || ''', ' ||
             '         ''' || :P_PROJECT_ID || ''', ' ||
             '         ''' || :P_TABLE_NAME || ''', ' ||
             '         OBJECT_CONSTRUCT(*) AS ROW_DATA, ' ||
             '         SHA2(OBJECT_CONSTRUCT(*), 256) AS ROW_HASH ' ||
             '  FROM ' || :P_STAGE_PATH || ' ' ||
             ') ' ||
             'FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = ''"'' ' ||
             'NULL_IF = ('''', ''NULL'', ''.'', ''null'') EMPTY_FIELD_AS_NULL = TRUE)';
    
    EXECUTE IMMEDIATE v_sql;
    
    -- Compter les lignes chargées
    SELECT COUNT(*) INTO v_row_count
    FROM STAGING.SAS_STAGING_DATA
    WHERE LOAD_ID = v_load_id;
    
    RETURN 'Loaded ' || v_row_count || ' rows for table ' || :P_TABLE_NAME;
END;
$$;

-- ============================================================================
-- PROCÉDURE: SP_COMPARE_TABLE
-- Description: Compare une table spécifique entre SAS et Snowflake
-- ============================================================================
-- CREATE OR REPLACE PROCEDURE SP_COMPARE_TABLE(
--     P_RUN_ID VARCHAR,
--     P_PROJECT_ID VARCHAR,
--     P_TABLE_NAME VARCHAR
-- )
-- RETURNS VARCHAR
-- LANGUAGE SQL
-- EXECUTE AS CALLER
-- AS
-- $$
-- DECLARE
--     v_config_rec OBJECT;
--     v_snow_table VARCHAR;
--     v_pk_cols VARCHAR;
--     v_compare_cols VARCHAR;
--     v_has_pk BOOLEAN;
--     v_sas_count NUMBER;
--     v_snow_count NUMBER;
--     v_matched_count NUMBER;
--     v_only_sas NUMBER;
--     v_only_snow NUMBER;
--     v_diff_values NUMBER;
--     v_match_pct NUMBER(5,2);
--     v_status VARCHAR;
--     v_start_time TIMESTAMP_NTZ;
--     v_exec_time NUMBER;
--     v_sql VARCHAR;
--     v_error_msg VARCHAR;
-- BEGIN
--     v_start_time := CURRENT_TIMESTAMP();
    
--     -- Récupérer la configuration de la table
--     SELECT OBJECT_CONSTRUCT(
--         'SNOW_TABLE', SNOW_TABLE,
--         'PRIMARY_KEY_COLS', PRIMARY_KEY_COLS,
--         'COMPARE_COLS', COMPARE_COLS,
--         'NUMERIC_TOLERANCE', NUMERIC_TOLERANCE
--     ) INTO v_config_rec
--     FROM CONFIG_TABLES
--     WHERE PROJECT_ID = :P_PROJECT_ID 
--       AND TABLE_NAME = :P_TABLE_NAME
--       AND IS_ACTIVE = TRUE;
    
--     IF (v_config_rec IS NULL) THEN
--         v_error_msg := 'Table configuration not found: ' || :P_TABLE_NAME;
--         INSERT INTO RESULTS.COMPARISON_RESULTS (
--             RUN_ID, PROJECT_ID, TABLE_NAME, STATUS, ERROR_MESSAGE
--         ) VALUES (
--             :P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, 'ERROR', v_error_msg
--         );
--         RETURN v_error_msg;
--     END IF;
    
--     v_snow_table := v_config_rec:SNOW_TABLE;
--     v_pk_cols := v_config_rec:PRIMARY_KEY_COLS;
--     v_compare_cols := v_config_rec:COMPARE_COLS;
--     v_has_pk := (v_pk_cols IS NOT NULL AND v_pk_cols != '');
    
--     -- Compter les lignes SAS
--     SELECT COUNT(*) INTO v_sas_count
--     FROM STAGING.SAS_STAGING_DATA
--     WHERE PROJECT_ID = :P_PROJECT_ID AND TABLE_NAME = :P_TABLE_NAME;
    
--     -- Compter les lignes Snowflake
--     v_sql := 'SELECT COUNT(*) FROM ' || v_snow_table;
--     EXECUTE IMMEDIATE v_sql INTO v_snow_count;
    
--     IF v_has_pk THEN
--         -- Comparaison avec clé primaire
--         CALL SP_COMPARE_WITH_PK(:P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, 
--                                v_snow_table, v_pk_cols, v_compare_cols,
--                                v_matched_count, v_only_sas, v_only_snow, v_diff_values);
--     ELSE
--         -- Comparaison sans clé primaire (basée sur le hash de la ligne complète)
--         CALL SP_COMPARE_WITHOUT_PK(:P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, 
--                                   v_snow_table, v_compare_cols,
--                                   v_matched_count, v_only_sas, v_only_snow, v_diff_values);
--     END IF;
    
--     -- Calculer le pourcentage de correspondance
--     IF v_sas_count + v_snow_count > 0 THEN
--         v_match_pct := ROUND(200.0 * v_matched_count / (v_sas_count + v_snow_count), 2);
--     ELSE
--         v_match_pct := 100.0;
--     END IF;
    
--     -- Déterminer le statut
--     IF v_sas_count = v_snow_count AND v_only_sas = 0 AND v_only_snow = 0 AND v_diff_values = 0 THEN
--         v_status := 'IDENTICAL';
--     ELSE
--         v_status := 'DIFFERENT';
--     END IF;
    
--     -- Calculer le temps d'exécution
--     v_exec_time := DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP());
    
--     -- Enregistrer les résultats
--     INSERT INTO RESULTS.COMPARISON_RESULTS (
--         RUN_ID, PROJECT_ID, TABLE_NAME, SAS_ROW_COUNT, SNOW_ROW_COUNT,
--         MATCHED_ROWS, ONLY_IN_SAS, ONLY_IN_SNOW, DIFF_VALUES,
--         MATCH_PERCENTAGE, STATUS, EXECUTION_TIME_SEC, HAS_PRIMARY_KEY
--     ) VALUES (
--         :P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, v_sas_count, v_snow_count,
--         v_matched_count, v_only_sas, v_only_snow, v_diff_values,
--         v_match_pct, v_status, v_exec_time, v_has_pk
--     );
    
--     RETURN 'Table ' || :P_TABLE_NAME || ' comparison completed: ' || v_status;
-- EXCEPTION
--     WHEN OTHER THEN
--         v_error_msg := 'Error comparing table ' || :P_TABLE_NAME || ': ' || SQLERRM;
--         INSERT INTO RESULTS.COMPARISON_RESULTS (
--             RUN_ID, PROJECT_ID, TABLE_NAME, STATUS, ERROR_MESSAGE
--         ) VALUES (
--             :P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, 'ERROR', v_error_msg
--         );
--         RETURN v_error_msg;
-- END;
-- $$;


CREATE OR REPLACE PROCEDURE SP_COMPARE_TABLE(
    P_RUN_ID      VARCHAR,
    P_PROJECT_ID  VARCHAR,
    P_TABLE_NAME  VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_config_rec   OBJECT;
    v_snow_table   VARCHAR;
    v_pk_cols      VARCHAR;
    v_compare_cols VARCHAR;
    v_has_pk       BOOLEAN;

    v_sas_count     NUMBER;
    v_snow_count    NUMBER;

    v_matched_count NUMBER;
    v_only_sas      NUMBER;
    v_only_snow     NUMBER;
    v_diff_values   NUMBER;

    v_match_pct   NUMBER(5,2);
    v_status      VARCHAR;
    v_start_time  TIMESTAMP_NTZ;
    v_exec_time   NUMBER;
    v_error_msg   VARCHAR;
BEGIN
    v_start_time := CURRENT_TIMESTAMP();

    -- 1) Récupérer la configuration de la table
    SELECT OBJECT_CONSTRUCT(
        'SNOW_TABLE',        SNOW_TABLE,
        'PRIMARY_KEY_COLS',  PRIMARY_KEY_COLS,
        'COMPARE_COLS',      COMPARE_COLS,
        'NUMERIC_TOLERANCE', NUMERIC_TOLERANCE
    )
    INTO v_config_rec
    FROM CONFIG_TABLES
    WHERE PROJECT_ID = :P_PROJECT_ID
      AND TABLE_NAME = :P_TABLE_NAME
      AND IS_ACTIVE = TRUE;

    IF (v_config_rec IS NULL) THEN
        v_error_msg := 'Table configuration not found: ' || :P_TABLE_NAME;
        INSERT INTO RESULTS.COMPARISON_RESULTS (
            RUN_ID, PROJECT_ID, TABLE_NAME, STATUS, ERROR_MESSAGE
        )
        VALUES (
            :P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, 'ERROR', :v_error_msg
        );
        RETURN v_error_msg;
    END IF;

    -- 2) Extraire les champs de l'OBJECT (avec casts)
    v_snow_table   := v_config_rec['SNOW_TABLE']::STRING;
    v_pk_cols      := v_config_rec['PRIMARY_KEY_COLS']::STRING;
    v_compare_cols := v_config_rec['COMPARE_COLS']::STRING;
    v_has_pk := (v_pk_cols IS NOT NULL AND LENGTH(TRIM(v_pk_cols)) > 0);

    -- 3) Compter les lignes SAS
    SELECT COUNT(*)
    INTO v_sas_count
    FROM STAGING.SAS_STAGING_DATA
    WHERE PROJECT_ID = :P_PROJECT_ID
      AND TABLE_NAME = :P_TABLE_NAME;

    -- 4) Compter les lignes côté Snowflake sans EXECUTE IMMEDIATE
    SELECT COUNT(*)
    INTO v_snow_count
    FROM IDENTIFIER(:v_snow_table);

    -- 5) Table temporaire pour récupérer les métriques écrites par les sous-procédures
    CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_COMPARISON_METRICS (
        RUN_ID        VARCHAR,
        PROJECT_ID    VARCHAR,
        TABLE_NAME    VARCHAR,
        MATCHED_COUNT NUMBER,
        ONLY_SAS      NUMBER,
        ONLY_SNOW     NUMBER,
        DIFF_VALUES   NUMBER
    );

    -- Nettoyage des éventuels restes pour ce run/table
    DELETE FROM TEMP_COMPARISON_METRICS
     WHERE RUN_ID = :P_RUN_ID
       AND PROJECT_ID = :P_PROJECT_ID
       AND TABLE_NAME = :P_TABLE_NAME;

    -- 6) Lancer la comparaison via les sous-procédures
    -- Hypothèse: elles écrivent UNE ligne dans TEMP_COMPARISON_METRICS
    --            (RUN_ID, PROJECT_ID, TABLE_NAME, MATCHED_COUNT, ONLY_SAS, ONLY_SNOW, DIFF_VALUES)
    IF v_has_pk THEN
        CALL SP_COMPARE_WITH_PK(:P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME,
                                :v_snow_table, :v_pk_cols, :v_compare_cols);
    ELSE
        CALL SP_COMPARE_WITHOUT_PK(:P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME,
                                   :v_snow_table, :v_compare_cols);
    END IF;

    -- 7) Récupérer les métriques ; si aucune ligne n'est écrite, on force à 0
    SELECT
        COALESCE(MAX(MATCHED_COUNT), 0),
        COALESCE(MAX(ONLY_SAS), 0),
        COALESCE(MAX(ONLY_SNOW), 0),
        COALESCE(MAX(DIFF_VALUES), 0)
    INTO v_matched_count, v_only_sas, v_only_snow, v_diff_values
    FROM TEMP_COMPARISON_METRICS
    WHERE RUN_ID = :P_RUN_ID
      AND PROJECT_ID = :P_PROJECT_ID
      AND TABLE_NAME = :P_TABLE_NAME;

    -- 8) Calculer le pourcentage de correspondance
    IF (v_sas_count + v_snow_count) > 0 THEN
        -- Formule symétrique: 2 * matched / (sas + snow)
        v_match_pct := ROUND(200.0 * v_matched_count / (v_sas_count + v_snow_count), 2);
    ELSE
        v_match_pct := 100.0;
    END IF;

    -- 9) Déterminer le statut
    IF v_sas_count = v_snow_count
       AND v_only_sas = 0
       AND v_only_snow = 0
       AND v_diff_values = 0 THEN
        v_status := 'IDENTICAL';
    ELSE
        v_status := 'DIFFERENT';
    END IF;

    -- 10) Calculer le temps d'exécution
    v_exec_time := DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP());

    -- 11) Enregistrer les résultats
    INSERT INTO RESULTS.COMPARISON_RESULTS (
        RUN_ID, PROJECT_ID, TABLE_NAME, SAS_ROW_COUNT, SNOW_ROW_COUNT,
        MATCHED_ROWS, ONLY_IN_SAS, ONLY_IN_SNOW, DIFF_VALUES,
        MATCH_PERCENTAGE, STATUS, EXECUTION_TIME_SEC, HAS_PRIMARY_KEY
    )
    VALUES (
        :P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, :v_sas_count, :v_snow_count,
        :v_matched_count, :v_only_sas, :v_only_snow, :v_diff_values,
        :v_match_pct, :v_status, :v_exec_time, :v_has_pk
    );

    RETURN 'Table ' || :P_TABLE_NAME || ' comparison completed: ' || v_status;

EXCEPTION
    WHEN OTHER THEN
        v_error_msg := 'Error comparing table ' || :P_TABLE_NAME || ': ' || SQLERRM;
        INSERT INTO RESULTS.COMPARISON_RESULTS (
            RUN_ID, PROJECT_ID, TABLE_NAME, STATUS, ERROR_MESSAGE
        )
        VALUES (
            :P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, 'ERROR', :v_error_msg
        );
        RETURN v_error_msg;
END;
$$;



-- ============================================================================
-- PROCÉDURE: SP_COMPARE_WITH_PK
-- Description: Compare les tables ayant une clé primaire
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_COMPARE_WITH_PK(
    P_RUN_ID VARCHAR,
    P_PROJECT_ID VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_SNOW_TABLE VARCHAR,
    P_PK_COLS VARCHAR,
    P_COMPARE_COLS VARCHAR,
    P_MATCHED_COUNT NUMBER OUTPUT,
    P_ONLY_SAS NUMBER OUTPUT,
    P_ONLY_SNOW NUMBER OUTPUT,
    P_DIFF_VALUES NUMBER OUTPUT
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_pk_list ARRAY;
    v_col_list ARRAY;
    v_sql VARCHAR;
    v_pk_select VARCHAR;
    v_pk_join VARCHAR;
    v_col_compare VARCHAR;
    i INTEGER;
    v_col VARCHAR;
    v_sas_expr VARCHAR;
    v_snow_expr VARCHAR;
BEGIN
    -- Parser les colonnes de clé primaire
    v_pk_list := SPLIT(:P_PK_COLS, ',');
    
    -- Construire la sélection et jointure sur PK
    v_pk_select := '';
    v_pk_join := '';
    FOR i IN 0 TO ARRAY_SIZE(v_pk_list) - 1 DO
        v_col := TRIM(v_pk_list[i]);
        IF i > 0 THEN
            v_pk_select := v_pk_select || ' || ''|'' || ';
            v_pk_join := v_pk_join || ' AND ';
        END IF;
        v_pk_select := v_pk_select || 'COALESCE(s.ROW_DATA:' || v_col || '::VARCHAR, '''')';
        v_pk_join := v_pk_join || 's.ROW_DATA:' || v_col || ' = n.' || v_col;
    END LOOP;
    
    -- Créer une table temporaire pour les données Snowflake
    v_sql := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_SNOW_DATA AS ' ||
             'SELECT *, OBJECT_CONSTRUCT(*) AS ROW_DATA FROM ' || :P_SNOW_TABLE;
    EXECUTE IMMEDIATE v_sql;
    
    -- 1. Compter les lignes qui existent uniquement dans SAS
    v_sql := 'INSERT INTO RESULTS.COMPARISON_DIFF_DETAILS ' ||
             '(RUN_ID, PROJECT_ID, TABLE_NAME, DIFF_TYPE, PRIMARY_KEY_VALUE, SAS_ROW_DATA) ' ||
             'SELECT ''' || :P_RUN_ID || ''', ' ||
             '       ''' || :P_PROJECT_ID || ''', ' ||
             '       ''' || :P_TABLE_NAME || ''', ' ||
             '       ''ONLY_SAS'', ' ||
             '       ' || v_pk_select || ', ' ||
             '       s.ROW_DATA ' ||
             'FROM STAGING.SAS_STAGING_DATA s ' ||
             'WHERE s.PROJECT_ID = ''' || :P_PROJECT_ID || ''' ' ||
             '  AND s.TABLE_NAME = ''' || :P_TABLE_NAME || ''' ' ||
             '  AND NOT EXISTS ( ' ||
             '    SELECT 1 FROM TEMP_SNOW_DATA n ' ||
             '    WHERE ' || v_pk_join || ' ' ||
             '  )';
    EXECUTE IMMEDIATE v_sql;
    GET DIAGNOSTICS :P_ONLY_SAS = ROW_COUNT;
    
    -- 2. Compter les lignes qui existent uniquement dans Snowflake
    v_sql := 'INSERT INTO RESULTS.COMPARISON_DIFF_DETAILS ' ||
             '(RUN_ID, PROJECT_ID, TABLE_NAME, DIFF_TYPE, PRIMARY_KEY_VALUE, SNOW_ROW_DATA) ' ||
             'SELECT ''' || :P_RUN_ID || ''', ' ||
             '       ''' || :P_PROJECT_ID || ''', ' ||
             '       ''' || :P_TABLE_NAME || ''', ' ||
             '       ''ONLY_SNOW'', ' ||
             '       ' || REPLACE(REPLACE(v_pk_select, 's.ROW_DATA:', 'n.ROW_DATA:'), 's.', 'n.') || ', ' ||
             '       n.ROW_DATA ' ||
             'FROM TEMP_SNOW_DATA n ' ||
             'WHERE NOT EXISTS ( ' ||
             '    SELECT 1 FROM STAGING.SAS_STAGING_DATA s ' ||
             '    WHERE s.PROJECT_ID = ''' || :P_PROJECT_ID || ''' ' ||
             '      AND s.TABLE_NAME = ''' || :P_TABLE_NAME || ''' ' ||
             '      AND ' || v_pk_join || ' ' ||
             '  )';
    EXECUTE IMMEDIATE v_sql;
    GET DIAGNOSTICS :P_ONLY_SNOW = ROW_COUNT;
    
    -- 3. Comparer les valeurs pour les lignes qui existent des deux côtés
    IF :P_COMPARE_COLS = 'ALL' THEN
        -- Comparer toutes les colonnes via le hash
        v_sql := 'SELECT COUNT(*) FROM ( ' ||
                 '  SELECT s.ROW_HASH ' ||
                 '  FROM STAGING.SAS_STAGING_DATA s ' ||
                 '  JOIN TEMP_SNOW_DATA n ON ' || v_pk_join || ' ' ||
                 '  WHERE s.PROJECT_ID = ''' || :P_PROJECT_ID || ''' ' ||
                 '    AND s.TABLE_NAME = ''' || :P_TABLE_NAME || ''' ' ||
                 '    AND s.ROW_HASH != SHA2(n.ROW_DATA, 256) ' ||
                 ')';
        EXECUTE IMMEDIATE v_sql INTO :P_DIFF_VALUES;
    ELSE
        -- Comparer les colonnes spécifiées
        v_col_list := SPLIT(:P_COMPARE_COLS, ',');
        v_col_compare := '';
        
        FOR i IN 0 TO ARRAY_SIZE(v_col_list) - 1 DO
            v_col := TRIM(v_col_list[i]);
            IF i > 0 THEN
                v_col_compare := v_col_compare || ' OR ';
            END IF;
            -- Gestion des NULL et normalisation
            v_sas_expr := 'COALESCE(NULLIF(NULLIF(s.ROW_DATA:' || v_col || '::VARCHAR, ''''), ''.''), ''__NULL__'')';
            v_snow_expr := 'COALESCE(n.' || v_col || '::VARCHAR, ''__NULL__'')';
            v_col_compare := v_col_compare || '(' || v_sas_expr || ' != ' || v_snow_expr || ')';
        END LOOP;
        
        v_sql := 'SELECT COUNT(*) FROM ( ' ||
                 '  SELECT 1 ' ||
                 '  FROM STAGING.SAS_STAGING_DATA s ' ||
                 '  JOIN TEMP_SNOW_DATA n ON ' || v_pk_join || ' ' ||
                 '  WHERE s.PROJECT_ID = ''' || :P_PROJECT_ID || ''' ' ||
                 '    AND s.TABLE_NAME = ''' || :P_TABLE_NAME || ''' ' ||
                 '    AND (' || v_col_compare || ') ' ||
                 ')';
        EXECUTE IMMEDIATE v_sql INTO :P_DIFF_VALUES;
    END IF;
    
    -- 4. Calculer les lignes correspondantes
    SELECT COUNT(*) INTO :P_MATCHED_COUNT
    FROM STAGING.SAS_STAGING_DATA s
    JOIN TEMP_SNOW_DATA n ON CASE WHEN v_pk_join != '' THEN TRUE ELSE FALSE END
    WHERE s.PROJECT_ID = :P_PROJECT_ID 
      AND s.TABLE_NAME = :P_TABLE_NAME;
    
    :P_MATCHED_COUNT := :P_MATCHED_COUNT - :P_DIFF_VALUES;
    
    -- Nettoyer la table temporaire
    DROP TABLE IF EXISTS TEMP_SNOW_DATA;
    
    RETURN 'Comparison with PK completed';
END;
$$;

-- ============================================================================
-- PROCÉDURE: SP_COMPARE_WITHOUT_PK
-- Description: Compare les tables sans clé primaire (basé sur hash complet)
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_COMPARE_WITHOUT_PK(
    P_RUN_ID VARCHAR,
    P_PROJECT_ID VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_SNOW_TABLE VARCHAR,
    P_COMPARE_COLS VARCHAR,
    P_MATCHED_COUNT NUMBER OUTPUT,
    P_ONLY_SAS NUMBER OUTPUT,
    P_ONLY_SNOW NUMBER OUTPUT,
    P_DIFF_VALUES NUMBER OUTPUT
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_sql VARCHAR;
BEGIN
    -- Pour les tables sans PK, on utilise le hash de la ligne complète
    
    -- Créer une table temporaire avec les hashes Snowflake
    v_sql := 'CREATE OR REPLACE TEMPORARY TABLE TEMP_SNOW_HASHES AS ' ||
             'SELECT SHA2(OBJECT_CONSTRUCT(*), 256) AS ROW_HASH, ' ||
             '       OBJECT_CONSTRUCT(*) AS ROW_DATA ' ||
             'FROM ' || :P_SNOW_TABLE;
    EXECUTE IMMEDIATE v_sql;
    
    -- 1. Lignes identiques (même hash)
    v_sql := 'SELECT COUNT(*) FROM ( ' ||
             '  SELECT s.ROW_HASH ' ||
             '  FROM STAGING.SAS_STAGING_DATA s ' ||
             '  WHERE s.PROJECT_ID = ''' || :P_PROJECT_ID || ''' ' ||
             '    AND s.TABLE_NAME = ''' || :P_TABLE_NAME || ''' ' ||
             '    AND EXISTS ( ' ||
             '      SELECT 1 FROM TEMP_SNOW_HASHES n ' ||
             '      WHERE n.ROW_HASH = s.ROW_HASH ' ||
             '    ) ' ||
             ')';
    EXECUTE IMMEDIATE v_sql INTO :P_MATCHED_COUNT;
    
    -- 2. Lignes uniquement dans SAS
    v_sql := 'INSERT INTO RESULTS.COMPARISON_DIFF_DETAILS ' ||
             '(RUN_ID, PROJECT_ID, TABLE_NAME, DIFF_TYPE, ROW_IDENTIFIER, SAS_ROW_DATA) ' ||
             'SELECT ''' || :P_RUN_ID || ''', ' ||
             '       ''' || :P_PROJECT_ID || ''', ' ||
             '       ''' || :P_TABLE_NAME || ''', ' ||
             '       ''ONLY_SAS'', ' ||
             '       s.ROW_HASH, ' ||
             '       s.ROW_DATA ' ||
             'FROM STAGING.SAS_STAGING_DATA s ' ||
             'WHERE s.PROJECT_ID = ''' || :P_PROJECT_ID || ''' ' ||
             '  AND s.TABLE_NAME = ''' || :P_TABLE_NAME || ''' ' ||
             '  AND NOT EXISTS ( ' ||
             '    SELECT 1 FROM TEMP_SNOW_HASHES n ' ||
             '    WHERE n.ROW_HASH = s.ROW_HASH ' ||
             '  )';
    EXECUTE IMMEDIATE v_sql;
    GET DIAGNOSTICS :P_ONLY_SAS = ROW_COUNT;
    
    -- 3. Lignes uniquement dans Snowflake
    v_sql := 'INSERT INTO RESULTS.COMPARISON_DIFF_DETAILS ' ||
             '(RUN_ID, PROJECT_ID, TABLE_NAME, DIFF_TYPE, ROW_IDENTIFIER, SNOW_ROW_DATA) ' ||
             'SELECT ''' || :P_RUN_ID || ''', ' ||
             '       ''' || :P_PROJECT_ID || ''', ' ||
             '       ''' || :P_TABLE_NAME || ''', ' ||
             '       ''ONLY_SNOW'', ' ||
             '       n.ROW_HASH, ' ||
             '       n.ROW_DATA ' ||
             'FROM TEMP_SNOW_HASHES n ' ||
             'WHERE NOT EXISTS ( ' ||
             '    SELECT 1 FROM STAGING.SAS_STAGING_DATA s ' ||
             '    WHERE s.PROJECT_ID = ''' || :P_PROJECT_ID || ''' ' ||
             '      AND s.TABLE_NAME = ''' || :P_TABLE_NAME || ''' ' ||
             '      AND s.ROW_HASH = n.ROW_HASH ' ||
             '  )';
    EXECUTE IMMEDIATE v_sql;
    GET DIAGNOSTICS :P_ONLY_SNOW = ROW_COUNT;
    
    -- 4. Pas de différences de valeurs car on compare par hash complet
    :P_DIFF_VALUES := 0;
    
    -- Nettoyer la table temporaire
    DROP TABLE IF EXISTS TEMP_SNOW_HASHES;
    
    RETURN 'Comparison without PK completed';
END;
$$;

-- ============================================================================
-- PROCÉDURE: SP_RUN_COMPARISON
-- Description: Lance la comparaison complète pour un projet
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_RUN_COMPARISON(
    P_PROJECT_ID VARCHAR,
    P_RUN_TYPE VARCHAR DEFAULT 'FULL'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_table_count NUMBER;
    v_tables_processed NUMBER := 0;
    v_tables_matched NUMBER := 0;
    v_tables_different NUMBER := 0;
    v_tables_error NUMBER := 0;
    v_cursor CURSOR FOR 
        SELECT TABLE_NAME, PRIORITY
        FROM CONFIG_TABLES
        WHERE PROJECT_ID = :P_PROJECT_ID AND IS_ACTIVE = TRUE
        ORDER BY 
            CASE PRIORITY 
                WHEN 'HIGH' THEN 1 
                WHEN 'MEDIUM' THEN 2 
                ELSE 3 
            END,
            TABLE_NAME;
    v_table_name VARCHAR;
    v_priority VARCHAR;
    v_result VARCHAR;
    v_status VARCHAR;
BEGIN
    -- Générer un ID unique pour ce run
    v_run_id := UUID_STRING();
    
    -- Compter le nombre de tables à traiter
    SELECT COUNT(*) INTO v_table_count
    FROM CONFIG_TABLES
    WHERE PROJECT_ID = :P_PROJECT_ID AND IS_ACTIVE = TRUE;
    
    IF v_table_count = 0 THEN
        RETURN 'Error: No tables configured for project ' || :P_PROJECT_ID;
    END IF;
    
    -- Créer l'enregistrement du run
    INSERT INTO RESULTS.COMPARISON_RUNS (
        RUN_ID, PROJECT_ID, RUN_TYPE, TOTAL_TABLES
    ) VALUES (
        v_run_id, :P_PROJECT_ID, :P_RUN_TYPE, v_table_count
    );
    
    -- Logger le début
    INSERT INTO RESULTS.EXECUTION_LOGS (RUN_ID, PROCEDURE_NAME, LOG_LEVEL, LOG_MESSAGE)
    VALUES (v_run_id, 'SP_RUN_COMPARISON', 'INFO', 
            'Starting comparison for project ' || :P_PROJECT_ID || ' with ' || v_table_count || ' tables');
    
    -- Traiter chaque table
    OPEN v_cursor;
    LOOP
        FETCH v_cursor INTO v_table_name, v_priority;
        IF NOT FOUND THEN
            BREAK;
        END IF;
        
        -- Logger le traitement de la table
        INSERT INTO RESULTS.EXECUTION_LOGS (RUN_ID, PROCEDURE_NAME, LOG_LEVEL, LOG_MESSAGE)
        VALUES (v_run_id, 'SP_RUN_COMPARISON', 'INFO', 
                'Processing table ' || v_table_name || ' (Priority: ' || v_priority || ')');
        
        -- Comparer la table
        CALL SP_COMPARE_TABLE(v_run_id, :P_PROJECT_ID, v_table_name);
        
        -- Récupérer le statut
        SELECT STATUS INTO v_status
        FROM RESULTS.COMPARISON_RESULTS
        WHERE RUN_ID = v_run_id AND TABLE_NAME = v_table_name;
        
        -- Mettre à jour les compteurs
        v_tables_processed := v_tables_processed + 1;
        CASE v_status
            WHEN 'IDENTICAL' THEN v_tables_matched := v_tables_matched + 1;
            WHEN 'DIFFERENT' THEN v_tables_different := v_tables_different + 1;
            WHEN 'ERROR' THEN v_tables_error := v_tables_error + 1;
        END CASE;
    END LOOP;
    CLOSE v_cursor;
    
    -- Mettre à jour le run avec les résultats finaux
    UPDATE RESULTS.COMPARISON_RUNS
    SET END_TIME = CURRENT_TIMESTAMP(),
        TABLES_MATCHED = v_tables_matched,
        TABLES_DIFFERENT = v_tables_different,
        TABLES_ERROR = v_tables_error,
        STATUS = CASE 
            WHEN v_tables_error > 0 THEN 'ERROR'
            WHEN v_tables_different = 0 THEN 'SUCCESS'
            ELSE 'DIFFERENCES_FOUND'
        END
    WHERE RUN_ID = v_run_id;
    
    -- Logger la fin
    INSERT INTO RESULTS.EXECUTION_LOGS (RUN_ID, PROCEDURE_NAME, LOG_LEVEL, LOG_MESSAGE)
    VALUES (v_run_id, 'SP_RUN_COMPARISON', 'INFO', 
            'Comparison completed. Matched: ' || v_tables_matched || 
            ', Different: ' || v_tables_different || 
            ', Errors: ' || v_tables_error);
    
    RETURN 'Comparison completed. Run ID: ' || v_run_id || 
           '. Matched: ' || v_tables_matched || 
           '/' || v_table_count || ' tables';
END;
$$;

-- ============================================================================
-- PROCÉDURE: SP_SHOW_REPORT
-- Description: Affiche un rapport formaté des résultats
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_SHOW_REPORT(
    P_RUN_ID VARCHAR DEFAULT NULL
)
RETURNS TABLE (REPORT_LINE VARCHAR)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id VARCHAR;
    v_project_id VARCHAR;
    v_project_name VARCHAR;
    v_start_time VARCHAR;
    v_total_tables NUMBER;
    v_matched NUMBER;
    v_different NUMBER;
    v_errors NUMBER;
    v_status VARCHAR;
BEGIN
    -- Si pas de RUN_ID fourni, prendre le plus récent
    IF :P_RUN_ID IS NULL THEN
        SELECT RUN_ID INTO v_run_id
        FROM RESULTS.COMPARISON_RUNS
        ORDER BY START_TIME DESC
        LIMIT 1;
    ELSE
        v_run_id := :P_RUN_ID;
    END IF;
    
    -- Récupérer les informations du run
    SELECT 
        r.PROJECT_ID,
        p.PROJECT_NAME,
        TO_VARCHAR(r.START_TIME, 'YYYY-MM-DD HH24:MI:SS'),
        r.TOTAL_TABLES,
        r.TABLES_MATCHED,
        r.TABLES_DIFFERENT,
        r.TABLES_ERROR,
        r.STATUS
    INTO 
        v_project_id,
        v_project_name,
        v_start_time,
        v_total_tables,
        v_matched,
        v_different,
        v_errors,
        v_status
    FROM RESULTS.COMPARISON_RUNS r
    JOIN CONFIG.CONFIG_PROJECTS p ON r.PROJECT_ID = p.PROJECT_ID
    WHERE r.RUN_ID = v_run_id;
    
    -- Créer le rapport
    LET res RESULTSET := (
        SELECT * FROM (
            SELECT 1 AS SORT_ORDER, 
                   '╔═══════════════════════════════════════════════════════════════════════════╗' AS REPORT_LINE
            UNION ALL
            SELECT 2, '║                 RAPPORT DATA DIFF - ' || RPAD(v_project_id, 39) || '║'
            UNION ALL
            SELECT 3, '║                 Date: ' || RPAD(v_start_time, 53) || '║'
            UNION ALL
            SELECT 4, '╠═══════════════════════════════════════════════════════════════════════════╣'
            UNION ALL
            SELECT 5, '║  RÉSUMÉ: ' || v_total_tables || ' tables | ✓ ' || 
                     v_matched || ' identiques | ✗ ' || 
                     v_different || ' différentes | ⚠ ' || 
                     v_errors || ' erreurs' || 
                     REPEAT(' ', 79 - LENGTH('  RÉSUMÉ: ' || v_total_tables || ' tables | ✓ ' || 
                     v_matched || ' identiques | ✗ ' || 
                     v_different || ' différentes | ⚠ ' || 
                     v_errors || ' erreurs')) || '║'
            UNION ALL
            SELECT 6, '╠═══════════════════════════════════════════════════════════════════════════╣'
            UNION ALL
            SELECT 7, '║  TABLE                    │ SAS     │ SNOW    │ MATCH  │ STATUS            ║'
            UNION ALL
            SELECT 8, '║  ─────────────────────────┼─────────┼─────────┼────────┼─────────────────  ║'
            UNION ALL
            SELECT 
                9 + ROW_NUMBER() OVER (ORDER BY TABLE_NAME) AS SORT_ORDER,
                '║  ' || RPAD(TABLE_NAME, 25) || '│ ' || 
                LPAD(TO_VARCHAR(SAS_ROW_COUNT, '999,999'), 7) || ' │ ' ||
                LPAD(TO_VARCHAR(SNOW_ROW_COUNT, '999,999'), 7) || ' │ ' ||
                LPAD(TO_VARCHAR(MATCH_PERCENTAGE, '990.0') || '%', 6) || ' │ ' ||
                CASE STATUS 
                    WHEN 'IDENTICAL' THEN '✓ IDENTICAL      '
                    WHEN 'DIFFERENT' THEN '✗ DIFFERENT      '
                    ELSE '⚠ ERROR          '
                END || ' ║' AS REPORT_LINE
            FROM RESULTS.COMPARISON_RESULTS
            WHERE RUN_ID = v_run_id
            UNION ALL
            SELECT 999997, '╠═══════════════════════════════════════════════════════════════════════════╣'
            UNION ALL
            SELECT 999998, '║  STATUT FINAL: ' || 
                CASE v_status
                    WHEN 'SUCCESS' THEN '✓ MIGRATION VALIDÉE                                         '
                    WHEN 'DIFFERENCES_FOUND' THEN '✗ DIFFÉRENCES TROUVÉES                                    '
                    ELSE '⚠ ERREURS RENCONTRÉES                                      '
                END || '║'
            UNION ALL
            SELECT 999999, '╚═══════════════════════════════════════════════════════════════════════════╝'
        )
        ORDER BY SORT_ORDER
    );
    
    RETURN TABLE(res);
END;
$$;

-- ============================================================================
-- PROCÉDURE: SP_EXPORT_DIFF_DETAILS
-- Description: Exporte les détails des différences pour analyse
-- ============================================================================
CREATE OR REPLACE PROCEDURE SP_EXPORT_DIFF_DETAILS(
    P_RUN_ID VARCHAR,
    P_TABLE_NAME VARCHAR DEFAULT NULL,
    P_DIFF_TYPE VARCHAR DEFAULT NULL
)
RETURNS TABLE (
    TABLE_NAME VARCHAR,
    DIFF_TYPE VARCHAR,
    PRIMARY_KEY VARCHAR,
    COLUMN_NAME VARCHAR,
    SAS_VALUE VARCHAR,
    SNOW_VALUE VARCHAR
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    LET res RESULTSET := (
        SELECT 
            TABLE_NAME,
            DIFF_TYPE,
            COALESCE(PRIMARY_KEY_VALUE, ROW_IDENTIFIER) AS PRIMARY_KEY,
            COLUMN_NAME,
            SAS_VALUE,
            SNOW_VALUE
        FROM RESULTS.COMPARISON_DIFF_DETAILS
        WHERE RUN_ID = :P_RUN_ID
          AND (:P_TABLE_NAME IS NULL OR TABLE_NAME = :P_TABLE_NAME)
          AND (:P_DIFF_TYPE IS NULL OR DIFF_TYPE = :P_DIFF_TYPE)
        ORDER BY TABLE_NAME, DIFF_TYPE, PRIMARY_KEY, COLUMN_NAME
    );
    
    RETURN TABLE(res);
END;
$$;

-- Message de confirmation
SELECT 'All procedures created successfully!' AS MESSAGE;