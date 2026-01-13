-- ============================================================================
-- DATA DIFF TOOL - SCRIPT PRINCIPAL
-- ============================================================================
-- Description: Script complet pour installer et utiliser l'outil de comparaison
-- Version: 2.0
-- Date: 2025-01-09
-- Base: TEAM_DB.EXTERNAL
-- ============================================================================

-- ============================================================================
-- PARTIE 1 : CRÉATION DES OBJETS (SETUP)
-- ============================================================================

USE DATABASE TEAM_DB;
USE SCHEMA EXTERNAL;

-- Tables de résultats
CREATE OR REPLACE TABLE DIFF_RESULTS (
    COMPARISON_ID       VARCHAR(50) DEFAULT UUID_STRING() PRIMARY KEY,
    TABLE1              VARCHAR(200) NOT NULL,
    TABLE2              VARCHAR(200) NOT NULL,
    COMPARISON_TIME     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    TABLE1_ROW_COUNT    NUMBER DEFAULT 0,
    TABLE2_ROW_COUNT    NUMBER DEFAULT 0,
    MATCHED_ROWS        NUMBER DEFAULT 0,
    ONLY_IN_TABLE1      NUMBER DEFAULT 0,
    ONLY_IN_TABLE2      NUMBER DEFAULT 0,
    DIFF_VALUES         NUMBER DEFAULT 0,
    MATCH_PERCENTAGE    NUMBER(5,2),
    IS_IDENTICAL        BOOLEAN,
    HAS_PRIMARY_KEY     BOOLEAN,
    PRIMARY_KEY_COLS    VARCHAR(1000),
    EXECUTION_TIME_SEC  NUMBER,
    EXECUTED_BY         VARCHAR(100) DEFAULT CURRENT_USER()
);

CREATE OR REPLACE TABLE DIFF_DETAILS (
    DETAIL_ID           NUMBER AUTOINCREMENT PRIMARY KEY,
    COMPARISON_ID       VARCHAR(50) NOT NULL,
    DIFF_TYPE           VARCHAR(20) NOT NULL,
    PRIMARY_KEY_VALUE   VARCHAR(4000),
    ROW_HASH            VARCHAR(64),
    COLUMN_NAME         VARCHAR(200),
    VALUE_TABLE1        VARCHAR(4000),
    VALUE_TABLE2        VARCHAR(4000),
    ROW_DATA_TABLE1     VARIANT,
    ROW_DATA_TABLE2     VARIANT,
    CONSTRAINT FK_COMPARISON FOREIGN KEY (COMPARISON_ID) REFERENCES DIFF_RESULTS(COMPARISON_ID)
);

CREATE OR REPLACE TABLE TEMP_TABLE_COLUMNS (
    TABLE_NAME          VARCHAR(200),
    COLUMN_NAME         VARCHAR(200),
    DATA_TYPE           VARCHAR(100),
    ORDINAL_POSITION    NUMBER
);

-- Vues utilitaires
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

-- ============================================================================
-- PARTIE 2 : PROCÉDURE PRINCIPALE SP_COMPARE
-- ============================================================================

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
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
    // Initialisation
    var comparison_id = snowflake.createStatement({sqlText: "SELECT UUID_STRING()"}).execute().next().getColumnValue(1);
    var start_time = new Date();
    
    // Déterminer si on a une clé primaire
    var has_pk = (P_PRIMARY_KEY && P_PRIMARY_KEY !== '' && P_PRIMARY_KEY.toUpperCase() !== 'NO');
    
    // Compter les lignes
    var count1_stmt = snowflake.createStatement({sqlText: `SELECT COUNT(*) FROM ${P_TABLE1}`});
    var table1_count = count1_stmt.execute().next().getColumnValue(1);
    
    var count2_stmt = snowflake.createStatement({sqlText: `SELECT COUNT(*) FROM ${P_TABLE2}`});
    var table2_count = count2_stmt.execute().next().getColumnValue(1);
    
    // Variables pour les résultats
    var matched_count = 0, only_table1 = 0, only_table2 = 0, diff_values = 0;
    
    if (has_pk) {
        // Comparaison avec clé primaire
        var pk_cols = P_PRIMARY_KEY.split(',').map(col => col.trim());
        var pk_join = pk_cols.map(col => `t1.${col} = t2.${col}`).join(' AND ');
        var pk_select = pk_cols.map(col => `COALESCE(t1.${col}::VARCHAR, '')`).join(" || '|' || ");
        
        // 1. Lignes uniquement dans TABLE1
        var sql_only1 = `
            INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, PRIMARY_KEY_VALUE, ROW_DATA_TABLE1)
            SELECT '${comparison_id}', 'ONLY_TABLE1', ${pk_select}, OBJECT_CONSTRUCT(*)
            FROM ${P_TABLE1} t1
            WHERE NOT EXISTS (SELECT 1 FROM ${P_TABLE2} t2 WHERE ${pk_join})
        `;
        var stmt1 = snowflake.createStatement({sqlText: sql_only1});
        stmt1.execute();
        only_table1 = stmt1.getNumRowsAffected();
        
        // 2. Lignes uniquement dans TABLE2
        var pk_select2 = pk_cols.map(col => `COALESCE(t2.${col}::VARCHAR, '')`).join(" || '|' || ");
        var sql_only2 = `
            INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, PRIMARY_KEY_VALUE, ROW_DATA_TABLE2)
            SELECT '${comparison_id}', 'ONLY_TABLE2', ${pk_select2}, OBJECT_CONSTRUCT(*)
            FROM ${P_TABLE2} t2
            WHERE NOT EXISTS (SELECT 1 FROM ${P_TABLE1} t1 WHERE ${pk_join})
        `;
        var stmt2 = snowflake.createStatement({sqlText: sql_only2});
        stmt2.execute();
        only_table2 = stmt2.getNumRowsAffected();
        
        // 3. Comparer les valeurs (simplifié pour les colonnes ALL)
        if (P_COLUMNS_TO_COMPARE === 'ALL') {
            // Utiliser SHA2 pour comparer toute la ligne
            var sql_diff = `
                SELECT COUNT(*) FROM ${P_TABLE1} t1
                JOIN ${P_TABLE2} t2 ON ${pk_join}
                WHERE SHA2(OBJECT_CONSTRUCT(t1.*), 256) != SHA2(OBJECT_CONSTRUCT(t2.*), 256)
            `;
            var stmt_diff = snowflake.createStatement({sqlText: sql_diff});
            stmt_diff.execute();
            diff_values = stmt_diff.next().getColumnValue(1);
        }
        
        // Calculer les correspondances
        var sql_matched = `SELECT COUNT(*) FROM ${P_TABLE1} t1 JOIN ${P_TABLE2} t2 ON ${pk_join}`;
        var stmt_matched = snowflake.createStatement({sqlText: sql_matched});
        stmt_matched.execute();
        matched_count = stmt_matched.next().getColumnValue(1) - diff_values;
        
    } else {
        // Comparaison sans clé primaire (par hash)
        var hash_expr = 'SHA2(OBJECT_CONSTRUCT(*), 256)';
        
        // Créer tables temporaires
        snowflake.createStatement({sqlText: 
            `CREATE OR REPLACE TEMPORARY TABLE TEMP_T1_HASHES AS 
             SELECT ${hash_expr} AS ROW_HASH, OBJECT_CONSTRUCT(*) AS ROW_DATA FROM ${P_TABLE1}`
        }).execute();
        
        snowflake.createStatement({sqlText: 
            `CREATE OR REPLACE TEMPORARY TABLE TEMP_T2_HASHES AS 
             SELECT ${hash_expr} AS ROW_HASH, OBJECT_CONSTRUCT(*) AS ROW_DATA FROM ${P_TABLE2}`
        }).execute();
        
        // Compter les correspondances
        var sql_matched = `
            SELECT COUNT(*) FROM TEMP_T1_HASHES t1 
            WHERE EXISTS (SELECT 1 FROM TEMP_T2_HASHES t2 WHERE t1.ROW_HASH = t2.ROW_HASH)
        `;
        var stmt_matched = snowflake.createStatement({sqlText: sql_matched});
        stmt_matched.execute();
        matched_count = stmt_matched.next().getColumnValue(1);
        
        // Lignes uniquement dans TABLE1
        var sql_only1 = `
            INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, ROW_HASH, ROW_DATA_TABLE1)
            SELECT '${comparison_id}', 'ONLY_TABLE1', ROW_HASH, ROW_DATA
            FROM TEMP_T1_HASHES t1
            WHERE NOT EXISTS (SELECT 1 FROM TEMP_T2_HASHES t2 WHERE t1.ROW_HASH = t2.ROW_HASH)
        `;
        var stmt1 = snowflake.createStatement({sqlText: sql_only1});
        stmt1.execute();
        only_table1 = stmt1.getNumRowsAffected();
        
        // Lignes uniquement dans TABLE2
        var sql_only2 = `
            INSERT INTO DIFF_DETAILS (COMPARISON_ID, DIFF_TYPE, ROW_HASH, ROW_DATA_TABLE2)
            SELECT '${comparison_id}', 'ONLY_TABLE2', ROW_HASH, ROW_DATA
            FROM TEMP_T2_HASHES t2
            WHERE NOT EXISTS (SELECT 1 FROM TEMP_T1_HASHES t1 WHERE t1.ROW_HASH = t2.ROW_HASH)
        `;
        var stmt2 = snowflake.createStatement({sqlText: sql_only2});
        stmt2.execute();
        only_table2 = stmt2.getNumRowsAffected();
        
        // Nettoyer
        snowflake.createStatement({sqlText: "DROP TABLE IF EXISTS TEMP_T1_HASHES"}).execute();
        snowflake.createStatement({sqlText: "DROP TABLE IF EXISTS TEMP_T2_HASHES"}).execute();
    }
    
    // Calculer le pourcentage et le statut
    var match_pct = 0;
    if (table1_count + table2_count > 0) {
        match_pct = Math.round(200.0 * matched_count / (table1_count + table2_count) * 100) / 100;
    } else {
        match_pct = 100;
    }
    
    var is_identical = (table1_count === table2_count && only_table1 === 0 && 
                       only_table2 === 0 && diff_values === 0);
    
    // Temps d'exécution
    var exec_time = Math.round((new Date() - start_time) / 1000);
    
    // Enregistrer les résultats
    var insert_sql = `
        INSERT INTO DIFF_RESULTS (
            COMPARISON_ID, TABLE1, TABLE2,
            TABLE1_ROW_COUNT, TABLE2_ROW_COUNT,
            MATCHED_ROWS, ONLY_IN_TABLE1, ONLY_IN_TABLE2, DIFF_VALUES,
            MATCH_PERCENTAGE, IS_IDENTICAL, HAS_PRIMARY_KEY, PRIMARY_KEY_COLS,
            EXECUTION_TIME_SEC
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
    `;
    
    snowflake.createStatement({
        sqlText: insert_sql,
        binds: [comparison_id, P_TABLE1, P_TABLE2, table1_count, table2_count,
                matched_count, only_table1, only_table2, diff_values,
                match_pct, is_identical, has_pk, P_PRIMARY_KEY, exec_time]
    }).execute();
    
    // Afficher le rapport si demandé
    if (P_SHOW_REPORT) {
        snowflake.createStatement({
            sqlText: `CALL SP_SHOW_COMPARISON_REPORT('${comparison_id}')`
        }).execute();
    }
    
    var result = `Comparaison terminée. ID: ${comparison_id}
Tables ${is_identical ? 'IDENTIQUES' : 'DIFFÉRENTES'}
Correspondance: ${match_pct}%
Lignes: ${table1_count} vs ${table2_count}`;
    
    return result;
    
} catch (err) {
    return "Erreur: " + err.message;
}
$$;

-- ============================================================================
-- PARTIE 3 : PROCÉDURE D'AFFICHAGE DU RAPPORT
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
    IF P_COMPARISON_ID IS NULL THEN
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

-- ============================================================================
-- PARTIE 4 : PROCÉDURES UTILITAIRES
-- ============================================================================

-- Procédure pour voir les différences détaillées
CREATE OR REPLACE PROCEDURE SP_SHOW_DIFF_DETAILS(
    P_COMPARISON_ID VARCHAR,
    P_DIFF_TYPE VARCHAR DEFAULT NULL,
    P_LIMIT NUMBER DEFAULT 100
)
RETURNS TABLE (
    DIFF_TYPE VARCHAR,
    PRIMARY_KEY VARCHAR,
    ROW_HASH VARCHAR,
    COLUMN_NAME VARCHAR,
    VALUE_TABLE1 VARCHAR,
    VALUE_TABLE2 VARCHAR
)
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN
    LET res RESULTSET := (
        SELECT 
            DIFF_TYPE,
            COALESCE(PRIMARY_KEY_VALUE, ROW_HASH) AS PRIMARY_KEY,
            ROW_HASH,
            COLUMN_NAME,
            VALUE_TABLE1,
            VALUE_TABLE2
        FROM DIFF_DETAILS
        WHERE COMPARISON_ID = :P_COMPARISON_ID
          AND (:P_DIFF_TYPE IS NULL OR DIFF_TYPE = :P_DIFF_TYPE)
        ORDER BY DIFF_TYPE, PRIMARY_KEY
        LIMIT :P_LIMIT
    );
    
    RETURN TABLE(res);
END;
$$;

-- Procédure pour nettoyer les anciennes comparaisons
CREATE OR REPLACE PROCEDURE SP_CLEANUP_OLD_COMPARISONS(
    P_DAYS_TO_KEEP NUMBER DEFAULT 30
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_deleted_comparisons NUMBER;
    v_deleted_details NUMBER;
BEGIN
    -- Supprimer les détails d'abord
    DELETE FROM DIFF_DETAILS
    WHERE COMPARISON_ID IN (
        SELECT COMPARISON_ID 
        FROM DIFF_RESULTS
        WHERE COMPARISON_TIME < DATEADD('day', -:P_DAYS_TO_KEEP, CURRENT_TIMESTAMP())
    );
    GET DIAGNOSTICS v_deleted_details = ROW_COUNT;
    
    -- Puis supprimer les résultats
    DELETE FROM DIFF_RESULTS
    WHERE COMPARISON_TIME < DATEADD('day', -:P_DAYS_TO_KEEP, CURRENT_TIMESTAMP());
    GET DIAGNOSTICS v_deleted_comparisons = ROW_COUNT;
    
    RETURN 'Nettoyage terminé: ' || v_deleted_comparisons || 
           ' comparaisons et ' || v_deleted_details || ' détails supprimés';
END;
$$;

-- ============================================================================
-- MESSAGE FINAL
-- ============================================================================

SELECT '✓ Installation complète de l''outil Data Diff dans TEAM_DB.EXTERNAL' AS MESSAGE;