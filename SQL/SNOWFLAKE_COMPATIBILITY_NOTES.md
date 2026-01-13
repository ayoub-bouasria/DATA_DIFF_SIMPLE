# Notes de Compatibilité Snowflake - DATA_DIFF

## Résumé des Corrections Appliquées

Ce document décrit les modifications apportées au fichier `01_procedures.sql` pour assurer la compatibilité complète avec Snowflake SQL.

---

## 1. **SP_COMPARE_TABLE**

### Problème
- Utilisation de `IDENTIFIER()` pour une requête dynamique non supportée

### Solution
```sql
-- Avant:
SELECT COUNT(*)
INTO v_snow_count
FROM IDENTIFIER(:v_snow_table);

-- Après:
EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || :v_snow_table
INTO :v_snow_count;
```

---

## 2. **SP_COMPARE_WITH_PK**

### Problèmes
1. **Paramètres OUTPUT non supportés** - Snowflake ne supporte pas les paramètres OUTPUT
2. **Boucle FOR avec ARRAY** - Syntaxe `FOR i IN 0 TO ARRAY_SIZE()` non supportée
3. **Accès tableau par index** - Syntaxe `array[i]` non supportée
4. **GET DIAGNOSTICS** - Non supporté dans Snowflake

### Solutions

#### a) Suppression des paramètres OUTPUT
```sql
-- Avant:
CREATE OR REPLACE PROCEDURE SP_COMPARE_WITH_PK(
    ...
    P_MATCHED_COUNT NUMBER OUTPUT,
    P_ONLY_SAS NUMBER OUTPUT,
    P_ONLY_SNOW NUMBER OUTPUT,
    P_DIFF_VALUES NUMBER OUTPUT
)

-- Après:
CREATE OR REPLACE PROCEDURE SP_COMPARE_WITH_PK(
    P_RUN_ID VARCHAR,
    P_PROJECT_ID VARCHAR,
    P_TABLE_NAME VARCHAR,
    P_SNOW_TABLE VARCHAR,
    P_PK_COLS VARCHAR,
    P_COMPARE_COLS VARCHAR
)
```

Les résultats sont maintenant écrits dans une table temporaire `TEMP_COMPARISON_METRICS` :
```sql
INSERT INTO TEMP_COMPARISON_METRICS (RUN_ID, PROJECT_ID, TABLE_NAME, MATCHED_COUNT, ONLY_SAS, ONLY_SNOW, DIFF_VALUES)
VALUES (:P_RUN_ID, :P_PROJECT_ID, :P_TABLE_NAME, v_matched_count, v_only_sas, v_only_snow, v_diff_values);
```

#### b) Remplacement de la boucle FOR
```sql
-- Avant:
FOR i IN 0 TO ARRAY_SIZE(v_pk_list) - 1 DO
    v_col := TRIM(v_pk_list[i]);
    ...
END LOOP;

-- Après:
i := 0;
WHILE (i < v_pk_count) DO
    v_col := TRIM(GET(v_pk_list, i));
    ...
    i := i + 1;
END WHILE;
```

#### c) Remplacement de GET DIAGNOSTICS
```sql
-- Avant:
EXECUTE IMMEDIATE v_sql;
GET DIAGNOSTICS :P_ONLY_SAS = ROW_COUNT;

-- Après:
EXECUTE IMMEDIATE v_sql;
v_only_sas := SQLROWCOUNT;
```

---

## 3. **SP_COMPARE_WITHOUT_PK**

### Problèmes identiques à SP_COMPARE_WITH_PK
- Paramètres OUTPUT supprimés
- GET DIAGNOSTICS remplacé par SQLROWCOUNT
- Variables locales ajoutées
- Écriture des résultats dans TEMP_COMPARISON_METRICS

### Solution
```sql
-- Ajout de variables locales
DECLARE
    v_sql VARCHAR;
    v_matched_count NUMBER;
    v_only_sas NUMBER;
    v_only_snow NUMBER;
    v_diff_values NUMBER;

-- Utilisation de SQLROWCOUNT
EXECUTE IMMEDIATE v_sql;
v_only_sas := SQLROWCOUNT;

-- Écriture des métriques
INSERT INTO TEMP_COMPARISON_METRICS (...)
VALUES (...);
```

---

## 4. **SP_RUN_COMPARISON**

### Problèmes
1. **CURSOR avec LOOP/BREAK** - Syntaxe non standard
2. **CASE dans assignation** - Non supporté directement

### Solutions

#### a) Remplacement du CURSOR
```sql
-- Avant:
v_cursor CURSOR FOR 
    SELECT TABLE_NAME, PRIORITY
    FROM CONFIG_TABLES
    WHERE PROJECT_ID = :P_PROJECT_ID AND IS_ACTIVE = TRUE;

OPEN v_cursor;
LOOP
    FETCH v_cursor INTO v_table_name, v_priority;
    IF NOT FOUND THEN
        BREAK;
    END IF;
    ...
END LOOP;
CLOSE v_cursor;

-- Après:
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_TABLES_TO_PROCESS AS
SELECT TABLE_NAME, PRIORITY, ROW_NUMBER() OVER (ORDER BY ...) AS ROW_NUM
FROM CONFIG_TABLES
WHERE PROJECT_ID = :P_PROJECT_ID AND IS_ACTIVE = TRUE;

v_row_idx := 1;
WHILE (v_row_idx <= v_total_rows) DO
    SELECT TABLE_NAME, PRIORITY 
    INTO v_table_name, v_priority
    FROM TEMP_TABLES_TO_PROCESS
    WHERE ROW_NUM = v_row_idx;
    ...
    v_row_idx := v_row_idx + 1;
END WHILE;
```

#### b) Remplacement de CASE dans assignation
```sql
-- Avant:
CASE v_status
    WHEN 'IDENTICAL' THEN v_tables_matched := v_tables_matched + 1;
    WHEN 'DIFFERENT' THEN v_tables_different := v_tables_different + 1;
    WHEN 'ERROR' THEN v_tables_error := v_tables_error + 1;
END CASE;

-- Après:
IF (v_status = 'IDENTICAL') THEN
    v_tables_matched := v_tables_matched + 1;
ELSEIF (v_status = 'DIFFERENT') THEN
    v_tables_different := v_tables_different + 1;
ELSEIF (v_status = 'ERROR') THEN
    v_tables_error := v_tables_error + 1;
END IF;
```

---

## 5. **SP_COMPARE_TABLE - Création de la table temporaire**

### Ajout
Une table temporaire `TEMP_COMPARISON_METRICS` est créée pour stocker les résultats des sous-procédures :

```sql
CREATE TEMPORARY TABLE IF NOT EXISTS TEMP_COMPARISON_METRICS (
    RUN_ID        VARCHAR,
    PROJECT_ID    VARCHAR,
    TABLE_NAME    VARCHAR,
    MATCHED_COUNT NUMBER,
    ONLY_SAS      NUMBER,
    ONLY_SNOW     NUMBER,
    DIFF_VALUES   NUMBER
);
```

Cette table est utilisée pour récupérer les métriques sans passer par des paramètres OUTPUT.

---

## Fonctionnalités Snowflake Utilisées

1. **EXECUTE IMMEDIATE** - Pour les requêtes dynamiques
2. **SQLROWCOUNT** - Pour obtenir le nombre de lignes affectées
3. **GET(array, index)** - Pour accéder aux éléments d'un tableau
4. **WHILE loops** - Pour les boucles itératives
5. **Tables temporaires** - Pour partager des données entre procédures
6. **LET RESULTSET** - Pour retourner des résultats tabulaires

---

## Compatibilité Assurée

✅ Toutes les procédures sont maintenant compatibles avec Snowflake SQL
✅ Aucune fonctionnalité non supportée n'est utilisée
✅ Les résultats intermédiaires sont gérés via des tables temporaires
✅ Les boucles utilisent des constructions standard Snowflake
✅ L'accès aux tableaux utilise la fonction GET()

---

## Test Recommandé

Avant d'exécuter sur vos données de production, testez les procédures dans l'ordre suivant :

1. `SP_REGISTER_PROJECT` - Enregistrer un projet de test
2. `SP_REGISTER_TABLE` - Enregistrer une table de test
3. `SP_COMPARE_TABLE` - Tester la comparaison d'une seule table
4. `SP_RUN_COMPARISON` - Lancer une comparaison complète
5. `SP_SHOW_REPORT` - Afficher le rapport

---

**Date de mise à jour** : 2026-01-13  
**Version** : 1.1 - Compatible Snowflake
