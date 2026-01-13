# DATA DIFF SIMPLE - Guide d'utilisation

## Vue d'ensemble

Outil SQL Snowflake pour comparer deux tables directement dans TEAM_DB.EXTERNAL avec une syntaxe simple :

```sql
CALL SP_COMPARE('TABLE1', 'TABLE2', 'PRIMARY_KEY');
```

## Installation rapide

Exécuter le fichier `main.sql` dans TEAM_DB.EXTERNAL. C'est tout !

## Utilisation

### Syntaxe de base

```sql
CALL SP_COMPARE(
    P_TABLE1,              -- Nom de la première table
    P_TABLE2,              -- Nom de la deuxième table  
    P_PRIMARY_KEY,         -- Clé primaire (optionnel)
    P_COLUMNS_TO_COMPARE,  -- Colonnes à comparer (défaut: 'ALL')
    P_NUMERIC_TOLERANCE,   -- Tolérance numérique (défaut: 0)
    P_CASE_SENSITIVE,      -- Sensible à la casse (défaut: TRUE)
    P_SHOW_REPORT         -- Afficher le rapport (défaut: TRUE)
);
```

### Exemples simples

#### 1. Comparaison sans clé primaire
```sql
CALL SP_COMPARE('HISTORICO_REMESAS', 'HISTORICO_REMESAS_NEW');
```

#### 2. Comparaison avec clé primaire simple
```sql
CALL SP_COMPARE('OUTPUT_ENVIO', 'OUTPUT_ENVIO_V2', 'CODNUM');
```

#### 3. Comparaison avec clé primaire composite
```sql
CALL SP_COMPARE(
    'OUTPUT_DOMICILIACIONES_NO_NEGATIVAS',
    'OUTPUT_DOMICILIACIONES_NO_NEGATIVAS_NEW',
    'CONTRATO,CODNUM'  -- Clés séparées par virgule
);
```

#### 4. Comparaison avec tolérance numérique
```sql
CALL SP_COMPARE(
    'REMESA_BBVA',
    'REMESA_BBVA_MIGRATED',
    NULL,    -- Pas de clé primaire
    'ALL',   -- Toutes les colonnes
    0.01     -- Tolérance de ±0.01
);
```

## Consultation des résultats

### Rapport automatique
Par défaut, un rapport s'affiche après chaque comparaison :

```
╔═══════════════════════════════════════════════════════════════════════════╗
║                        RAPPORT DE COMPARAISON                              ║
║ ID: 123e4567-e89b-12d3-a456-426614174000                                   ║
║ Date: 2025-01-09 14:30:00                                                  ║
╠═══════════════════════════════════════════════════════════════════════════╣
║ TABLE 1: HISTORICO_REMESAS                                                 ║
║ TABLE 2: HISTORICO_REMESAS_NEW                                             ║
╠═══════════════════════════════════════════════════════════════════════════╣
║ STATISTIQUES:                                                              ║
║   - Lignes Table 1:          125,432                                       ║
║   - Lignes Table 2:          125,432                                       ║
║   - Lignes identiques:       125,432                                       ║
║   - Uniquement Table 1:            0                                       ║
║   - Uniquement Table 2:            0                                       ║
║   - Valeurs différentes:           0                                       ║
╠═══════════════════════════════════════════════════════════════════════════╣
║ RÉSULTAT: ✓ TABLES IDENTIQUES (100.0% match)                              ║
╚═══════════════════════════════════════════════════════════════════════════╝
```

### Requêtes utiles

```sql
-- Voir les comparaisons récentes
SELECT * FROM V_RECENT_COMPARISONS;

-- Afficher le dernier rapport
CALL SP_SHOW_COMPARISON_REPORT();

-- Voir les différences détaillées
CALL SP_SHOW_DIFF_DETAILS(
    'COMPARISON_ID',  -- ID de la comparaison
    'ONLY_TABLE1',    -- Type: ONLY_TABLE1, ONLY_TABLE2, VALUE_DIFF
    50                -- Limite de lignes
);

-- Statistiques par tables
SELECT 
    TABLE1, TABLE2,
    COUNT(*) AS NB_COMPARISONS,
    AVG(MATCH_PERCENTAGE) AS AVG_MATCH
FROM DIFF_RESULTS
GROUP BY TABLE1, TABLE2;
```

## Cas d'usage PROYECTO 0

```sql
-- Comparer toutes les tables du projet
CALL SP_COMPARE('HISTORICO_REMESAS', 'HISTORICO_REMESAS_SAS', 'CODNUM');
CALL SP_COMPARE('OUTPUT_ENVIO', 'OUTPUT_ENVIO_SAS', 'CODNUM');
CALL SP_COMPARE('OUTPUT_ENVIO_BBVA', 'OUTPUT_ENVIO_BBVA_SAS', 'CODNUM');
CALL SP_COMPARE('EXCLUSIONES_REMESAS', 'EXCLUSIONES_REMESAS_SAS'); -- Sans PK
CALL SP_COMPARE('REMESA_BBVA', 'REMESA_BBVA_SAS'); -- Sans PK
```

## Maintenance

```sql
-- Nettoyer les anciennes comparaisons (>30 jours)
CALL SP_CLEANUP_OLD_COMPARISONS(30);

-- Voir l'espace utilisé
SELECT 
    'DIFF_RESULTS' AS TABLE_NAME,
    COUNT(*) AS ROW_COUNT
FROM DIFF_RESULTS
UNION ALL
SELECT 'DIFF_DETAILS', COUNT(*) FROM DIFF_DETAILS;
```

## Tables créées

- **DIFF_RESULTS** : Résumé de chaque comparaison
- **DIFF_DETAILS** : Détails des différences trouvées
- **V_RECENT_COMPARISONS** : Vue des 100 dernières comparaisons

## Points importants

1. **Sans clé primaire** : Comparaison par hash SHA-256 de la ligne complète
2. **Avec clé primaire** : Comparaison colonne par colonne possible
3. **Performance** : Optimisé pour des tables jusqu'à plusieurs millions de lignes
4. **Tolérance numérique** : Utile pour les différences de précision entre systèmes

## Support

En cas d'erreur, vérifier :
1. Que les deux tables existent dans la base accessible
2. Que les noms de colonnes de la clé primaire sont corrects
3. Les permissions sur les tables