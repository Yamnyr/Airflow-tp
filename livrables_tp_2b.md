# Livrables : TP 2B — Pipeline complet API → PostgreSQL

## 1. DAG Complet
Le code source est disponible dans le fichier `dags/tp_open_library_etl.py`. Il implémente une logique ETL robuste avec audit.

## 2. Script SQL de création des tables
Ces tables sont créées automatiquement par le DAG lors de sa première exécution.

```sql
-- Stockage des données transformées
CREATE TABLE IF NOT EXISTS open_library_books (
    id SERIAL PRIMARY KEY,
    book_key VARCHAR(100) UNIQUE,
    title TEXT,
    authors TEXT,
    first_publish_year INT,
    edition_count INT,
    search_query VARCHAR(100),
    completeness_score FLOAT,
    ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Traçabilité des exécutions
CREATE TABLE IF NOT EXISTS open_library_audit (
    id SERIAL PRIMARY KEY,
    source_url TEXT,
    search_query VARCHAR(100),
    limit_param INT,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status VARCHAR(20),
    records_extracted INT,
    records_loaded INT
);
```

## 3. Preuve du chargement et d'exécution tracée
Résultats obtenus après une exécution avec le paramètre `query="python"` et `limit=20` :

*   **Nombre de livres chargés** : 20
*   **Audit de l'exécution** :
    *   Statut : `SUCCESS`
    *   Records extraits : 20
    *   Records chargés : 20

## 4. Requêtes SQL de contrôle
Voici les requêtes permettant de valider la qualité des données :

```sql
-- Top 5 des auteurs les plus représentés
SELECT authors, count(*) as nb FROM open_library_books 
WHERE authors IS NOT NULL GROUP BY authors ORDER BY nb DESC LIMIT 5;

-- Livres avec un faible score de complétude (< 0.5)
SELECT title, completeness_score FROM open_library_books 
WHERE completeness_score < 0.5;

-- Vérification de la traçabilité
SELECT search_query, status, start_time, end_time FROM open_library_audit;
```

## 5. Explication des choix de transformation
*   **Simplification des Auteurs** : L'API renvoyant une liste, nous avons choisi de les concaténer en une chaîne unique pour faciliter les recherches textuelles en SQL.
*   **Score de complétude** : Création d'un indicateur métier (de 0 à 1) évaluant la présence des champs essentiels (Titre, Auteur, Année, Éditions). Cela permet de filtrer rapidement les données de faible qualité.
*   **Idempotence** : Utilisation de la clause `ON CONFLICT` pour permettre de relancer le pipeline sans créer de doublons, tout en mettant à jour les informations si elles ont changé.
