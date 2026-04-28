# TP 2B : Pipeline complet API → Transformation → PostgreSQL

Ce projet consiste en la mise en place d'une pipeline de données orchestrée récupérant des informations sur des livres via l'API Open Library, les transformant et les stockant dans une base PostgreSQL avec un suivi d'audit complet.

## 🚀 Fonctionnement du DAG
Le DAG `tp_open_library_complete_etl` est découpé en 5 étapes :
1.  **setup_and_audit_start** : Initialisation des tables SQL et ouverture d'une session d'audit (statut `RUNNING`).
2.  **extract_step** : Appel à l'API Open Library avec paramètres dynamiques.
3.  **transform_step** : Nettoyage des données et calcul d'un score de qualité.
4.  **load_step** : Insertion/Mise à jour (Upsert) des données dans PostgreSQL.
5.  **audit_finish** : Clôture de l'audit avec statistiques (statut `SUCCESS`).

## 📊 Schéma des Données (Livres)
| Champ | Description |
| :--- | :--- |
| `book_key` | Identifiant unique Open Library (Clé primaire). |
| `title` | Titre du livre. |
| `authors` | Liste des auteurs transformée en chaîne de caractères. |
| `first_publish_year` | Première année de publication. |
| `completeness_score` | Score (0 à 1) qualifiant la richesse des données récupérées. |
| `search_query` | Mot-clé utilisé pour la recherche. |

## 🛡️ Traçabilité (Audit)
Chaque exécution est enregistrée dans la table `open_library_audit` pour suivre le nombre de lignes extraites vs chargées, ainsi que les temps de traitement.

## ⚙️ Paramétrage
Le pipeline est flexible grâce aux `params` Airflow :
*   `query` : Le mot-clé de recherche (ex: *python*, *history*, *data*).
*   `limit` : Le nombre maximum de résultats à traiter.

## 🔍 Requêtes de Contrôle
Pour vérifier les résultats en base :
```sql
-- Nombre de livres chargés
SELECT count(*) FROM open_library_books;

-- Trace de la dernière ingestion
SELECT * FROM open_library_audit ORDER BY start_time DESC LIMIT 1;
```

---
*Projet réalisé pour le TP Airflow - Avril 2026*
