# Projet Airflow : ETL Météo Multi-Villes

Ce projet implémente un pipeline de données (DAG) sous Apache Airflow pour extraire, transformer et préparer des données météorologiques provenant de l'API publique Open-Meteo.

## Livrables du Projet

### 1. Le DAG (Pipeline ETL)

*   **Extraction** : Récupération asynchrone des JSON bruts pour Paris, Lyon et Marseille.
*   **Transformation** : Nettoyage des données, cast des types (float/int) et application d'un schéma métier cible.
*   **Préparation** : Mise en forme finale et logs pour validation avant insertion en base de données.

### 2. Aperçu des Données Préparées
Chaque exécution génère des enregistrements structurés comme suit :
```json
{
    "city_id": "paris",
    "temp_celsius": 14.0,
    "wind_speed_kmh": 12.1,
    "weather_condition_code": 3,
    "observation_timestamp": "2026-04-28T09:15",
    "ingestion_date": "2026-04-28 09:19:13"
}
```

### 3. Dictionnaire des Données (Champs retenus)

| Champ | Type | Description |
| :--- | :--- | :--- |
| `city_id` | String | Identifiant de la ville en minuscules (Clé de jointure). |
| `temp_celsius` | Float | Température actuelle mesurée en degrés Celsius. |
| `wind_speed_kmh` | Float | Vitesse du vent en kilomètres par heure. |
| `weather_condition_code` | Integer | Code standard WMO représentant l'état du ciel. |
| `observation_timestamp` | String | Date et heure précises de la mesure météo. |
| `ingestion_date` | String | Horodatage du passage dans la pipeline (Traçabilité). |

