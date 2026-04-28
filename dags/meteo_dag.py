from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import json
import urllib.request

# Configuration Métier : Coordonnées des villes cibles
CITIES = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698}
}

# --- ÉTAPE 1 : EXTRACTION (Extraction brute de l'API) ---
def extract_raw_weather_data():
    """Récupère les données JSON brutes de l'API Open-Meteo pour chaque ville."""
    raw_payloads = {}
    for city, coords in CITIES.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        with urllib.request.urlopen(url) as response:
            # On stocke l'intégralité du JSON pour garder une trace de l'origine si besoin
            raw_payloads[city] = json.loads(response.read().decode())
    return raw_payloads

# --- ÉTAPE 2 : TRANSFORMATION (Nettoyage et structuration selon le schéma cible) ---
def transform_to_clean_schema(ti):
    """Filtre les données brutes pour ne garder que les champs métier utiles à la future table."""
    # Récupération des données brutes via XCom
    api_data = ti.xcom_pull(task_ids='extract_raw_data_task')
    
    clean_data_list = []
    
    for city_name, full_json in api_data.items():
        # Extraction de la section météo actuelle du JSON
        api_weather = full_json.get('current_weather', {})
        
        # MAPPING MÉTIER : Définition explicite de la structure de données "propre"
        # On ne garde que le nécessaire pour l'analyse météo business.
        formatted_record = {
            "city_id": city_name.lower(),                   # Clé primaire potentielle
            "temp_celsius": float(api_weather.get("temperature", 0)),
            "wind_speed_kmh": float(api_weather.get("windspeed", 0)),
            "weather_condition_code": int(api_weather.get("weathercode", 0)),
            "observation_timestamp": api_weather.get("time"), # Heure de la mesure
            "ingestion_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S") # Timestamp de la pipeline
        }
        clean_data_list.append(formatted_record)
        
    return clean_data_list

# --- ÉTAPE 3 : CHARGEMENT / AFFICHAGE (Préparation à l'insertion) ---
def log_final_prepared_data(ti):
    """Affiche les données transformées prêtes à être insérées en base de données."""
    prepared_data = ti.xcom_pull(task_ids='transform_data_task')
    
    print(f"--- PRÉPARATION DE L'INSERTION ({len(prepared_data)} villes) ---")
    for record in prepared_data:
        # Structure de données cohérente et répétable pour une future table SQL
        print(f"DONNÉES CIBLES : {record}")

# Définition du DAG avec une nomenclature explicite
with DAG(
    dag_id="etl_meteo_production_ready",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=['production', 'etl', 'clean_code'],
    doc_md="""
    ### Pipeline ETL Météo
    Ce DAG extrait les données météo de 3 villes, transforme le JSON brut en un schéma 
    propre et cohérent, et prépare l'insertion en base de données.
    """
) as dag:

    # Tâches nommées selon leur rôle technique précis
    extract_task = PythonOperator(
        task_id="extract_raw_data_task",
        python_callable=extract_raw_weather_data
    )

    transform_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform_to_clean_schema
    )

    load_task = PythonOperator(
        task_id="prepare_load_task",
        python_callable=log_final_prepared_data
    )

    # Flux de données : Extraction -> Transformation -> Chargement
    extract_task >> transform_task >> load_task
