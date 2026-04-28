from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from datetime import datetime
import json
import urllib.request

# Configuration des villes disponibles
CITIES_CONFIG = {
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Lyon": {"lat": 45.7640, "lon": 4.8357},
    "Marseille": {"lat": 43.2965, "lon": 5.3698}
}

# --- 1. INITIALISATION DES TABLES ---
def create_tables():
    """Crée les tables de données et de suivi si elles n'existent pas."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Table des mesures météorologiques
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS weather_measurements (
            id SERIAL PRIMARY KEY,
            city_name VARCHAR(50),
            temperature FLOAT,
            wind_speed FLOAT,
            obs_time TIMESTAMP,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Table d'audit (suivi d'ingestion)
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS ingestion_tracking (
            id SERIAL PRIMARY KEY,
            dag_run_id VARCHAR(100),
            status VARCHAR(20),
            records_count INT,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

# --- 2. EXTRACTION ET TRANSFORMATION ---
def fetch_and_transform(**context):
    """Récupère la donnée API selon le paramètre ville et prépare le record."""
    # Récupération du paramètre dynamique
    city_name = context['params']['city']
    coords = CITIES_CONFIG.get(city_name, CITIES_CONFIG["Paris"])
    
    url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
    
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())
        weather = data['current_weather']
        
        return {
            "city": city_name,
            "temp": float(weather['temperature']),
            "wind": float(weather['windspeed']),
            "time": weather['time']
        }

# --- 3. CHARGEMENT DANS POSTGRES ---
def load_to_postgres(**context):
    """Insère la donnée transformée dans la table de mesures."""
    record = context['ti'].xcom_pull(task_ids='transform_weather')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    sql = "INSERT INTO weather_measurements (city_name, temperature, wind_speed, obs_time) VALUES (%s, %s, %s, %s)"
    pg_hook.run(sql, parameters=(record['city'], record['temp'], record['wind'], record['time']))

# --- 4. AUDIT ET SUIVI ---
def track_ingestion(**context):
    """Enregistre le succès de l'opération dans la table d'audit."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    run_id = context['run_id']
    
    sql = "INSERT INTO ingestion_tracking (dag_run_id, status, records_count) VALUES (%s, %s, %s)"
    pg_hook.run(sql, parameters=(run_id, 'SUCCESS', 1))

# --- DÉFINITION DU DAG ---
with DAG(
    dag_id="weather_etl_postgres_orchestrated",
    start_date=datetime(2024, 1, 1),
    schedule=None, # Manuel uniquement pour cet exercice
    catchup=False,
    params={
        "city": Param("Paris", type="string", enum=["Paris", "Lyon", "Marseille"], description="Choisissez la ville à ingérer")
    },
    tags=['tp', 'postgres', 'etl_orchestrated']
) as dag:

    init_db = PythonOperator(
        task_id="setup_tables",
        python_callable=create_tables
    )

    extract_transform = PythonOperator(
        task_id="transform_weather",
        python_callable=fetch_and_transform
    )

    load_data = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    log_audit = PythonOperator(
        task_id="audit_tracking",
        python_callable=track_ingestion
    )

    # Définition du workflow
    init_db >> extract_transform >> load_data >> log_audit
