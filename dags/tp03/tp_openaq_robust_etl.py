from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json
import urllib.request
from airflow.exceptions import AirflowFailException

# --- LOGIQUE DE CONTRÔLE QUALITÉ (QC) ---
def quality_control(ti):
    """
    Vérifie la validité des données de localisation OpenAQ V3.
    """
    raw_data = ti.xcom_pull(task_ids='extract_openaq_data')
    
    if not raw_data or 'results' not in raw_data:
        raise AirflowFailException("QC FAILED: Le format de réponse API est invalide (clé 'results' manquante).")

    results = raw_data['results']
    valid_records = []
    invalid_count = 0

    for loc in results:
        # Check 1 : Présence du nom et des coordonnées
        name = loc.get('name')
        coords = loc.get('coordinates') # attendu: {'latitude': ..., 'longitude': ...}
        
        if not name or not coords:
            invalid_count += 1
            continue
            
        # Check 2 : Validité des coordonnées GPS
        lat = coords.get('latitude')
        lon = coords.get('longitude')
        if lat is None or lon is None or not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
            invalid_count += 1
            continue

        # Donnée valide
        valid_records.append(loc)

    # Seuil de robustesse : Si plus de 40% des données sont mal formées
    if len(results) > 0:
        error_rate = invalid_count / len(results)
        if error_rate > 0.4:
            raise AirflowFailException(f"QC FAILED: Trop de données corrompues ({error_rate:.2%}).")

    print(f"QC PASSED: {len(valid_records)} localisations valides sur {len(results)}.")
    return valid_records

# --- EXTRACTION (V3 URL) ---
def fetch_openaq_data():
    """Récupère les localisations OpenAQ V3."""
    # On utilise l'endpoint locations qui est le plus stable en V3
    url = "https://api.openaq.org/v3/locations?limit=100"
    
    headers = {
        'User-Agent': 'Airflow-Data-Pipeline/1.0',
        'X-API-Key': '66d6416d0ada7e4129cf0546bec176f51de87a340dd0f8275c4844a8a940b72a'
    }
    req = urllib.request.Request(url, headers=headers)
    
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        raise Exception(f"Échec de l'extraction OpenAQ V3 : {str(e)}")

# --- CHARGEMENT ---
def load_to_postgres(ti):
    valid_locs = ti.xcom_pull(task_ids='quality_control_step')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    pg_hook.run("""
        CREATE TABLE IF NOT EXISTS air_quality_locations (
            id SERIAL PRIMARY KEY,
            openaq_id INT,
            name TEXT,
            country_code VARCHAR(10),
            latitude FLOAT,
            longitude FLOAT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    for loc in valid_locs:
        pg_hook.run(
            """INSERT INTO air_quality_locations (openaq_id, name, country_code, latitude, longitude) 
               VALUES (%s, %s, %s, %s, %s)""",
            parameters=(
                loc.get('id'), 
                loc.get('name'), 
                loc.get('country', {}).get('code') if isinstance(loc.get('country'), dict) else None,
                loc.get('coordinates', {}).get('latitude'),
                loc.get('coordinates', {}).get('longitude')
            )
        )

# --- DÉFINITION DU DAG ---
with DAG(
    dag_id="tp05_openaq_robust_etl",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=['tp05', 'robustness', 'openaq_v3']
) as dag:

    extract = PythonOperator(
        task_id="extract_openaq_data",
        python_callable=fetch_openaq_data,
        retries=3,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=2)
    )

    qc = PythonOperator(
        task_id="quality_control_step",
        python_callable=quality_control
    )

    load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres
    )

    extract >> qc >> load
