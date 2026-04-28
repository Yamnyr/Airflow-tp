from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.param import Param
from airflow.utils.timezone import utcnow as now
from datetime import datetime
import json
import urllib.request
import urllib.parse

# --- 1. SETUP & AUDIT START ---
def start_ingestion_audit(**context):
    """Prépare les tables et ouvre une ligne d'audit."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    # Création des tables (Contrainte 3 et 4)
    pg_hook.run("""
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
    """)

    # Initialisation de l'audit
    query = context['params']['query']
    limit = context['params']['limit']
    source = f"https://openlibrary.org/search.json?q={query}&limit={limit}"
    
    sql = """
        INSERT INTO open_library_audit (source_url, search_query, limit_param, start_time, status)
        VALUES (%s, %s, %s, %s, %s) RETURNING id
    """
    audit_id = pg_hook.get_first(sql, parameters=(source, query, limit, now(), 'RUNNING'))[0]
    return audit_id

# --- 2. EXTRACTION ---
def extract_from_api(**context):
    """Récupère les données JSON de l'API Open Library (Contrainte 1)."""
    query = context['params']['query']
    limit = context['params']['limit']
    
    encoded_query = urllib.parse.quote(query)
    url = f"https://openlibrary.org/search.json?q={encoded_query}&limit={limit}"
    
    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode())
        docs = data.get('docs', [])
        return docs

# --- 3. TRANSFORMATION ---
def transform_data(**context):
    """Transforme le JSON brut en structure exploitable (Contrainte 2)."""
    raw_docs = context['ti'].xcom_pull(task_ids='extract_step')
    query_used = context['params']['query']
    
    transformed_results = []
    
    for doc in raw_docs:
        # Simplification des auteurs (liste -> string)
        authors_list = doc.get('author_name', [])
        authors_str = ", ".join(authors_list) if authors_list else None
        
        # Calcul du score de complétude (Qualification métier)
        fields_to_check = ['title', 'author_name', 'first_publish_year', 'edition_count']
        score = sum(1 for f in fields_to_check if doc.get(f)) / len(fields_to_check)
        
        clean_doc = {
            "book_key": doc.get('key'),
            "title": doc.get('title', 'Inconnu'),
            "authors": authors_str,
            "year": doc.get('first_publish_year'),
            "edition_count": doc.get('edition_count', 0),
            "search_query": query_used,
            "completeness_score": score
        }
        transformed_results.append(clean_doc)
        
    return transformed_results

# --- 4. CHARGEMENT ---
def load_to_postgres(**context):
    """Charge les données dans PostgreSQL avec gestion des doublons (Contrainte 3)."""
    books = context['ti'].xcom_pull(task_ids='transform_step')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    
    loaded_count = 0
    for b in books:
        sql = """
            INSERT INTO open_library_books (book_key, title, authors, first_publish_year, edition_count, search_query, completeness_score)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (book_key) DO UPDATE SET
                title = EXCLUDED.title,
                authors = EXCLUDED.authors,
                completeness_score = EXCLUDED.completeness_score,
                ingested_at = CURRENT_TIMESTAMP
        """
        pg_hook.run(sql, parameters=(
            b['book_key'], b['title'], b['authors'], b['year'], 
            b['edition_count'], b['search_query'], b['completeness_score']
        ))
        loaded_count += 1
    return loaded_count

# --- 5. AUDIT FINISH ---
def end_ingestion_audit(**context):
    """Clôture l'audit avec les statistiques finales (Contrainte 4)."""
    audit_id = context['ti'].xcom_pull(task_ids='setup_and_audit_start')
    raw_data = context['ti'].xcom_pull(task_ids='extract_step')
    loaded_count = context['ti'].xcom_pull(task_ids='load_step')
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    sql = """
        UPDATE open_library_audit 
        SET end_time = %s, status = %s, records_extracted = %s, records_loaded = %s
        WHERE id = %s
    """
    pg_hook.run(sql, parameters=(now(), 'SUCCESS', len(raw_data), loaded_count, audit_id))

# --- CONFIGURATION ---
with DAG(
    dag_id="tp_open_library_complete_etl",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "query": Param("python", type="string", description="Mot-clé de recherche (ex: history, data, business)"),
        "limit": Param(20, type="integer", minimum=1, maximum=100, description="Nombre de livres à récupérer")
    },
    tags=['tp', 'data_ingestion', 'final']
) as dag:

    # Orchestration explicite (Contrainte : DAG lisible)
    start = PythonOperator(task_id="setup_and_audit_start", python_callable=start_ingestion_audit)
    extract = PythonOperator(task_id="extract_step", python_callable=extract_from_api)
    transform = PythonOperator(task_id="transform_step", python_callable=transform_data)
    load = PythonOperator(task_id="load_step", python_callable=load_to_postgres)
    finish = PythonOperator(task_id="audit_finish", python_callable=end_ingestion_audit)

    start >> extract >> transform >> load >> finish
