from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def extract():
    print("Extraction des données")

def transform():
    print("Transformation des données")

def load():
    print("Chargement des données")

with DAG(
    dag_id="mon_premier_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="extract_data",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id="load_data",
        python_callable=load
    )

    t1 >> t2 >> t3