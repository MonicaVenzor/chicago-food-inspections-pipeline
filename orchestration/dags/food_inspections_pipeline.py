"""
DAG: food_inspections_pipeline
Orquesta el pipeline completo de Chicago Food Inspections:
  1. download_data     — descarga CSV actualizado desde portal oficial de Chicago
  2. load_to_postgres  — ingesta idempotente a raw_data.food_inspections
  3. dbt_run           — ejecuta staging + mart
  4. dbt_test          — valida calidad de datos

Decisión de diseño: pipeline secuencial con dependencias estrictas.
Si cualquier tarea falla, las siguientes no corren — igual que un
proceso regulado con criterios de aceptación formales en cada etapa.

Schedule: diario a las 6am (los datos de Chicago se actualizan diariamente)
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import urllib.request
import os

# --- Rutas dentro del contenedor ---
PROJECT_DIR = "/opt/airflow/project"
RAW_FILE    = f"{PROJECT_DIR}/data/raw/food_inspections.csv"
VENV_PYTHON = f"{PROJECT_DIR}/.venv/bin/python"
DBT_DIR     = f"{PROJECT_DIR}/dbt_project"

default_args = {
    "owner": "mvenzor",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def download_data():
    """Descarga CSV actualizado desde portal oficial de Chicago."""
    url = "https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.csv?accessType=DOWNLOAD"
    os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)
    print(f"Descargando desde: {url}")
    urllib.request.urlretrieve(url, RAW_FILE)
    size_mb = os.path.getsize(RAW_FILE) / 1024 / 1024
    print(f"Descarga completada: {size_mb:.1f} MB")

def load_to_postgres():
    """Ingesta idempotente a raw_data.food_inspections via Unix socket."""
    print(f"Leyendo CSV: {RAW_FILE}")
    df = pd.read_csv(
        RAW_FILE,
        low_memory=False,
        dtype={"License #": str, "Zip": str}
    )
    print(f"Filas leídas: {len(df):,}")

    df.columns = [
        "inspection_id", "dba_name", "aka_name", "license_number",
        "facility_type", "risk", "address", "city", "state", "zip",
        "inspection_date", "inspection_type", "results", "violations",
        "latitude", "longitude", "location"
    ]
    df = df.where(pd.notna(df), None)

    conn = psycopg2.connect(dbname="mvenzor_db", user="mvenzor",
                            password="5121", host="172.29.180.193", port=5432)
    cur  = conn.cursor()
    cur.execute("TRUNCATE TABLE raw_data.food_inspections;")

    cols = [
        "inspection_id", "dba_name", "aka_name", "license_number",
        "facility_type", "risk", "address", "city", "state", "zip",
        "inspection_date", "inspection_type", "results", "violations",
        "latitude", "longitude", "location"
    ]
    insert_sql = f"INSERT INTO raw_data.food_inspections ({', '.join(cols)}) VALUES %s"
    rows = [
        tuple(None if pd.isna(v) else v.item() if hasattr(v, "item") else v
              for v in row)
        for row in df[cols].itertuples(index=False, name=None)
    ]

    BATCH = 5000
    for i in range(0, len(rows), BATCH):
        execute_values(cur, insert_sql, rows[i:i+BATCH])
        print(f"  -> {min(i+BATCH, len(rows)):,} / {len(rows):,}")

    conn.commit()
    cur.close()
    conn.close()
    print(f"Carga completada: {len(rows):,} registros")

with DAG(
    dag_id="food_inspections_pipeline",
    default_args=default_args,
    description="Pipeline diario — Chicago Food Inspections",
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["chicago", "food-inspections", "governance"],
) as dag:

    t1_download = PythonOperator(
        task_id="download_data",
        python_callable=download_data,
    )

    t2_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t3_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir /home/mvenzor/.dbt",
    )

    t4_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir /home/mvenzor/.dbt",
    )

    t1_download >> t2_load >> t3_dbt_run >> t4_dbt_test
