"""
DAG: food_inspections_pipeline
Orquesta el pipeline completo de Chicago Food Inspections:
  1. download_data     — descarga CSV actualizado desde portal oficial de Chicago
  2. load_to_postgres  — ingesta idempotente a raw_data.food_inspections
  3. dbt_run           — ejecuta staging + mart
  4. dbt_test          — valida calidad de datos

Credentials are loaded from environment variables — never hardcoded.
Set PG_PASSWORD and PG_HOST in your environment or Docker compose file.
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
import csv

PROJECT_DIR = "/opt/airflow/project"
RAW_FILE    = f"{PROJECT_DIR}/data/raw/food_inspections.csv"
DBT_DIR     = f"{PROJECT_DIR}/dbt_project"

# Credentials from environment variables
PG_DBNAME   = os.getenv("PG_DBNAME", "mvenzor_db")
PG_USER     = os.getenv("PG_USER", "mvenzor")
PG_PASSWORD = os.getenv("PG_PASSWORD", "")
PG_HOST     = os.getenv("PG_HOST", "localhost")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))

default_args = {
    "owner": "analytics-engineer",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

def download_data():
    url = "https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.csv?accessType=DOWNLOAD"
    os.makedirs(os.path.dirname(RAW_FILE), exist_ok=True)
    print(f"Downloading from: {url}")
    urllib.request.urlretrieve(url, RAW_FILE)
    size_mb = os.path.getsize(RAW_FILE) / 1024 / 1024
    print(f"Download complete: {size_mb:.1f} MB")

def load_to_postgres():
    print(f"Reading CSV: {RAW_FILE}")
    df = pd.read_csv(
        RAW_FILE,
        low_memory=False,
        dtype={"License #": str, "Zip": str},
        quoting=csv.QUOTE_ALL,
        on_bad_lines='skip'
    )
    print(f"Rows read: {len(df):,}")

    df.columns = [
        "inspection_id", "dba_name", "aka_name", "license_number",
        "facility_type", "risk", "address", "city", "state", "zip",
        "inspection_date", "inspection_type", "results", "violations",
        "latitude", "longitude", "location"
    ]
    df = df.where(pd.notna(df), None)

    conn = psycopg2.connect(
        dbname=PG_DBNAME, user=PG_USER,
        password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )
    cur = conn.cursor()
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
    print(f"Load complete: {len(rows):,} records")

with DAG(
    dag_id="food_inspections_pipeline",
    default_args=default_args,
    description="Daily pipeline — Chicago Food Inspections",
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
