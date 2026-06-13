"""
Ingesta de Food Inspections de Chicago hacia PostgreSQL.
Fuente: City of Chicago Open Data Portal (Socrata API)
Destino: raw_data.food_inspections

Decisiones de diseño:
- Conexión via Unix socket (sin host) — evita conflictos de autenticación md5
- Carga via execute_values — más eficiente que insert row by row
- Sin transformaciones en esta capa — raw significa raw
- ingested_at se asigna automáticamente por DEFAULT NOW() en la tabla
"""

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import sys
from datetime import datetime

# --- Configuración ---
DB_NAME = "mvenzor_db"
DB_USER = "mvenzor"
RAW_FILE = "data/raw/food_inspections.csv"
TARGET_TABLE = "raw_data.food_inspections"
BATCH_SIZE = 5000

def load_data():
    print(f"[{datetime.now():%H:%M:%S}] Iniciando carga de {RAW_FILE}")

    # --- Leer CSV ---
    print(f"[{datetime.now():%H:%M:%S}] Leyendo CSV...")
    df = pd.read_csv(
        RAW_FILE,
        low_memory=False,
        dtype={
            "Inspection ID": "Int64",
            "License #": str,
            "Zip": str,
        }
    )
    print(f"[{datetime.now():%H:%M:%S}] Filas leídas: {len(df):,}")

    # --- Renombrar columnas a snake_case ---
    df.columns = [
        "inspection_id", "dba_name", "aka_name", "license_number",
        "facility_type", "risk", "address", "city", "state", "zip",
        "inspection_date", "inspection_type", "results", "violations",
        "latitude", "longitude", "location"
    ]

    # --- Reemplazar NaN por None para que PostgreSQL reciba NULL ---
    df = df.where(pd.notna(df), None)

    # --- Conectar via Unix socket ---
    print(f"[{datetime.now():%H:%M:%S}] Conectando a PostgreSQL...")
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER)
    cur = conn.cursor()

    # --- Limpiar tabla antes de cargar (idempotente) ---
    cur.execute(f"TRUNCATE TABLE {TARGET_TABLE};")
    print(f"[{datetime.now():%H:%M:%S}] Tabla truncada — carga limpia")

    # --- Cargar en batches ---
    cols = [
        "inspection_id", "dba_name", "aka_name", "license_number",
        "facility_type", "risk", "address", "city", "state", "zip",
        "inspection_date", "inspection_type", "results", "violations",
        "latitude", "longitude", "location"
    ]

    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(cols)})
        VALUES %s
    """

    rows = [tuple(None if pd.isna(v) else v.item() if hasattr(v, "item") else v for v in row) for row in df[cols].itertuples(index=False, name=None)]
    total = len(rows)
    loaded = 0

    print(f"[{datetime.now():%H:%M:%S}] Cargando {total:,} registros en batches de {BATCH_SIZE:,}...")

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]
        execute_values(cur, insert_sql, batch)
        loaded += len(batch)
        pct = loaded / total * 100
        print(f"  -> {loaded:,} / {total:,} ({pct:.1f}%)")

    conn.commit()
    cur.close()
    conn.close()

    print(f"[{datetime.now():%H:%M:%S}] Carga completada exitosamente.")
    print(f"[{datetime.now():%H:%M:%S}] Total registros insertados: {loaded:,}")

if __name__ == "__main__":
    load_data()
