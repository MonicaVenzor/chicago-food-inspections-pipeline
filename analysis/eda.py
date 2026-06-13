"""
EDA — Chicago Food Inspections
Objetivo: entender distribuciones, correlaciones y preparar features para modelo.
Conexión: PostgreSQL Unix socket desde mart_food_inspections
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns
import warnings
import os

warnings.filterwarnings('ignore')
os.makedirs('analysis/outputs', exist_ok=True)

# --- Conexión ---
conn = psycopg2.connect(dbname='mvenzor_db', user='mvenzor')

print("Cargando datos desde mart_food_inspections...")
df = pd.read_sql("""
    SELECT
        inspection_id,
        facility_type_grouped,
        risk_level,
        risk_rank,
        inspection_type,
        results,
        pass_flag,
        fail_flag,
        violation_count,
        inspection_year,
        inspection_month,
        inspection_quarter,
        zip,
        license_total_inspections,
        license_total_fails,
        repeat_fail_flag,
        days_since_previous_inspection,
        flag_fail_without_violations,
        flag_missing_license
    FROM marts.mart_food_inspections
    WHERE inspection_year >= 2015
""", conn)
conn.close()

print(f"Registros cargados: {len(df):,}")
print(f"Período: {df['inspection_year'].min()} - {df['inspection_year'].max()}")

# =============================================
# 1. DISTRIBUCIÓN DE RESULTADOS
# =============================================
print("\n=== DISTRIBUCIÓN DE RESULTADOS ===")
results_dist = df['results'].value_counts()
results_pct  = df['results'].value_counts(normalize=True) * 100
print(pd.DataFrame({'count': results_dist, 'pct': results_pct.round(2)}))

fig, ax = plt.subplots(figsize=(10, 5))
colors = ['#2ecc71','#f39c12','#e74c3c','#95a5a6','#3498db','#9b59b6','#1abc9c']
results_dist.plot(kind='bar', ax=ax, color=colors[:len(results_dist)], edgecolor='white')
ax.set_title('Distribución de Resultados de Inspección\nChicago 2015–2026', fontsize=14, fontweight='bold')
ax.set_xlabel('')
ax.set_ylabel('Número de inspecciones')
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:,.0f}'))
plt.xticks(rotation=30, ha='right')
plt.tight_layout()
plt.savefig('analysis/outputs/01_results_distribution.png', dpi=150)
plt.close()
print("Gráfica guardada: 01_results_distribution.png")

# =============================================
# 2. TASA DE FALLA POR TIPO DE ESTABLECIMIENTO
# =============================================
print("\n=== TOP 10 TIPOS DE ESTABLECIMIENTO — TASA DE FALLA ===")
top_types = df[df['facility_type_grouped'].isin(
    df['facility_type_grouped'].value_counts().head(10).index
)]
fail_by_type = top_types.groupby('facility_type_grouped').agg(
    total=('inspection_id', 'count'),
    fails=('fail_flag', 'sum')
).assign(fail_rate=lambda x: x['fails'] / x['total'] * 100).sort_values('fail_rate', ascending=False)
print(fail_by_type.round(2))

fig, ax = plt.subplots(figsize=(12, 6))
fail_by_type['fail_rate'].plot(kind='bar', ax=ax, color='#e74c3c', edgecolor='white')
ax.set_title('Tasa de Falla por Tipo de Establecimiento (Top 10)\nChicago 2015–2026', fontsize=14, fontweight='bold')
ax.set_ylabel('Tasa de falla (%)')
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:.1f}%'))
plt.xticks(rotation=35, ha='right')
plt.tight_layout()
plt.savefig('analysis/outputs/02_fail_rate_by_type.png', dpi=150)
plt.close()
print("Gráfica guardada: 02_fail_rate_by_type.png")

# =============================================
# 3. TENDENCIA ANUAL DE CUMPLIMIENTO
# =============================================
print("\n=== TENDENCIA ANUAL ===")
yearly = df.groupby('inspection_year').agg(
    total=('inspection_id','count'),
    passes=('pass_flag','sum'),
    fails=('fail_flag','sum')
).assign(
    pass_rate=lambda x: x['passes']/x['total']*100,
    fail_rate=lambda x: x['fails']/x['total']*100
)
print(yearly[['total','pass_rate','fail_rate']].round(2))

fig, ax = plt.subplots(figsize=(12, 5))
ax.plot(yearly.index, yearly['pass_rate'], marker='o', color='#2ecc71', label='Tasa de aprobación', linewidth=2)
ax.plot(yearly.index, yearly['fail_rate'], marker='s', color='#e74c3c', label='Tasa de falla', linewidth=2)
ax.set_title('Tendencia Anual de Cumplimiento\nChicago 2015–2026', fontsize=14, fontweight='bold')
ax.set_ylabel('Tasa (%)')
ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f'{x:.1f}%'))
ax.legend()
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('analysis/outputs/03_yearly_trend.png', dpi=150)
plt.close()
print("Gráfica guardada: 03_yearly_trend.png")

# =============================================
# 4. VIOLACIONES POR NIVEL DE RIESGO
# =============================================
print("\n=== VIOLACIONES PROMEDIO POR NIVEL DE RIESGO ===")
risk_viol = df[df['risk_level'] != 'UNKNOWN'].groupby('risk_level').agg(
    avg_violations=('violation_count','mean'),
    median_violations=('violation_count','median'),
    total=('inspection_id','count')
).round(2)
print(risk_viol)

# =============================================
# 5. FLAGS DE CALIDAD
# =============================================
print("\n=== RESUMEN DE FLAGS DE CALIDAD ===")
total = len(df)
print(f"flag_fail_without_violations : {df['flag_fail_without_violations'].sum():,} ({df['flag_fail_without_violations'].mean()*100:.2f}%)")
print(f"flag_missing_license         : {df['flag_missing_license'].sum():,} ({df['flag_missing_license'].mean()*100:.2f}%)")
print(f"repeat_fail_flag             : {df['repeat_fail_flag'].sum():,} ({df['repeat_fail_flag'].mean()*100:.2f}%)")

print("\nEDA completado. Gráficas en analysis/outputs/")
