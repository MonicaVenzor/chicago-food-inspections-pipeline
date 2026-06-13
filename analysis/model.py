"""
Modelo Predictivo — Chicago Food Inspections
Objetivo: clasificar si una inspección resultará en cumplimiento (Pass/Pass w/ Conditions)
o falla (Fail).
Target: compliance_flag (1 = cumplimiento, 0 = falla)
Excluye: Out of Business, No Entry, Not Ready, Business Not Located
         (condiciones operativas, no resultados de evaluación)
"""

import psycopg2
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import joblib
import os
import warnings

from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    classification_report, roc_auc_score, confusion_matrix,
    ConfusionMatrixDisplay, RocCurveDisplay
)
from xgboost import XGBClassifier

warnings.filterwarnings('ignore')
os.makedirs('analysis/outputs', exist_ok=True)
os.makedirs('analysis/model', exist_ok=True)

# =============================================
# 1. CARGA DE DATOS
# =============================================
print("Cargando datos...")
conn = psycopg2.connect(dbname='mvenzor_db', user='mvenzor')
df = pd.read_sql("""
    SELECT
        facility_type_grouped,
        risk_level,
        risk_rank,
        inspection_type,
        results,
        violation_count,
        inspection_year,
        inspection_month,
        inspection_quarter,
        license_total_inspections,
        license_total_fails,
        repeat_fail_flag,
        days_since_previous_inspection
    FROM marts.mart_food_inspections
    WHERE inspection_year >= 2015
      AND results IN ('Pass', 'Pass w/ Conditions', 'Fail')
""", conn)
conn.close()
print(f"Registros para modelado: {len(df):,}")

# =============================================
# 2. PREPARACIÓN DE FEATURES
# =============================================
# Target: 1 = cumplimiento, 0 = falla
df['compliance_flag'] = (df['results'].isin(['Pass', 'Pass w/ Conditions'])).astype(int)
print(f"\nDistribución del target:")
print(df['compliance_flag'].value_counts())
print(f"Balance: {df['compliance_flag'].mean()*100:.1f}% cumplimiento")

# Encoding de categorías
le_facility = LabelEncoder()
le_risk     = LabelEncoder()
le_type     = LabelEncoder()

df['facility_type_enc'] = le_facility.fit_transform(df['facility_type_grouped'].fillna('UNKNOWN'))
df['risk_level_enc']    = le_risk.fit_transform(df['risk_level'].fillna('UNKNOWN'))
df['inspection_type_enc'] = le_type.fit_transform(df['inspection_type'].fillna('UNKNOWN'))

# Imputar days_since_previous_inspection: -1 para primera inspección
df['days_since_previous_inspection'] = df['days_since_previous_inspection'].fillna(-1)

# Features finales
FEATURES = [
    'facility_type_enc',
    'risk_rank',
    'risk_level_enc',
    'inspection_type_enc',
    'violation_count',
    'inspection_year',
    'inspection_month',
    'inspection_quarter',
    'license_total_inspections',
    'license_total_fails',
    'repeat_fail_flag',
    'days_since_previous_inspection'
]

df['repeat_fail_flag'] = df['repeat_fail_flag'].astype(int)
df['risk_rank'] = df['risk_rank'].fillna(0).astype(int)

X = df[FEATURES]
y = df['compliance_flag']

# =============================================
# 3. SPLIT TEMPORAL — no aleatorio
# =============================================
# Decisión de gobernanza: split por año, no aleatorio.
# Un split aleatorio introduce data leakage en series temporales:
# el modelo vería inspecciones futuras al entrenar.
train_mask = df['inspection_year'] <= 2023
X_train, X_test = X[train_mask], X[~train_mask]
y_train, y_test = y[train_mask], y[~train_mask]

print(f"\nSplit temporal:")
print(f"  Train: 2015-2023 — {len(X_train):,} registros")
print(f"  Test:  2024-2026 — {len(X_test):,} registros")

# =============================================
# 4. MODELO XGBOOST
# =============================================
# scale_pos_weight compensa el desequilibrio de clases
# ratio = negativos / positivos
neg = (y_train == 0).sum()
pos = (y_train == 1).sum()
scale = neg / pos
print(f"\nDesequilibrio de clases — scale_pos_weight: {scale:.3f}")

model = XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    scale_pos_weight=scale,
    use_label_encoder=False,
    eval_metric='logloss',
    random_state=42,
    n_jobs=-1
)

print("\nEntrenando modelo...")
model.fit(X_train, y_train)

# =============================================
# 5. EVALUACIÓN
# =============================================
y_pred  = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

roc_auc = roc_auc_score(y_test, y_proba)
print(f"\n=== MÉTRICAS DE EVALUACIÓN ===")
print(f"ROC-AUC: {roc_auc:.4f}")
print(f"\nClassification Report:")
print(classification_report(y_test, y_pred, target_names=['Fail', 'Compliant']))

# Validación cruzada estratificada sobre train
cv = StratifiedKFold(n_splits=5, shuffle=False)
cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring='roc_auc')
print(f"\nCross-validation ROC-AUC (5-fold estratificado, sin shuffle):")
print(f"  Folds: {cv_scores.round(4)}")
print(f"  Media: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

# =============================================
# 6. FEATURE IMPORTANCE
# =============================================
importance = pd.DataFrame({
    'feature': FEATURES,
    'importance': model.feature_importances_
}).sort_values('importance', ascending=False)

print(f"\n=== FEATURE IMPORTANCE ===")
print(importance.to_string(index=False))

fig, ax = plt.subplots(figsize=(10, 6))
importance.sort_values('importance').plot(
    kind='barh', x='feature', y='importance',
    ax=ax, color='#3498db', edgecolor='white', legend=False
)
ax.set_title(f'Feature Importance — XGBoost\nROC-AUC: {roc_auc:.4f}',
             fontsize=14, fontweight='bold')
ax.set_xlabel('Importance')
plt.tight_layout()
plt.savefig('analysis/outputs/04_feature_importance.png', dpi=150)
plt.close()
print("\nGráfica guardada: 04_feature_importance.png")

# Curva ROC
fig, ax = plt.subplots(figsize=(8, 6))
RocCurveDisplay.from_predictions(y_test, y_proba, ax=ax)
ax.set_title(f'Curva ROC — XGBoost\nROC-AUC: {roc_auc:.4f}', fontsize=14, fontweight='bold')
ax.plot([0,1],[0,1],'k--', alpha=0.5)
plt.tight_layout()
plt.savefig('analysis/outputs/05_roc_curve.png', dpi=150)
plt.close()
print("Gráfica guardada: 05_roc_curve.png")

# Matriz de confusión
fig, ax = plt.subplots(figsize=(7, 6))
ConfusionMatrixDisplay.from_predictions(
    y_test, y_pred,
    display_labels=['Fail', 'Compliant'],
    ax=ax, colorbar=False,
    cmap='Blues'
)
ax.set_title('Matriz de Confusión — Test Set 2024-2026',
             fontsize=13, fontweight='bold')
plt.tight_layout()
plt.savefig('analysis/outputs/06_confusion_matrix.png', dpi=150)
plt.close()
print("Gráfica guardada: 06_confusion_matrix.png")

# =============================================
# 7. SERIALIZACIÓN DEL MODELO
# =============================================
joblib.dump(model, 'analysis/model/xgboost_compliance.joblib')
joblib.dump({
    'facility_type': le_facility,
    'risk_level': le_risk,
    'inspection_type': le_type
}, 'analysis/model/label_encoders.joblib')

print("\nModelo serializado: analysis/model/xgboost_compliance.joblib")
print("Encoders serializados: analysis/model/label_encoders.joblib")
print("\n=== MODELO COMPLETADO ===")
