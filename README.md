# Chicago Food Inspections — End-to-End Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.11-blue)](https://python.org)
[![dbt](https://img.shields.io/badge/dbt-1.11-orange)](https://getdbt.com)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.3-green)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue)](https://postgresql.org)
[![XGBoost](https://img.shields.io/badge/XGBoost-ROC--AUC%200.89-red)](https://xgboost.readthedocs.io)

End-to-end data pipeline built on **311,714 real government inspection records** from the City of Chicago Open Data Portal. Covers data ingestion, multi-layer transformation with documented governance decisions, predictive modeling, orchestration, and executive dashboarding.

---

## Business Context

The Chicago Department of Public Health conducts food safety inspections across 16,000+ establishments. This project models compliance risk, identifies repeat offenders, and surfaces actionable patterns for resource prioritization — directly applicable to food & beverage companies (PepsiCo, Mondelez), regulated industrial environments (CEMEX, Ternium, Caterpillar), and financial risk teams (HeyBanco).

**Key findings:**
- 67% overall compliance rate across 311K inspections (2010–2026)
- Compliance dropped from 68% (2015) to 63% (2021) during COVID-19 and is recovering
- 62% of inspections correspond to establishments with repeat fail history
- Grocery & Butcher (44%) and Wine Stores (43%) have the highest fail rates by facility type
- MCDONALD'S vs MCDONALDS appear as separate entities — a documented data quality finding

---

## Architecture

City of Chicago API (Socrata)
↓
raw_data.food_inspections (PostgreSQL)
↓
staging.stg_food_inspections (dbt view — 11 tests)
↓
marts.mart_food_inspections (dbt table — 13 tests)
↓
XGBoost Classifier + Power BI Dashboard
↑
Apache Airflow DAG (daily orchestration)


---

## Tech Stack

| Layer | Technology |
|---|---|
| Source | City of Chicago Open Data Portal (Socrata API) |
| Storage | PostgreSQL 16 |
| Transformation | dbt-core 1.11.11 + dbt-postgres 1.10.0 |
| Orchestration | Apache Airflow 2.9.3 (Docker) |
| Modeling | XGBoost 3.2, scikit-learn 1.9, pandas 3.0 |
| Visualization | Power BI Desktop (Import mode) |
| Environment | Python 3.11.9, WSL2 Ubuntu 24.04 |

---

## Data Governance

This project applies ISO 9001/17025-informed governance practices at every layer:

### Data Quality Findings (raw layer)
| Field | Issue | Volume | Decision |
|---|---|---|---|
| `city` | Case inconsistency: CHICAGO / Chicago / chicago / CCHICAGO | 745 records | Normalized to UPPER in staging |
| `facility_type` | 526 unique values — free text, no validation | Full dataset | Normalized + grouped; types with <10 records → OTHER |
| `violations` | 28.1% null | 87,612 records | NULL treated as no recorded violations; flagged separately |
| `risk` | Value 'All' outside domain (1/2/3) | 92 records | Mapped to UNKNOWN with flag |
| `results = Fail` + `violations = NULL` | Internal inconsistency | 1,036 records | Flagged as `flag_fail_without_violations` |
| `state` | Records from IN, CA, WI, CO | 89 records | Flagged as `flag_out_of_jurisdiction`; excluded from mart |

### Governance Flags (staging layer)
- `flag_out_of_jurisdiction` — city != CHICAGO
- `flag_fail_without_violations` — failed inspection with no violations recorded
- `flag_invalid_risk` — risk value outside domain
- `flag_missing_license` — no license number on record

### dbt Test Coverage
- **Staging:** 11 tests — unique, not_null, accepted_values
- **Mart:** 13 tests — unique, not_null, accepted_values
- **Total: 24 tests, 100% passing**

---

## Predictive Model

**Objective:** Classify whether an inspection will result in compliance (Pass / Pass w/ Conditions) or failure (Fail).

**Approach:**
- Target: `compliance_flag` (1 = compliant, 0 = fail)
- Excluded: Out of Business, No Entry, Not Ready (operational conditions, not evaluation outcomes)
- Split: temporal (train 2015–2023, test 2024–2026) — prevents data leakage
- Class imbalance handled via `scale_pos_weight`

**Results:**

| Metric | Value |
|---|---|
| ROC-AUC | 0.89 |
| Recall (Fail) | 84% |
| Training set | 142,491 records |
| Test set | 38,245 records |

**Top features:** `violation_count` (46%), `license_total_fails` (23%), `days_since_previous_inspection` (7%)

**Business interpretation:** The model correctly identifies 84 out of every 100 establishments that will fail — enabling proactive resource prioritization before failures occur.

---

## Pipeline Orchestration

Apache Airflow DAG `food_inspections_pipeline` runs daily at 06:00 UTC:

download_data → load_to_postgres → dbt_run → dbt_test


Sequential with strict dependencies — if any step fails, downstream tasks do not run. Equivalent to formal acceptance criteria in a regulated process.

---

## Project Structure

chicago-food-inspections/
├── data/
│ └── raw/ # Raw CSV (gitignored — 326MB)
├── ingestion/
│ └── load_food_inspections.py
├── dbt_project/
│ ├── models/
│ │ ├── staging/ # stg_food_inspections + 11 tests
│ │ └── marts/ # mart_food_inspections + 13 tests
│ └── macros/
│ └── generate_schema_name.sql
├── analysis/
│ ├── eda.py
│ └── model.py
├── orchestration/
│ ├── docker-compose.yml
│ └── dags/
│ └── food_inspections_pipeline.py
└── requirements.txt


---

## STAR Metrics

| Metric | Value |
|---|---|
| Records processed | 311,714 |
| Data quality issues detected | 6 types documented, 95,000+ records flagged |
| dbt tests implemented | 24 (100% passing) |
| Model ROC-AUC | 0.89 on unseen 2024–2026 data |
| Fail recall | 84% |
| Pipeline runtime | ~3 min ingestion + ~10 sec dbt |
| Time span covered | 16 years (2010–2026) |

---

## For Recruiters

### Analytics Engineer / Data Engineer Jr
Built a production-grade pipeline with layered architecture (raw → staging → mart), 24 automated data quality tests, and documented governance decisions at each transformation step. Used dbt with custom macros, temporal split strategy to prevent data leakage, and Airflow for daily orchestration.

### Data Scientist Jr
Trained an XGBoost classifier on 180K real government records achieving ROC-AUC 0.89. Applied temporal train/test split to prevent leakage, handled class imbalance via scale_pos_weight, and validated with 5-fold cross-validation. Top predictor: violation history (46% importance).

### Data Analyst
Built a two-page Power BI dashboard (Operational + Executive views) on 311K records with DAX measures, Calendar table, and interactive year slicer. Surfaces compliance trends, repeat offenders, and facility-type risk — directly actionable for food safety resource allocation.

---

## Governance & Ethics

This project was built with a scientific mindset informed by real experience in ISO 17025/9001 regulated environments:

- **Traceability:** every transformation decision is documented in dbt model headers with justification and alternatives considered
- **No silent drops:** records with quality issues are flagged and preserved, not silently deleted
- **Documented exclusions:** the 386 out-of-jurisdiction records are explicitly excluded in the mart with a documented criterion, not filtered without explanation
- **Temporal integrity:** the predictive model uses a temporal split — training on past data, evaluating on future data — to reflect real-world deployment conditions
- **Source attribution:** data sourced directly from the City of Chicago Open Data Portal via Socrata API; ingestion timestamp recorded as `ingested_at` in raw layer

---

## Setup

```bash
git clone git@github.com:MonicaVenzor/chicago-food-inspections-pipeline.git
cd chicago-food-inspections-pipeline
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Download data
curl -o data/raw/food_inspections.csv \
  "https://data.cityofchicago.org/api/views/4ijn-s7e5/rows.csv?accessType=DOWNLOAD"

# Load to PostgreSQL
python ingestion/load_food_inspections.py

# Run dbt
cd dbt_project && dbt run && dbt test

# Run model
cd .. && python analysis/model.py
```

---

*Data source: [City of Chicago Open Data Portal](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5)*
