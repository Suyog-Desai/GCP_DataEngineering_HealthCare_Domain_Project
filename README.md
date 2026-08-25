# Healthcare Revenue Cycle Management (RCM) — GCP Data Engineering Project

An end-to-end batch data engineering pipeline that ingests, cleans, models, and aggregates healthcare Revenue Cycle Management (RCM) data from multiple hospital sources on Google Cloud Platform, using a Medallion (Bronze / Silver / Gold) architecture orchestrated with Cloud Composer (Airflow) and deployed via CI/CD with Cloud Build.

## Table of Contents

- [Business Context](#business-context)
- [Architecture](#architecture)
- [Data Sources](#data-sources)
- [Tech Stack](#tech-stack)
- [Repository Structure](#repository-structure)
- [Pipeline Walkthrough](#pipeline-walkthrough)
  - [1. Data Sources Setup](#1-data-sources-setup)
  - [2. Ingestion — Source to GCS Landing](#2-ingestion--source-to-gcs-landing)
  - [3. Bronze Layer — Raw Standardized Tables](#3-bronze-layer--raw-standardized-tables)
  - [4. Silver Layer — Cleaned, Conformed, Historized Data](#4-silver-layer--cleaned-conformed-historized-data)
  - [5. Gold Layer — Business/Reporting Tables](#5-gold-layer--businessreporting-tables)
  - [6. Orchestration — Cloud Composer (Airflow)](#6-orchestration--cloud-composer-airflow)
  - [7. CI/CD — Cloud Build](#7-cicd--cloud-build)
- [Key Engineering Techniques](#key-engineering-techniques)
- [Setup / Reproduce](#setup--reproduce)
- [Gold Layer Tables](#gold-layer-tables)
- [Cost Note](#cost-note)
- [Author](#author)

## Business Context

Revenue Cycle Management (RCM) is the financial process healthcare providers use to track a patient's journey — from registration and appointment scheduling through treatment, billing, claims, and final payment collection. This project simulates a real-world data platform for a healthcare organization operating **two hospitals**, each with its own Electronic Medical Record (EMR) system, plus monthly insurance **claims** files and standard **CPT (Current Procedural Terminology)** codes.

As a data engineer, the goal is to extract data from these disparate sources, build reliable ETL/ELT pipelines, and produce clean, analysis-ready tables that support revenue cycle dashboards and KPIs — patient summaries, provider performance, department performance, payor performance, and financial metrics.

## Architecture

```
 ┌─────────────┐   ┌─────────────┐   ┌──────────────┐   ┌──────────────┐
 │ Cloud SQL   │   │ Cloud SQL   │   │  Claims CSV  │   │ CPT Codes    │
 │ Hospital A  │   │ Hospital B  │   │ (monthly, per│   │ CSV (static  │
 │ (MySQL EMR) │   │ (MySQL EMR) │   │  hospital)   │   │ reference)   │
 └──────┬──────┘   └──────┬──────┘   └──────┬───────┘   └──────┬───────┘
        │  Dataproc (PySpark, JDBC, metadata-driven)            │ (dropped
        │  incremental / full load, archiving                   │  directly)
        ▼                  ▼                  ▼                  ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │                     GCS Landing Zone (raw JSON/CSV)                │
 │   /landing/hospital-a/*  /landing/hospital-b/*  /landing/claims/*  │
 │   /landing/cptcodes/*                /archive/... (dated history)  │
 └───────────────────────────────────────────────────────────────────┘
        │ BigQuery External Tables         │ Dataproc (PySpark)
        ▼                                   ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │  BRONZE (BigQuery) — raw external/native tables, 1:1 with source   │
 └───────────────────────────────────────────────────────────────────┘
        │ BigQuery SQL (MERGE / TRUNCATE+LOAD)
        ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │  SILVER (BigQuery) — Common Data Model, SCD Type 2, data-quality   │
 │  flags (is_quarantined), full-load truncate+load for dimensions    │
 └───────────────────────────────────────────────────────────────────┘
        │ BigQuery SQL (joins/aggregations)
        ▼
 ┌───────────────────────────────────────────────────────────────────┐
 │  GOLD (BigQuery) — business/reporting tables: patient history,     │
 │  provider performance, department performance, payor performance,  │
 │  financial metrics                                                  │
 └───────────────────────────────────────────────────────────────────┘
        │
        ▼
        BI / Dashboards & Reports

 Orchestration: Cloud Composer (Airflow) — DAG_Master → DAG_Pyspark → DAG_BQ
 CI/CD: GitHub → Cloud Build trigger → syncs workflows/ and data/ to the
        Composer environment's GCS bucket (DAGs + PySpark/SQL assets)
```

## Data Sources

| # | Source | Type | Load pattern |
|---|--------|------|---------------|
| 1 | Hospital A EMR | Cloud SQL (MySQL) | `patients`, `encounters`, `transactions` → Incremental (watermark: `ModifiedDate`); `providers`, `departments` → Full load |
| 2 | Hospital B EMR | Cloud SQL (MySQL) | Same 5 tables, incremental/full split as above |
| 3 | Insurance Claims | Flat CSV files, delivered monthly per hospital | Incremental |
| 4 | CPT Codes | Flat CSV, standardized medical procedure codes | Static/reference (truncate + load) |

Each hospital EMR exposes 5 tables: `patients`, `providers`, `departments`, `encounters`, `transactions`. Column naming is **not consistent** between the two hospitals (e.g., Hospital B uses `updated_date` where Hospital A uses `ModifiedDate`), which is intentional — it mirrors real-world source-system drift and is one of the reasons a Common Data Model is needed downstream.

## Tech Stack

- **Cloud SQL (MySQL)** — source-of-truth EMR databases for two hospitals
- **Google Cloud Storage (GCS)** — landing zone + archive (date-partitioned) + Composer DAG/code bucket
- **Dataproc (PySpark)** — JDBC extraction from MySQL, incremental/full-load logic, file archiving, JSON writes to GCS, claims/CPT processing
- **BigQuery** — Bronze (external tables), Silver (Common Data Model + SCD Type 2 via `MERGE`), Gold (aggregated reporting tables), plus `temp_dataset` for the metadata-driven audit log and pipeline logs
- **Cloud Composer (Airflow)** — DAG orchestration (`DAG_Master` → `DAG_Pyspark` → `DAG_BQ`)
- **Cloud Build + GitHub** — CI/CD that syncs `workflows/` (DAGs) and `data/` (PySpark + SQL scripts) to the Composer bucket on every push
- **Python / PySpark / SQL** — all transformation logic

## Repository Structure

```
├── cloudbuild.yaml                          # CI/CD pipeline definition
├── data/
│   ├── BQ/
│   │   ├── Bronze_external_table_query.sql  # Bronze external table DDL
│   │   ├── Silver_truncate_insert_query.sql # Silver: full-load tables + CDM
│   │   ├── silver_incremental_merge_changes.sql # Silver: SCD Type 2 MERGE logic
│   │   └── gold_tables_query.sql            # Gold: business/reporting tables
│   ├── EMR/
│   │   ├── hospital-a/  (ddl.sql + 5 sample CSVs)
│   │   └── hospital-b/  (ddl.sql + 5 sample CSVs)
│   ├── INGESTION/
│   │   ├── hospitalA_mysqlToLanding.py      # MySQL → GCS landing (Hospital A)
│   │   ├── hospitalB_mysqlToLanding.py      # MySQL → GCS landing (Hospital B)
│   │   ├── claims.py                        # Claims CSV → Bronze (via Dataproc)
│   │   └── cpt_codes.py                     # CPT codes CSV → Bronze (via Dataproc)
│   ├── claims/          (sample monthly claim files per hospital)
│   ├── cptcodes/        (sample CPT reference file)
│   └── config/
│       └── load_config.csv                  # Metadata-driven config: table, load type, watermark, active flag, target path
├── workflows/
│   ├── DAG_Master.py                        # Parent DAG — triggers Pyspark DAG then BQ DAG
│   ├── DAG_Pyspark.py                       # Starts Dataproc cluster, runs 4 PySpark jobs, stops cluster
│   └── DAG_BQ.py                            # Runs Bronze → Silver → Gold SQL in BigQuery
└── utils/
    ├── add_dags_to_composer.py              # Script invoked by Cloud Build to sync DAGs/data to Composer bucket
    └── requirements.txt
```

## Pipeline Walkthrough

### 1. Data Sources Setup
Two Cloud SQL (MySQL) instances simulate Hospital A and Hospital B EMR systems, each seeded with 5 tables (`patients`, `providers`, `departments`, `encounters`, `transactions`) via `ddl.sql` and sample CSVs. A GCS bucket holds the `config/`, `landing/`, and `temp/` prefixes; `claims/` and `cptcodes/` prefixes are pre-loaded with monthly claim files and the CPT reference file respectively, simulating client-delivered flat files.

### 2. Ingestion — Source to GCS Landing
A **metadata-driven** approach (`data/config/load_config.csv`) tells each PySpark ingestion job which tables are active, whether they're full or incremental, and which column is the watermark. `hospitalA_mysqlToLanding.py` / `hospitalB_mysqlToLanding.py`:
- Connect to Cloud SQL via JDBC
- Look up the last successful watermark from a BigQuery **audit table** (`temp_dataset`) for incremental tables (defaulting to an old date on first run)
- Pull only new/changed rows (incremental) or the full table (full load)
- **Archive** any existing file in the target landing path to a dated `archive/<table>/yyyy/mm/dd/` folder before writing the new extract
- Write results as JSON to `landing/<hospital>/<table>/`
- Log every step to both GCS and a BigQuery `pipeline_logs` table, and record run metadata (row count, timestamp, status) to the audit table

### 3. Bronze Layer — Raw Standardized Tables
- Hospital A/B tables: **BigQuery external tables** created directly on top of the GCS landing JSON files (no data movement, schema auto-detected) — `Bronze_external_table_query.sql`
- Claims and CPT codes: processed with a separate PySpark/Dataproc job that tags each record with its source hospital (parsed from the file path), deduplicates, and writes natively into BigQuery (`claims.py`, `cpt_codes.py`)

### 4. Silver Layer — Cleaned, Conformed, Historized Data
This is where the bulk of the modeling happens:
- **Common Data Model (CDM):** Hospital A and B tables are `UNION ALL`-ed together and standardized to common column names. A synthetic surrogate key (e.g., `Patient_Key = CONCAT(PatientID, '-', DataSource)`) is generated so that the same source ID from two different hospitals is never mistaken for a duplicate.
- **Data quality flag:** an `is_quarantined` boolean column marks rows with null business keys, so downstream Gold aggregations can exclude bad records without deleting them.
- **Full-load tables** (`providers`, `departments`, `cpt_codes`): truncate + reload every run — `Silver_truncate_insert_query.sql`
- **Incremental tables** (`patients`, `encounters`, `transactions`, `claims`): implemented with **SCD Type 2** via `MERGE` — `silver_incremental_merge_changes.sql`. New records are inserted as new active rows; changed records close out the previous row (`EndDate` set, `IsCurrent = FALSE`) and insert a new current version (`IsCurrent = TRUE`), preserving full history.

### 5. Gold Layer — Business/Reporting Tables
Business-use-case-driven aggregation queries join multiple Silver tables and truncate/load into Gold — `gold_tables_query.sql`:
- `patient_history` — full patient journey: demographics, encounters, transactions, claims
- `provider_charge_summary` / `provider_performance` — revenue and activity by provider
- `department_performance` — department-level rollups
- `payor_performance` — insurer/payor-level claim and payment analysis
- `financial_metrics` — overall RCM financial KPIs

### 6. Orchestration — Cloud Composer (Airflow)
Three DAGs in `workflows/`:
- **`DAG_Pyspark.py`** — starts the Dataproc cluster, submits the 4 PySpark jobs (Hospital A, Hospital B, Claims, CPT Codes) in sequence, then stops the cluster
- **`DAG_BQ.py`** — reads and submits the Bronze → Silver → Gold `.sql` files to BigQuery via `BigQueryInsertJobOperator`, in dependency order
- **`DAG_Master.py`** — the parent DAG (the only one with a schedule) that triggers `DAG_Pyspark` and then `DAG_BQ` using `TriggerDagRunOperator`, so PySpark ingestion always completes before BigQuery transformation begins

### 7. CI/CD — Cloud Build
A Cloud Build trigger fires on every push to the GitHub repo. It installs dependencies and runs `utils/add_dags_to_composer.py`, which copies `workflows/` → the Composer environment's `dags/` folder and `data/` → its `data/` folder, so any code change is live in Airflow within minutes — no manual file uploads.

## Key Engineering Techniques

- Metadata/config-driven pipeline (no hardcoded table lists)
- Incremental loading via watermark columns + BigQuery audit table
- File archiving strategy (dated partitions) before each new landing write
- Common Data Model to reconcile heterogeneous source schemas
- Slowly Changing Dimension Type 2 for historical tracking
- Data quality quarantine flagging (nulls) without discarding rows
- Medallion architecture (Bronze/Silver/Gold) in BigQuery
- Full pipeline logging + auditing (GCS + BigQuery)
- Workflow orchestration with DAG-of-DAGs dependency pattern
- CI/CD for Airflow DAG and code deployment

## Setup / Reproduce

> This project was built to be cost-conscious with free-tier GCP credits. Costly resources (Cloud SQL, Dataproc, Composer) should be stopped/deleted when not actively in use.

1. **Create Cloud SQL instances** for Hospital A and B (MySQL), load schema (`data/EMR/*/ddl.sql`) and sample data (`data/EMR/*/*.csv`).
2. **Create a GCS bucket** with `config/`, `landing/`, `temp/`, `claims/`, `cptcodes/` prefixes; upload `data/config/load_config.csv` and the sample claims/CPT files.
3. **Create BigQuery datasets:** `bronze_dataset`, `silver_dataset`, `gold_dataset`, `temp_dataset` (the last holds the audit log and pipeline logs tables).
4. **Create a Dataproc cluster** to test the PySpark ingestion scripts interactively (Jupyter) before automating.
5. **Create a Cloud Composer 3 environment**; note its GCS bucket name.
6. Update project ID / bucket names in `data/BQ/*.sql`, `workflows/*.py`, and `cloudbuild.yaml`.
7. **Set up a Cloud Build trigger** on this repository (push to branch) pointing at `cloudbuild.yaml`.
8. Push a commit — Cloud Build syncs DAGs/data to Composer automatically. Trigger `DAG_Master` in the Airflow UI (or wait for its schedule).
9. Verify Bronze/Silver/Gold tables populate in BigQuery.

## Cost Note

GCP resources used here (Cloud SQL, Dataproc, Composer) are billed hourly and are **not** part of the always-free tier. Stop or delete Cloud SQL instances, Dataproc clusters, and the Composer environment when the project is idle.

## Author

Suyog Desai
Repo: https://github.com/Suyog-Desai/GCP_DataEngineering_HealthCare_Domain_Project
