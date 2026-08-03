# IMDb Data Engineering Project

## Overview

This project builds an end-to-end data engineering pipeline using the public IMDb bulk datasets.

The pipeline is designed to:

- download the required IMDb source files;
- convert the raw TSV data to Parquet;
- load the data into DuckDB;
- transform and test the data with dbt;
- orchestrate the complete workflow with Apache Airflow;
- build a query-ready star schema;
- preserve historical changes through dbt snapshots;
- answer the analytical questions required by the project specification.


---

## Project Objectives

The main objective is to create a reproducible local data platform that can answer questions about IMDb titles, ratings, people, roles, genres, and runtime trends.

The final warehouse should support analyses such as:

1. Who are the top 10 directors by average rating, restricted to directors with at least 5 titles and at least 1,000 combined votes?
2. Has average movie runtime changed by decade, and does runtime correlate with rating?
3. Which genres have the highest ratio of hidden gems compared with overrated titles?
4. Who are the most consistently well-rated directors and actors from the last 20 years, and in which genres are they concentrated?

---

## Technology Stack

- **Apache Airflow** — workflow orchestration
- **DuckDB** — local analytical warehouse
- **dbt** — transformations, tests, snapshots, and lineage documentation
- **Parquet** — raw landed data format
- **Docker Compose** — local environment
- **Python** — extraction, conversion, validation, and orchestration logic
- **Git/GitHub** — version control

---

## Architecture

```text
IMDb bulk datasets
        ↓
Airflow extraction tasks
        ↓
TSV.GZ source files
        ↓
Parquet conversion
        ↓
DuckDB raw tables or views
        ↓
dbt staging models
        ↓
dbt intermediate reshaping
        ↓
dimensions, bridge tables, and fact table
        ↓
dbt snapshots and tests
        ↓
analytical marts
        ↓
dbt docs and final reporting
```

The Airflow DAG is organized into four required logical stages:

```text
extract → load → transform → validate
```

The pipeline is designed to be idempotent, meaning that rerunning the same workflow should not create duplicate records.

---

## IMDb Data Sources

The project uses the following public IMDb datasets:

| Source file | Purpose |
|---|---|
| `title.basics.tsv.gz` | Core title metadata, years, runtime, and genres |
| `title.ratings.tsv.gz` | Average rating and vote count per title |
| `title.crew.tsv.gz` | Directors and writers per title |
| `title.principals.tsv.gz` | Principal cast and crew members |
| `name.basics.tsv.gz` | Person metadata and known-for titles |
| `title.akas.tsv.gz` | Alternative titles, regions, and languages |

Source:

```text
https://datasets.imdbws.com/
```

The downloaded source files and generated Parquet files are not committed to Git. They are created locally by the Airflow ingestion pipeline.

---

## Project Structure

```text
imdb-data-engineering/
├── dags/
│   └── ingest_dag.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── snapshots/
│   ├── tests/
│   ├── macros/
│   ├── sources.yml
│   └── dbt_project.yml
├── raw/
│   └── .gitkeep
├── docs/
│   ├── project_notes.md
│   └── project_documentation.docx
├── warehouse.duckdb
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

### Folder Responsibilities

| Path | Responsibility |
|---|---|
| `dags/` | Airflow DAGs and orchestration logic |
| `dbt_project/models/staging/` | Renaming, casting, null handling, and source cleanup |
| `dbt_project/models/intermediate/` | Reshaping and exploding multi-value columns |
| `dbt_project/models/marts/` | Dimensions, facts, bridge tables, and analytical marts |
| `dbt_project/snapshots/` | Historical tracking with dbt snapshots |
| `dbt_project/tests/` | Custom SQL data quality tests |
| `raw/` | Generated Parquet files; ignored by Git |
| `docs/` | Notes, screenshots, and final written documentation |
| `warehouse.duckdb` | Local DuckDB warehouse; ignored by Git |

---

## Data Pipeline

The final Airflow DAG is expected to follow this sequence:

```text
prepare_directories
        ↓
download_imdb_files
        ↓
convert_to_parquet
        ↓
load_to_duckdb
        ↓
validate_raw_data
        ↓
dbt_run_staging
        ↓
dbt_snapshot
        ↓
dbt_run_marts
        ↓
dbt_test
        ↓
dbt_docs_generate
```

### Pipeline Requirements

The DAG must:

- contain at least four logical stages;
- define visible task dependencies;
- configure at least one retry for failed tasks;
- produce meaningful log output;
- fail on critical validation or transformation errors;
- be idempotent;
- run end-to-end without manual intervention.

---

## dbt Modeling

### Staging Models

The project includes one staging model per raw source:

```text
stg_title_basics
stg_title_ratings
stg_title_crew
stg_title_principals
stg_name_basics
stg_title_akas
```

Staging models should contain only:

- column renaming;
- type casting;
- null handling;
- trimming;
- basic standardization.

IMDb uses `\N` to represent missing values. These values are converted to SQL `NULL`.

### Intermediate Models

Intermediate models are used for reshaping multi-value columns such as:

- genres;
- directors;
- writers;
- known-for titles.

Expected models include:

```text
stg_title_genres
int_title_directors
int_title_writers
int_person_known_for
```

### Star Schema

The warehouse follows a star-schema design.

#### Fact Table

```text
fct_title_ratings
```

**Grain:** one row per IMDb title.

Suggested measures:

- `average_rating`
- `num_votes`

#### Dimensions

```text
dim_title
dim_person
dim_genre
dim_year
```

#### Bridge Tables

```text
bridge_title_genre
bridge_title_crew
bridge_person_known_for_title
```

The bridge tables preserve many-to-many relationships without breaking the one-row-per-title grain of the fact table.

---

## Snapshots and Historical Tracking

### Ratings Snapshot

A dbt snapshot tracks changes in:

```text
average_rating
num_votes
```

Unique key:

```text
title_id
```

This preserves rating and vote-count history across successive IMDb loads.

### `dim_title` SCD Type 2

The required slowly changing dimension tracks changes to title metadata such as:

```text
primary_title
genres
runtime_minutes
```

Historical versions are identified using dbt snapshot metadata such as:

```text
dbt_scd_id
dbt_valid_from
dbt_valid_to
```

This prevents metadata corrections from silently overwriting previous values.

---

## Data Quality Tests

The project must include at least five dbt tests, including:

- `not_null`;
- `unique`;
- `relationships`;
- at least one custom SQL test;
- at least one IMDb-specific domain rule.

Planned checks include:

- primary keys are unique and non-null;
- `average_rating` is between 0 and 10;
- `start_year` is not in the future;
- `num_votes` is non-negative;
- runtime is positive;
- bridge tables contain no duplicate relationships;
- fact and bridge foreign keys reference valid dimension records.

Known dirty data may use:

```yaml
severity: warn
```

Such warnings must be justified in the final documentation.

---

## Analytical Marts

The final dbt marts include:

### `mart_top_directors`

Returns the top 10 directors who have:

- at least 5 titles;
- at least 1,000 combined votes.

### `mart_runtime_by_decade`

Calculates:

- movie count per decade;
- average runtime;
- average rating;
- runtime-rating correlation.

### `mart_genre_quality_analysis`

Compares hidden gems and overrated titles by genre.

Example definitions:

```text
Hidden gem:
average_rating >= 8.0
100 <= num_votes < 10,000

Overrated:
average_rating <= 6.0
num_votes >= 100,000
```

The final thresholds must be documented and justified.

### `mart_person_genre_performance`

Supports analysis of consistently well-rated directors and actors from the last 20 years and the genres in which they are concentrated.

---

## Prerequisites

Install:

- Docker Desktop
- Docker Compose
- Git

Recommended:

- VS Code
- DuckDB CLI or a compatible database client

Verify Docker:

```bash
docker --version
docker compose version
```

---

## Setup

Clone the repository:

```bash
git clone <repository-url>
cd imdb-data-engineering
```

Create local environment configuration if required:

```bash
cp .env.example .env
```

Start the environment:

```bash
docker compose up -d
```

Check the containers:

```bash
docker compose ps
```

Open the Airflow UI:

```text
http://localhost:8080
```

> The exact credentials and ports should be documented here after the Docker Compose setup is finalized.

---

## How to Run

### 1. Start the environment

```bash
docker compose up -d
```

### 2. Verify the Airflow services

```bash
docker compose ps
docker compose logs airflow-scheduler
```

### 3. Open Airflow

```text
http://localhost:8080
```

### 4. Trigger the IMDb ingestion DAG

Trigger:

```text
imdb_ingest_pipeline
```

The exact DAG ID should be updated here after implementation.

### 5. Verify generated raw files

Expected output:

```text
raw/
├── title_basics.parquet
├── title_ratings.parquet
├── title_crew.parquet
├── title_principals.parquet
├── name_basics.parquet
└── title_akas.parquet
```

### 6. Run dbt manually when troubleshooting

Enter the relevant container:

```bash
docker compose exec <airflow-service-name> bash
```

Then:

```bash
cd /opt/airflow/dbt_project
dbt debug
dbt run
dbt snapshot
dbt test
```

Replace `<airflow-service-name>` with the service that contains dbt.

### 7. Generate dbt documentation

```bash
dbt docs generate
dbt docs serve --host 0.0.0.0
```

The dbt docs port should be updated here after configuration.

---

## Rebuilding from a Clean State

To verify reproducibility:

1. Stop the environment
2. Remove generated Parquet files
3. Remove the local DuckDB database
4. Restart the environment
5. Trigger the Airflow DAG
6. Run all dbt tests
7. Generate dbt docs
8. Trigger the pipeline a second time
9. Verify that no duplicate records are created

Example cleanup commands:

```bash
docker compose down
rm -f raw/*.parquet
rm -f warehouse.duckdb
docker compose up -d
```

Windows PowerShell equivalent:

```powershell
docker compose down
Remove-Item raw/*.parquet -ErrorAction SilentlyContinue
Remove-Item warehouse.duckdb -ErrorAction SilentlyContinue
docker compose up -d
```

---

## Documentation

The final report is stored at:

```text
docs/project_documentation.docx
```

It should include:

- executive summary;
- dataset description;
- architecture overview;
- project structure walkthrough;
- data modeling explanation;
- fact-table grains;
- Airflow Graph View screenshot;
- DAG task explanations;
- dbt lineage screenshot;
- test descriptions;
- snapshot explanation;
- exact reproduction steps;
- answers to all deliverable questions;
- at least three challenges and lessons learned.

All screenshots must come from the actual project.

---

## Git and Data Files

The repository stores source code and configuration, not generated datasets.

The following files should remain ignored:

```text
*.tsv
*.tsv.gz
*.parquet
*.duckdb
```

Suggested `.gitignore` rules:

```gitignore
raw/*
!raw/.gitkeep

*.tsv
*.tsv.gz
*.parquet
*.duckdb
*.duckdb.wal

logs/
.env
.venv/
venv/
__pycache__/
*.pyc

dbt_project/target/
dbt_project/logs/
dbt_project/dbt_packages/

.vscode/
.idea/
.DS_Store
Thumbs.db
```

---

## Project Status

### Sprint 1 — Ingestion and Foundation

- [ ] Initialize repository
- [ ] Configure Airflow, DuckDB, and dbt
- [ ] Implement `title.ratings` vertical slice
- [ ] Ingest all six IMDb sources
- [ ] Convert raw data to Parquet
- [ ] Load data into DuckDB
- [ ] Implement validation and idempotency
- [ ] Start documentation

### Sprint 2 — Modeling and Testing

- [ ] Build six staging models
- [ ] Reshape multi-value columns
- [ ] Build dimensions
- [ ] Build bridge tables
- [ ] Build `fct_title_ratings`
- [ ] Implement ratings snapshot
- [ ] Implement `dim_title` SCD Type 2
- [ ] Add dbt tests
- [ ] Integrate dbt into Airflow

### Sprint 3 — Analytics and Delivery

- [ ] Build required analytical marts
- [ ] Answer deliverable questions
- [ ] Generate dbt docs
- [ ] Capture real screenshots
- [ ] Test clean-state reproducibility
- [ ] Complete README
- [ ] Complete written report
- [ ] Run final checklist

---

## License and Data Usage

This project uses the public IMDb non-commercial datasets.

Review the IMDb dataset usage terms before redistributing or using the data outside the scope of this educational project.

The raw IMDb files are not included in this repository.
