# IMDb Data Engineering Project — Sprint Plan

## Project Goal

Build an end-to-end IMDb data engineering pipeline using:

- Apache Airflow for orchestration
- DuckDB as the local warehouse
- dbt for transformation, testing, snapshots, and documentation
- Parquet for raw landed data
- Git/GitHub for version control

The project is organized into three logical sprints:

1. Ingestion and project foundation
2. dbt modeling, star schema, snapshots, and testing
3. Analytics, reproducibility, documentation, and final delivery

---

# Sprint 1 — Ingestion and Project Foundation

## Sprint Objective

Create the technical foundation of the project and build an Airflow pipeline that:

- downloads the required IMDb datasets;
- converts them to Parquet;
- stores them in `raw/`;
- exposes the data to DuckDB and dbt;
- can be rerun without creating duplicates.

## Expected Sprint Result

```text
IMDb TSV.GZ files
        ↓
Apache Airflow
        ↓
Parquet files
        ↓
DuckDB
        ↓
First dbt staging model
```

---

## Task 1.1 — Initialize the Git Repository

Create the repository and the required folder structure.

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
│   └── dbt_project.yml
├── raw/
├── docs/
│   ├── project_notes.md
│   └── project_documentation.docx
├── warehouse.duckdb
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

Add at least the following entries to `.gitignore`:

```gitignore
raw/
*.duckdb
logs/
.env
__pycache__/
dbt_project/target/
dbt_project/logs/
dbt_project/dbt_packages/
```

---

## Task 1.2 — Configure Airflow, DuckDB, and dbt

Configure the development environment so that:

- Airflow runs through Docker Compose;
- the `dags/` folder is mounted correctly;
- the `raw/` folder is accessible from the Airflow containers;
- DuckDB is accessible from dbt;
- `dbt debug` succeeds.

Useful commands:

```bash
docker compose up -d
docker compose ps
docker compose logs airflow-scheduler
```

For dbt:

```bash
dbt debug
```

### Acceptance Criteria

- [ ] Airflow UI is accessible
- [ ] Scheduler and webserver are running
- [ ] A test DAG appears in the Airflow UI
- [ ] `dbt debug` succeeds
- [ ] DuckDB can execute a simple query
- [ ] Docker paths and mounted volumes are verified

---

## Task 1.3 — Build a Vertical Slice with `title.ratings`

Start with one complete source before implementing all files.

Source:

```text
title.ratings.tsv.gz
```

Implement the following flow:

1. Download the file
2. Read the compressed TSV
3. Convert IMDb `\N` values to `NULL`
4. Convert the data to Parquet
5. Save it as `raw/title_ratings.parquet`
6. Declare it in `sources.yml`
7. Create `stg_title_ratings`
8. Add initial dbt tests

Expected staging columns:

```text
title_id
average_rating
num_votes
```

Useful commands:

```bash
dbt run --select stg_title_ratings
dbt test --select stg_title_ratings
```

### Acceptance Criteria

- [ ] File downloads successfully
- [ ] File is converted to Parquet
- [ ] `\N` is treated as null
- [ ] dbt source is declared
- [ ] `stg_title_ratings` runs successfully
- [ ] At least one test passes
- [ ] The model can be queried from DuckDB

---

## Task 1.4 — Implement Ingestion for All Required IMDb Files

Required sources:

```text
title.basics.tsv.gz
title.ratings.tsv.gz
title.crew.tsv.gz
title.principals.tsv.gz
name.basics.tsv.gz
title.akas.tsv.gz
```

Expected raw files:

```text
raw/
├── title_basics.parquet
├── title_ratings.parquet
├── title_crew.parquet
├── title_principals.parquet
├── name_basics.parquet
└── title_akas.parquet
```

For every file:

1. Download the source
2. Validate that the file is not empty
3. Convert it to Parquet
4. Log the number of processed rows
5. Save it separately in `raw/`
6. Replace the previous version safely

### Acceptance Criteria

- [ ] All six sources are downloaded
- [ ] All six sources are converted to Parquet
- [ ] Each source produces useful log output
- [ ] Invalid or empty downloads fail the task
- [ ] Raw data is not left only as TSV or CSV

---

## Task 1.5 — Build the First Airflow DAG Stages

The first version of the DAG should include:

```text
prepare_directories
        ↓
download_files
        ↓
convert_to_parquet
        ↓
validate_raw_files
```

Recommended logical groups:

```text
extract
convert
validate
```

Each task should include at least one retry:

```python
retries=1
retry_delay=timedelta(minutes=2)
```

Useful log messages should include:

- source file name;
- source URL;
- downloaded file size;
- row count;
- Parquet output path;
- validation result.

### Acceptance Criteria

- [ ] Dependencies are visible in Airflow Graph View
- [ ] Failed tasks retry at least once
- [ ] Logs are meaningful
- [ ] Critical validation failures stop the pipeline
- [ ] No credentials or configuration values are hard-coded unnecessarily

---

## Task 1.6 — Implement Idempotency

Rerunning the same DAG execution must not create duplicate records.

Recommended approach:

```text
download temporary file
        ↓
convert to temporary Parquet
        ↓
validate
        ↓
replace final Parquet file
```

Example:

```text
title_basics.tmp.parquet
        ↓
title_basics.parquet
```

Avoid appending full IMDb snapshots on every run.

### Acceptance Criteria

- [ ] DAG can run twice successfully
- [ ] The second run does not duplicate rows
- [ ] Temporary files are cleaned up
- [ ] Failed conversions do not overwrite valid output files
- [ ] Row counts remain stable when the source has not changed

---

## Sprint 1 Documentation Tasks

### Update `README.md`

Document:

- project name;
- project objective;
- technology stack;
- initial architecture;
- project structure;
- startup commands.

### Update `docs/project_notes.md`

Record:

- dataset choice;
- the six IMDb sources;
- important technical decisions;
- encountered problems;
- solutions and workarounds;
- row counts and file sizes.

### Start `docs/project_documentation.docx`

Draft the following sections:

#### Executive Summary

Include:

- what the project does;
- which dataset it uses;
- which technologies it uses;
- what analytical questions it will answer.

#### Dataset Description

Include:

- source;
- original format;
- update frequency;
- files used;
- approximate size and row count;
- scope decisions.

#### Architecture Overview

Initial architecture:

```text
IMDb → Airflow → Parquet → DuckDB → dbt
```

#### Project Structure Walkthrough

Explain each project folder in one sentence.

#### Challenges and Lessons Learned

Record issues immediately, such as:

- Docker volume configuration;
- Windows versus container paths;
- large source files;
- memory usage;
- `\N` null handling;
- download failures.

---

## Sprint 1 Definition of Done

- [ ] All six IMDb files are downloaded
- [ ] All six IMDb files are stored as Parquet
- [ ] Airflow runs through Docker Compose
- [ ] Tasks include retries and useful logs
- [ ] Pipeline is idempotent
- [ ] `stg_title_ratings` works
- [ ] dbt can connect to DuckDB
- [ ] README contains setup instructions
- [ ] Documentation has been started
- [ ] Sprint changes are committed

Suggested commit:

```bash
git commit -m "Implement IMDb ingestion pipeline and project foundation"
```

---

# Sprint 2 — dbt Modeling, Star Schema, Snapshots, and Testing

## Sprint Objective

Create the complete dbt transformation layer:

```text
staging
    ↓
intermediate reshaping
    ↓
dimensions, bridges, and fact table
    ↓
snapshots
    ↓
dbt tests
```

The main challenges of this sprint are:

- multi-table modeling;
- many-to-many relationships;
- bridge tables;
- star schema design;
- SCD Type 2;
- data quality validation.

---

## Task 2.1 — Create All Staging Models

Required models:

```text
stg_title_basics
stg_title_ratings
stg_title_crew
stg_title_principals
stg_name_basics
stg_title_akas
```

Staging models should contain only:

- renaming;
- type casting;
- null handling;
- trimming;
- basic standardization.

Do not add analytical aggregations or business classifications in staging.

Recommended grains:

| Model | Grain |
|---|---|
| `stg_title_basics` | One row per title |
| `stg_title_ratings` | One row per title |
| `stg_title_crew` | One row per title |
| `stg_title_principals` | One row per title and ordering position |
| `stg_name_basics` | One row per person |
| `stg_title_akas` | One row per title and alternative title |

### Acceptance Criteria

- [ ] All six staging models exist
- [ ] All required types are cast correctly
- [ ] `\N` values are converted to null
- [ ] Staging models contain no unnecessary business logic
- [ ] `dbt run --select staging` succeeds

---

## Task 2.2 — Reshape Multi-Value Columns

Create models that explode list-like columns into separate rows.

Required transformations:

```text
genres
directors
writers
knownForTitles
```

Recommended models:

```text
stg_title_genres
int_title_directors
int_title_writers
int_person_known_for
```

Example transformation:

```text
Drama,Crime,Thriller
```

becomes:

```text
title_id | genre_name
```

Validation rules:

- remove empty values;
- remove `\N`;
- trim whitespace;
- deduplicate key combinations.

### Acceptance Criteria

- [ ] Genres are split into separate rows
- [ ] Directors are split into separate rows
- [ ] Writers are split into separate rows
- [ ] `knownForTitles` is exploded
- [ ] No empty list elements remain
- [ ] Duplicate key combinations are removed

---

## Task 2.3 — Build the Dimensions

Required dimensions:

```text
dim_title
dim_person
dim_genre
dim_year
```

### `dim_title`

Suggested columns:

```text
title_id
primary_title
original_title
title_type
is_adult
start_year
end_year
runtime_minutes
genres
```

### `dim_person`

Suggested columns:

```text
person_id
primary_name
birth_year
death_year
primary_professions
```

### `dim_genre`

Suggested columns:

```text
genre_key
genre_name
```

### `dim_year`

Suggested columns:

```text
year_key
year
decade
```

### Acceptance Criteria

- [ ] All dimensions exist
- [ ] Every dimension has a documented grain
- [ ] Every dimension has a primary key
- [ ] Primary keys are stable
- [ ] Column names and types are consistent

---

## Task 2.4 — Build Bridge Tables

### `bridge_title_genre`

Grain:

> One row represents one title-to-genre association.

Suggested columns:

```text
title_id
genre_key
```

### `bridge_title_crew`

Grain:

> One row represents one title-person-role association.

Suggested columns:

```text
title_id
person_id
role
job
characters
ordering
```

Build it from:

- `title.crew`;
- `title.principals`.

Relevant standardized roles should include:

```text
director
writer
actor
```

Other roles may be preserved when useful.

Deduplicate on:

```text
title_id, person_id, role
```

### Acceptance Criteria

- [ ] `bridge_title_genre` exists
- [ ] `bridge_title_crew` exists
- [ ] `title.crew` and `title.principals` are combined correctly
- [ ] Roles are standardized
- [ ] Duplicate title-person-role combinations are removed
- [ ] Every foreign key points to a valid dimension record

---

## Task 2.5 — Build `fct_title_ratings`

Required fact table:

```text
fct_title_ratings
```

Required grain:

> One row per IMDb title.

Suggested columns:

```text
title_id
year_key
average_rating
num_votes
```

Optional:

```text
runtime_minutes
```

Do not join genres directly into the fact table because one title can have several genres, which could duplicate rating measures.

### Acceptance Criteria

- [ ] Fact table exists
- [ ] Grain is one row per title
- [ ] No duplicate title IDs exist
- [ ] Rating and vote measures are present
- [ ] Required dimension relationships are available
- [ ] Grain is documented explicitly

---

## Task 2.6 — Implement the Ratings Snapshot

Create a dbt snapshot for:

```text
title.ratings
```

Track changes in:

```text
average_rating
num_votes
```

Use:

```text
title_id
```

as the unique key.

This snapshot tracks changes in measures between IMDb loads.

### Acceptance Criteria

- [ ] Snapshot exists
- [ ] Unique key is configured
- [ ] Rating changes create a new version
- [ ] Vote count changes create a new version
- [ ] Historical rows contain validity metadata

---

## Task 2.7 — Implement SCD Type 2 for `dim_title`

Create the required SCD Type 2 snapshot for title metadata.

Track at least:

```text
primary_title
genres
runtime_minutes
```

The snapshot should preserve:

```text
dbt_scd_id
dbt_valid_from
dbt_valid_to
```

Test it in development:

1. Run the snapshot
2. Change one controlled value
3. Run the snapshot again
4. Verify that two versions exist
5. Restore the original data

### Acceptance Criteria

- [ ] `dim_title` snapshot exists
- [ ] Required descriptive attributes are tracked
- [ ] Historical versions are preserved
- [ ] Current rows can be identified
- [ ] A controlled change test is documented

---

## Task 2.8 — Add dbt Tests

The project must include at least:

- `not_null`;
- `unique`;
- `relationships`;
- one custom SQL test;
- one real IMDb domain rule.

Recommended minimum: 10 tests.

### Primary Key Tests

Add `not_null` and `unique` to:

- `dim_title.title_id`;
- `dim_person.person_id`;
- `dim_genre.genre_key`;
- `dim_year.year_key`;
- `fct_title_ratings.title_id`.

### Relationships Tests

Add relationships for:

- fact → `dim_title`;
- fact → `dim_year`;
- `bridge_title_genre` → `dim_title`;
- `bridge_title_genre` → `dim_genre`;
- `bridge_title_crew` → `dim_title`;
- `bridge_title_crew` → `dim_person`.

### Domain Tests

Add tests for:

- `average_rating` between 0 and 10;
- `start_year` not in the future;
- `num_votes >= 0`;
- `runtime_minutes > 0`;
- accepted crew roles.

Use `severity: warn` only for legitimate dirty data and document the reason.

### Custom SQL Test

Example:

```sql
select
    title_id,
    person_id,
    role,
    count(*) as row_count
from {{ ref('bridge_title_crew') }}
group by title_id, person_id, role
having count(*) > 1
```

### Acceptance Criteria

- [ ] At least five dbt tests exist
- [ ] Every primary key is tested
- [ ] Relationships tests exist for all required joins
- [ ] Rating range is validated
- [ ] Future start years are rejected
- [ ] At least one custom SQL test exists
- [ ] Critical test failures stop the pipeline

---

## Task 2.9 — Integrate dbt into Airflow

Extend the DAG:

```text
prepare_directories
        ↓
download_files
        ↓
convert_to_parquet
        ↓
validate_raw_files
        ↓
dbt_run_staging
        ↓
dbt_snapshot
        ↓
dbt_run_dimensions_and_fact
        ↓
dbt_test
```

Critical dbt tasks must fail the DAG when they fail.

### Acceptance Criteria

- [ ] dbt staging runs from Airflow
- [ ] dbt snapshots run from Airflow
- [ ] dimensions, bridges, and fact run from Airflow
- [ ] dbt tests run from Airflow
- [ ] Failed tests stop downstream critical tasks
- [ ] Airflow Graph View clearly shows dependencies

---

## Sprint 2 Documentation Tasks

### Complete the Data Modeling Section

For every model document:

- grain;
- primary key;
- foreign keys;
- source;
- role;
- relationships;
- modeling justification.

### Add a Dimensional Model Diagram

Include:

```text
fct_title_ratings
dim_title
dim_person
dim_genre
dim_year
bridge_title_genre
bridge_title_crew
```

### Document the Snapshots

Explain:

- ratings snapshot;
- `dim_title` SCD Type 2;
- tracked columns;
- `dbt_valid_from`;
- `dbt_valid_to`;
- why history matters.

### Document Tests

Create a table such as:

| Test | Model | Purpose | Severity |
|---|---|---|---|
| `unique` | `dim_title` | Prevent duplicate title IDs | error |
| Rating range | `fct_title_ratings` | Validate rating between 0 and 10 | error |
| Runtime positive | `dim_title` | Detect implausible runtime values | warn |
| Relationships | `bridge_title_crew` | Validate person references | error |
| Custom duplicate test | `bridge_title_crew` | Prevent duplicate associations | error |

### Update Challenges and Lessons Learned

Possible topics:

- many-to-many modeling;
- exploding list columns;
- duplicate records from multiple crew sources;
- snapshot testing;
- large joins;
- memory consumption;
- dbt dependency order.

---

## Sprint 2 Definition of Done

- [ ] All six staging models run
- [ ] Multi-value fields are exploded
- [ ] All required dimensions exist
- [ ] Both bridge tables exist
- [ ] Fact table has one row per title
- [ ] Ratings snapshot works
- [ ] `dim_title` SCD Type 2 works
- [ ] At least five dbt tests exist
- [ ] At least one custom SQL test exists
- [ ] dbt is integrated into Airflow
- [ ] Data modeling and testing are documented
- [ ] Sprint changes are committed

Suggested commit:

```bash
git commit -m "Build IMDb star schema snapshots and dbt tests"
```

---

# Sprint 3 — Analytics, Reproducibility, Documentation, and Final Delivery

## Sprint Objective

Complete all required analytical deliverables and make the project ready for evaluation.

The final project should include:

- reusable dbt marts;
- all required analytical answers;
- final Airflow DAG;
- dbt documentation;
- real screenshots;
- reproducible setup;
- complete README;
- complete written report.

---

## Task 3.1 — Build the Top Directors Mart

Create:

```text
mart_top_directors
```

Required question:

> Who are the top 10 directors by average rating, restricted to directors with at least 5 titles and at least 1,000 combined votes?

Suggested columns:

```text
person_id
director_name
title_count
combined_votes
average_rating
weighted_average_rating
rank
```

Required filters:

```text
role = director
title_count >= 5
combined_votes >= 1000
```

### Acceptance Criteria

- [ ] At least five titles are required
- [ ] At least 1,000 combined votes are required
- [ ] Results are ranked
- [ ] Top 10 directors are returned
- [ ] The query is reproducible from a dbt model
- [ ] Result interpretation is documented

---

## Task 3.2 — Build the Runtime by Decade Mart

Create:

```text
mart_runtime_by_decade
```

Required analysis:

- average movie runtime by decade;
- movie count by decade;
- average rating by decade;
- runtime-rating correlation.

Suggested columns:

```text
decade
movie_count
average_runtime
average_rating
runtime_rating_correlation
```

DuckDB function:

```sql
corr(runtime_minutes, average_rating)
```

Recommended filters:

- `title_type = 'movie'`;
- non-null runtime;
- positive runtime;
- non-null rating.

### Acceptance Criteria

- [ ] Only movies are analyzed
- [ ] Decades are derived consistently
- [ ] Average runtime is calculated
- [ ] Correlation is calculated
- [ ] Invalid runtime values are handled
- [ ] Results are interpreted in documentation

---

## Task 3.3 — Build the Hidden Gems vs Overrated Mart

Create:

```text
mart_genre_quality_analysis
```

Define explicit analytical thresholds.

Example:

```text
Hidden gem:
average_rating >= 8.0
100 <= num_votes < 10,000

Overrated:
average_rating <= 6.0
num_votes >= 100,000
```

Suggested columns:

```text
genre_name
title_count
hidden_gem_count
overrated_count
hidden_gem_ratio
overrated_ratio
```

The thresholds are a project decision and must be justified.

### Acceptance Criteria

- [ ] Hidden gem criteria are defined
- [ ] Overrated criteria are defined
- [ ] Ratios are calculated per genre
- [ ] Genres with too few titles are handled appropriately
- [ ] Thresholds are documented
- [ ] Results are reproducible

---

## Task 3.4 — Build the Final Person-Genre Performance Mart

Create:

```text
mart_person_genre_performance
```

Recommended grain:

> One row per person-role-genre combination.

Suggested columns:

```text
person_id
person_name
role
genre_name
title_count
combined_votes
average_rating
weighted_average_rating
```

Use it to support the overall project goal:

> Identify the most consistently well-rated directors and actors from the last 20 years and the genres in which they are concentrated.

### Acceptance Criteria

- [ ] Grain is documented
- [ ] Directors and actors can be analyzed
- [ ] Genre concentration can be measured
- [ ] Last-20-years logic is explicit
- [ ] Metrics are reproducible
- [ ] Underlying joins are tested

---

## Task 3.5 — Finalize the Airflow DAG

Recommended final flow:

```text
prepare_directories
        ↓
download_files
        ↓
convert_to_parquet
        ↓
validate_raw_files
        ↓
dbt_run_staging
        ↓
dbt_snapshot
        ↓
dbt_run_marts
        ↓
dbt_test
        ↓
generate_dbt_docs
```

Final validation:

- at least four logical stages;
- visible dependencies;
- retries;
- meaningful logs;
- idempotency;
- critical failures stop the pipeline;
- no manual intervention required.

### Acceptance Criteria

- [ ] DAG runs end-to-end
- [ ] DAG appears correctly in Airflow UI
- [ ] Graph View is clear
- [ ] Retry behavior is configured
- [ ] Critical validation failures stop the run
- [ ] The final graph screenshot is captured

---

## Task 3.6 — Generate dbt Documentation

Run:

```bash
dbt docs generate
dbt docs serve
```

Verify:

- sources are visible;
- models are documented;
- important columns are documented;
- tests are visible;
- lineage is understandable;
- marts can be traced back to raw sources.

### Acceptance Criteria

- [ ] `dbt docs generate` succeeds
- [ ] dbt docs site is browsable
- [ ] Lineage graph is clear
- [ ] Final lineage screenshot is captured
- [ ] Models and columns contain useful descriptions

---

## Task 3.7 — Test the Project from a Clean State

Simulate a fresh setup:

1. Stop the containers
2. Delete `warehouse.duckdb`
3. Delete generated Parquet files
4. Start the environment
5. Run the DAG
6. Verify all models
7. Run all tests
8. Generate dbt docs
9. Run the DAG a second time

Compare:

- row counts;
- duplicate counts;
- snapshot behavior;
- mart results.

### Acceptance Criteria

- [ ] Project runs from an empty DuckDB file
- [ ] Raw files are recreated
- [ ] Models are rebuilt successfully
- [ ] Tests pass
- [ ] Second run does not create duplicates
- [ ] README steps match the real process

---

## Task 3.8 — Finalize `README.md`

Recommended structure:

```text
# IMDb Data Engineering Project

## Objective
## Architecture
## Technologies
## Project Structure
## IMDb Datasets
## Prerequisites
## Setup
## How to Run
## Airflow DAG
## dbt Models
## Snapshots
## Tests
## Deliverable Questions
## Known Limitations
```

### Acceptance Criteria

- [ ] Setup commands are complete
- [ ] Commands were verified from a clean state
- [ ] Architecture is explained
- [ ] Data sources are listed
- [ ] Airflow and dbt usage is explained
- [ ] Known limitations are documented

---

## Task 3.9 — Finalize the Written Report

Required file:

```text
docs/project_documentation.docx
```

Recommended length:

```text
6–10 pages
```

Required sections:

### 1. Executive Summary

Explain in one paragraph:

- what the project does;
- which technologies are used;
- what business or analytical question it answers.

### 2. Dataset Description

Include:

- source;
- size;
- update frequency;
- files used;
- scope decisions;
- limitations.

### 3. Architecture Overview

Include:

- architecture diagram;
- Airflow, DuckDB, and dbt roles;
- schedule;
- trigger sequence.

### 4. Project Structure Walkthrough

Explain each folder and its purpose.

### 5. Data Modeling Explanation

Include:

- dimensions;
- fact table;
- bridge tables;
- grain of every important model;
- keys;
- relationships;
- many-to-many decisions;
- SCD Type 2 behavior.

### 6. Airflow DAG Explanation

Include:

- real Graph View screenshot;
- task descriptions;
- dependencies;
- retry configuration;
- schedule;
- idempotency behavior.

### 7. dbt Lineage and Tests

Include:

- real lineage screenshot;
- model layers;
- snapshot explanation;
- test list;
- reason for every important test.

### 8. How to Run

Provide exact steps from a clean DuckDB file.

### 9. Answers to Deliverable Questions

For every question include:

1. the original question;
2. model or query used;
3. result;
4. one-sentence interpretation.

### 10. Challenges and Lessons Learned

Document at least three real problems and how they were solved.

### Acceptance Criteria

- [ ] All required sections exist
- [ ] Screenshots come from the student's own project
- [ ] Every fact grain is documented
- [ ] Snapshot behavior is documented
- [ ] Tests are explained
- [ ] All analytical questions are answered
- [ ] At least three challenges are described

---

## Task 3.10 — Run the Final Submission Checklist

### Repository and Structure

- [ ] Git repository exists
- [ ] Required folder structure is present
- [ ] `.gitignore` is correct
- [ ] README is complete

### Airflow

- [ ] DAG appears in Airflow UI
- [ ] DAG runs end-to-end
- [ ] At least four logical stages exist
- [ ] Tasks include retries
- [ ] Logs are meaningful
- [ ] Pipeline is idempotent
- [ ] Critical failures stop the pipeline

### Raw and Warehouse

- [ ] Raw data lands as Parquet
- [ ] DuckDB is used as the local warehouse
- [ ] Project can start from a clean DuckDB file

### dbt Modeling

- [ ] Six IMDb staging models exist
- [ ] Intermediate reshaping models exist
- [ ] Star schema exists
- [ ] `fct_title_ratings` has one row per title
- [ ] `dim_title` exists
- [ ] `dim_person` exists
- [ ] `dim_genre` exists
- [ ] `dim_year` exists
- [ ] `bridge_title_genre` exists
- [ ] `bridge_title_crew` exists

### Snapshots and Testing

- [ ] Ratings snapshot exists
- [ ] `dim_title` SCD Type 2 exists
- [ ] Every primary key has `unique` and `not_null`
- [ ] Relationships tests exist
- [ ] Rating range is tested
- [ ] Future `start_year` values are tested
- [ ] At least one custom SQL test exists
- [ ] Known dirty data uses documented `severity: warn`

### Analytical Deliverables

- [ ] Top 10 directors question is answered
- [ ] Runtime by decade question is answered
- [ ] Runtime-rating correlation is calculated
- [ ] Hidden gems versus overrated question is answered
- [ ] Final person-genre mart supports the overall project goal

### Documentation

- [ ] `dbt docs generate` succeeds
- [ ] Lineage graph is browsable
- [ ] Real Airflow screenshot exists
- [ ] Real dbt lineage screenshot exists
- [ ] Written report is complete
- [ ] At least three challenges are documented
- [ ] How-to-run instructions were verified

---

# Sprint Summary

| Sprint | Main Goal | Final Result |
|---|---|---|
| Sprint 1 | Ingestion and infrastructure | Airflow downloads six IMDb sources and creates Parquet files |
| Sprint 2 | Modeling and testing | Star schema, bridge tables, snapshots, and dbt tests |
| Sprint 3 | Analytics and delivery | Marts, answers, reproducibility, dbt docs, README, and final report |

---

# Recommended Working Rule

For every task:

```text
implement
    ↓
test
    ↓
commit
    ↓
document decisions and problems
    ↓
continue to the next task
```

This prevents the documentation from becoming a separate project at the end.

---

# Suggested Commit Strategy

```bash
git commit -m "Initialize IMDb data engineering project"
git commit -m "Implement IMDb raw ingestion pipeline"
git commit -m "Add IMDb staging models"
git commit -m "Build title genre and crew bridge models"
git commit -m "Create IMDb dimensional model and fact table"
git commit -m "Add dbt snapshots and data quality tests"
git commit -m "Create IMDb analytical marts"
git commit -m "Integrate dbt workflow into Airflow DAG"
git commit -m "Add dbt docs and project documentation"
git commit -m "Finalize reproducible setup and README"
```
