# IMDb Data Engineering Project — Project Notes

## 1. Dataset Choice

The project uses the public IMDb bulk datasets.

IMDb was selected because it provides several related datasets that are suitable
for building a realistic data engineering pipeline involving:

- ingestion from multiple sources;
- large-scale tabular data processing;
- many-to-many relationships;
- dimensional modeling;
- historical tracking;
- data quality validation;
- analytical reporting.

The dataset also supports the analytical requirements of the project, including
questions related to titles, ratings, directors, actors, genres, runtime, vote
counts, and episode-to-series relationships.

The raw IMDb datasets are distributed as compressed tab-separated files
(`.tsv.gz`).

The project converts these files to Parquet before loading and transforming the
data because Parquet provides a more efficient columnar format for analytical
processing.

---

## 2. IMDb Sources

The project uses seven IMDb datasets.

| Source | Main purpose |
|---|---|
| `title.basics.tsv.gz` | General information about titles, including title type, primary title, years, runtime, and genres |
| `title.ratings.tsv.gz` | Average rating and number of votes for each title |
| `title.crew.tsv.gz` | Director and writer identifiers associated with each title |
| `title.principals.tsv.gz` | Principal cast and crew members associated with titles |
| `name.basics.tsv.gz` | Information about people, professions, and known-for titles |
| `title.akas.tsv.gz` | Alternative titles, regions, languages, and related metadata |
| `title.episode.tsv.gz` | Links episode titles to their parent series and provides season and episode numbers |

IMDb source:

```text
https://datasets.imdbws.com/
```

The source TSV files and generated Parquet files are local pipeline artifacts
and are not committed to Git.

---

## 3. Sprint 1 Architecture

The initial data flow is:

```text
IMDb source files
        ↓
Apache Airflow
        ↓
TSV.GZ validation
        ↓
Parquet conversion
        ↓
DuckDB
        ↓
dbt
```

Airflow is responsible for orchestrating the ingestion workflow.

Parquet is used as the raw landed format, DuckDB is used as the local analytical
warehouse, and dbt is used for the transformation and modeling layer.

---

## 4. Important Technical Decisions

### 4.1 Airflow runs through Docker Compose

Apache Airflow is executed inside Docker containers instead of being installed
and managed directly on the host operating system.

This provides a reproducible environment and isolates project dependencies from
the local Python installation.

---

### 4.2 DuckDB is used as the analytical warehouse

DuckDB was selected because it works well for local analytical workloads and
can efficiently query Parquet files.

It also integrates with dbt through the `dbt-duckdb` adapter.

The warehouse is stored locally as:

```text
warehouse/warehouse.duckdb
```

---

### 4.3 Parquet is used as the raw landed format

IMDb provides the source datasets as compressed TSV files.

The ingestion pipeline converts each source into Parquet before the data is used
by the downstream warehouse and transformation layers.

This reduces storage overhead and provides a columnar format better suited for
analytical queries.

---

### 4.4 A vertical slice was implemented first

Before processing all seven IMDb datasets, the ingestion architecture was tested
using:

```text
title.ratings.tsv.gz
```

The complete flow for this source was implemented first:

```text
source validation
        ↓
Parquet conversion
        ↓
validation
        ↓
DuckDB/dbt access
```

This reduced implementation risk because the architecture could be validated on
one relatively simple source before generalizing the logic to all seven datasets.

---

### 4.5 DuckDB performs the TSV-to-Parquet conversion

The final conversion implementation uses DuckDB's CSV reader and `COPY`
operation instead of loading the complete datasets into a pandas DataFrame.

Conceptually:

```sql
read_csv(...)
        ↓
COPY
        ↓
Parquet
```

This decision was important for large IMDb datasets such as
`title.principals`, `title.episode`, and `title.akas`, because it avoids keeping
the entire source dataset in Python memory during conversion.

---

### 4.6 IMDb null values are handled explicitly

IMDb represents missing values using:

```text
\N
```

The DuckDB reader is configured with:

```text
nullstr = '\N'
```

so IMDb missing values are interpreted as SQL `NULL` rather than regular text.

---

### 4.7 IMDb files are parsed as TSV without standard CSV quoting

IMDb files are tab-separated.

The reader therefore uses:

```text
delim = '\t'
header = true
quote = ''
```

Disabling standard CSV quote processing was necessary for reliable parsing of
the IMDb source format.

---

### 4.8 Temporary Parquet files are used for safe replacement

The conversion process writes to a temporary output first.

Example:

```text
title.basics.tmp.parquet
        ↓
validation
        ↓
title.basics.parquet
```

The final Parquet file is replaced only after the new conversion succeeds.

This prevents a failed conversion from destroying a previously valid output.

---

### 4.9 The ingestion pipeline is idempotent

IMDb datasets represent complete snapshots rather than incremental event data.

For this reason, the pipeline does not append the source dataset every time it
runs.

Instead, the existing Parquet output is replaced with the newly generated
version.

The DAG was executed more than once using the same source data and the resulting
row counts remained unchanged. No duplicated Parquet files or duplicated rows
were produced.

---

### 4.10 Generated data is excluded from Git

Large generated files are intentionally not version-controlled.

Examples include:

```text
*.tsv
*.tsv.gz
*.parquet
*.duckdb
```

Only source code, configuration, documentation, and placeholder files such as
`raw/.gitkeep` are stored in the repository.

---

## 5. Problems Encountered and Solutions

### 5.1 Docker image/network download problems

#### Problem

During the initial environment setup, Docker occasionally failed while pulling
the Apache Airflow image because of network/TLS timeout problems.

#### Solution

Network connectivity to Docker Hub was verified and the environment was retried
after changing the available network connection.

Once the image was available locally, the Airflow environment could be started
normally using Docker Compose.

---

### 5.2 Airflow services and DAG visibility

#### Problem

During the initial setup, some Airflow services were not running or a DAG did
not immediately appear in the Airflow UI.

#### Solution

The container state and DAG import status were checked using commands such as:

```bash
docker compose ps
docker compose logs airflow-scheduler
docker compose exec airflow-scheduler airflow dags list-import-errors
```

This helped distinguish container configuration issues from actual DAG Python
import errors.

---

### 5.3 DuckDB availability differed between containers

#### Problem

A direct Python test using:

```python
import duckdb
```

failed in one Airflow container because the Python DuckDB package was not
available in that specific container environment.

At the same time, dbt could successfully connect to DuckDB using the installed
`dbt-duckdb` adapter.

#### Solution

The container containing the required project dependencies was used for dbt
operations, and dependency availability was verified using:

```bash
dbt debug
```

This highlighted the importance of checking dependencies inside the actual
Docker service where each task executes rather than assuming every Airflow
container has identical Python packages.

---

### 5.4 Large IMDb files caused conversion concerns

#### Problem

Some IMDb datasets, especially `title.principals`, `title.episode`, and
`title.akas`, contain a large number of rows.

Using pandas for the complete TSV-to-Parquet conversion would require loading
large datasets into memory.

#### Solution

The conversion implementation was changed to use DuckDB directly:

```text
IMDb TSV.GZ
    ↓
DuckDB read_csv()
    ↓
COPY ... FORMAT PARQUET
```

This approach is better suited for the size of the IMDb datasets and reduces
Python memory pressure.

---

### 5.5 Parsing `title.principals`

#### Problem

`title.principals` was one of the sources that exposed parsing/conversion
problems while generalizing the pipeline to all seven datasets.

#### Solution

The DuckDB reader configuration was adjusted to explicitly describe the IMDb
TSV format:

```text
delimiter: tab
header: true
null value: \N
quote handling: disabled
```

After the change, the source completed the conversion successfully.

---

### 5.6 Python / SQL syntax problems during DAG development

#### Problem

Some intermediate versions of the ingestion code generated syntax errors while
the DuckDB SQL command was embedded inside Python.

#### Solution

The SQL statement was simplified and kept as a clearly defined multiline
f-string passed directly to:

```python
connection.execute(...)
```

The DAG import status was then checked again before triggering the workflow.

---

### 5.7 Idempotency needed explicit verification

#### Problem

A successful first DAG execution does not prove that an ingestion pipeline is
idempotent.

Repeated runs could potentially append data or leave multiple generated files.

#### Solution

The complete ingestion was executed again using the same input files.

The outputs from the second execution were compared with the previous run.

The verification showed that:

- the same Parquet files were produced;
- row counts remained unchanged;
- additional duplicate output files were not created;
- the pipeline did not append duplicate records.

---

## 6. Validation Strategy

Each source is validated before and after conversion.

The ingestion workflow checks that:

- the source exists;
- the source is not empty;
- the resulting Parquet file exists;
- the resulting Parquet file contains records;
- conversion failures stop the task;
- an invalid temporary result does not replace a valid final file.

Airflow tasks also produce log information that can be used to verify the
processing of each source.

---

## 7. Row Counts and File Sizes

The following table should contain the values produced by the actual ingestion
run.

| Dataset | Row count | Parquet size |
|---|---:|---:|
| `title.basics` | TODO | TODO |
| `title.ratings` | TODO | TODO |
| `title.crew` | TODO | TODO |
| `title.principals` | TODO | TODO |
| `name.basics` | TODO | TODO |
| `title.akas` | TODO | TODO |
| `title.episode` | TODO | TODO |

The values in this section must come from the generated project files rather
than from approximate values found online, because IMDb datasets are updated
regularly.

---

## 8. Sprint 1 Notes

At the end of Sprint 1, the project has established the ingestion foundation
required for the later modeling stages.

The main lessons from the implementation were:

- validate a complete vertical slice before generalizing the pipeline;
- avoid loading very large source files entirely into Python memory;
- treat generated data as replaceable pipeline artifacts;
- make idempotency an explicit design requirement;
- verify dependencies inside Docker containers rather than only on the host;
- keep source parsing rules explicit for external datasets;
- preserve valid outputs when a new conversion fails.

The next stage of the project will focus on dbt staging models, dimensional
modeling, bridge tables, snapshots, and data quality tests.
