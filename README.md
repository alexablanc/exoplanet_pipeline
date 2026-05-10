# NASA Exoplanet Pipeline

End-to-end data engineering pipeline that ingests 6,150+ exoplanet records from NASA's Exoplanet Archive API, loads them into Snowflake, and transforms them into analytics-ready dimensional models using dbt. Orchestrated daily with Apache Airflow on Astronomer.

---

## Project Overview

### What This Project Does

1. **Ingests** exoplanet data from NASA's public Exoplanet Archive API (6,150+ records, updated daily)
2. **Loads** raw data directly into a Snowflake raw table using Airflow's `SnowflakeHook`
3. **Transforms** data through a medallion architecture (staging → intermediate → marts) using dbt
4. **Validates** data quality with Airflow SQL checks and dbt built-in and custom tests
5. **Documents** all models, tests, and lineage via dbt docs

---

### Data Flow

```
NASA Exoplanet Archive API
         ↓
    [Fetch & Load Task]
         ↓
  Snowflake: EXOPLANETS_DB.RAW.RAW_EXOPLANET_DATA
         ↓
    [Validate Task — SQL row count & null checks]
         ↓
    Staging Layer (stg_exoplanets)
         ↓
    Intermediate Layer (int_exoplanets_enriched, int_discovery_trends)
         ↓
    Mart Layer (fct_exoplanets, dim_discovery_analysis)
         ↓
    [dbt Tests & Quality Checks]
         ↓
    Analytics-Ready Snowflake Tables
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| **Apache Airflow (Astronomer)** | Pipeline orchestration and scheduling |
| **Snowflake** | Cloud data warehouse — raw ingestion and analytics layer |
| **dbt (data build tool)** | SQL transformations, testing, and documentation |
| **NASA Exoplanet Archive API** | Data source |
| **Python** | API client, data parsing, Airflow operators |
| **Docker** | Local development environment via Astro CLI |

---

## Project Structure

```text
exoplanet_pipeline/
├── airflow/
│   ├── dags/
│   │   └── exoplanet_pipeline.py     # Main Airflow DAG
│   ├── scripts/
│   │   └── fetch_nasa_data.py        # Pure Python NASA API client
│   ├── Dockerfile                    # Astro Runtime image
│   └── requirements.txt              # Airflow container dependencies
├── dbt/
│   ├── models/
│   │   ├── staging/                  # Type casting, renaming, deduplication
│   │   ├── intermediate/             # Derived metrics and enrichment
│   │   └── marts/                    # Fact and dimension tables
│   ├── tests/specific/               # Custom SQL data quality tests
│   ├── dbt_project.yml               # dbt project configuration
│   └── profiles.yml                  # Snowflake connection profile
├── .env.example                      # Template for environment variables
└── requirements.txt                  # Local dev dependencies
```

---

## Data Models

### Staging Layer

#### `stg_exoplanets`
- **Purpose**: Read raw Snowflake data with minimal transformation
- **Operations**: Column renaming, `try_cast` type conversions, deduplication by `planet_name`
- **Source**: `EXOPLANETS_DB.RAW.RAW_EXOPLANET_DATA`
- **Grain**: One row per exoplanet

### Intermediate Layer

#### `int_exoplanets_enriched`
- **Purpose**: Enrich staging data with derived metrics
- **Operations**: Planet density calculation, habitability zone classification, planet type classification
- **Grain**: One row per exoplanet

#### `int_discovery_trends`
- **Purpose**: Aggregate discovery statistics across multiple dimensions
- **Operations**: Group by discovery year, discovery method, and stellar host type
- **Grain**: One row per discovery trend dimension

### Mart Layer

#### `fct_exoplanets` (Fact Table)
- **Purpose**: Core analytics table for exoplanet queries
- **Key fields**: Planet properties, orbital characteristics, thermal properties, discovery metadata
- **Grain**: One row per exoplanet

#### `dim_discovery_analysis` (Dimension Table)
- **Purpose**: Support discovery analytics and reporting
- **Key fields**: Trend type, dimension, aggregated metrics, discovery intensity classification
- **Grain**: One row per discovery trend aggregation

---

## Setup & Installation

### Prerequisites
- Python 3.9+
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- A Snowflake account

### 1. Snowflake Setup

Before running the pipeline, create the required Snowflake objects by running this SQL in your Snowflake worksheet:

```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS EXOPLANETS_DB;
USE DATABASE EXOPLANETS_DB;

-- Create schemas for each pipeline layer
CREATE SCHEMA IF NOT EXISTS RAW;           -- raw API data loaded by Airflow
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DEV; -- dbt dev target
CREATE SCHEMA IF NOT EXISTS ANALYTICS_PROD;-- dbt prod target

-- Create a warehouse to run queries
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
WITH WAREHOUSE_SIZE = 'XSMALL'
AUTO_SUSPEND = 60
AUTO_RESUME = TRUE;
```

### 2. Clone & Install Dependencies

```bash
git clone https://github.com/alexablanc/exoplanet_pipeline.git
cd exoplanet_pipeline

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 3. Configure Environment Variables

```bash
cp .env.example .env
# Open .env and fill in your Snowflake account, user, and password
```

### 4. Configure the Airflow Snowflake Connection

Start the Airflow environment and create the connection in the UI:

```bash
astro dev start
```

In the Airflow UI at `http://localhost:8080`, go to **Admin → Connections** and create:

| Field | Value |
|---|---|
| Conn Id | `snowflake_default` |
| Conn Type | `Snowflake` |
| Schema | `RAW` |
| Login | Your Snowflake username |
| Password | Your Snowflake password |
| Account | Your account identifier (e.g. `xy12345.us-east-1`) |
| Database | `EXOPLANETS_DB` |
| Warehouse | `COMPUTE_WH` |
| Role | `SYSADMIN` |

---

## Running the Pipeline

### Option 1: Airflow Orchestration (Recommended)

Once the Airflow connection is configured, unpause and trigger the `exoplanet_pipeline` DAG from the Airflow UI. The pipeline runs daily at 2 AM UTC.

```
fetch_and_load_nasa_data → validate_snowflake_data → dbt_transformations
                                                          ↓
                                               dbt_run → dbt_test → dbt_docs_generate
```

### Option 2: Local dbt Execution

```bash
source .env

cd dbt
dbt deps
dbt run
dbt test
dbt docs generate && dbt docs serve  # View docs at http://localhost:8080
```

---

## Data Quality & Testing

Data quality is enforced at two stages:

**Airflow Validation Task** runs SQL directly against Snowflake after ingestion:
- Verifies row count is greater than zero
- Reports null counts for critical columns (`kepler_name`, `koi_disposition`)

**dbt Tests** run after every transformation:
- `unique` and `not_null` checks on surrogate keys and critical fields
- Range validation on discovery years (1992–2026) and orbital periods
- Custom test `test_exoplanets_unique.sql` — no duplicate planets in the fact table
- Custom test `test_discovery_dates_valid.sql` — all discovery dates within valid bounds

---

## Example Queries

### Find Habitable Exoplanets

```sql
SELECT
  planet_name,
  host_star_name,
  equilibrium_temperature_k,
  planet_radius_earth_radii,
  orbital_period_days
FROM EXOPLANETS_DB.ANALYTICS_DEV.fct_exoplanets
WHERE habitability_classification = 'Habitable Zone'
ORDER BY equilibrium_temperature_k DESC;
```

### Discovery Trends by Year

```sql
SELECT
  dimension          AS discovery_year,
  planets_discovered,
  unique_stars,
  avg_equilibrium_temp_k,
  discovery_intensity
FROM EXOPLANETS_DB.ANALYTICS_DEV.dim_discovery_analysis
WHERE trend_type = 'by_year'
ORDER BY dimension DESC;
```

### Most Common Discovery Methods

```sql
SELECT
  dimension          AS discovery_method,
  planets_discovered,
  avg_planet_radius,
  avg_equilibrium_temp_k
FROM EXOPLANETS_DB.ANALYTICS_DEV.dim_discovery_analysis
WHERE trend_type = 'by_method'
ORDER BY planets_discovered DESC;
```

---

## Resources

- [NASA Exoplanet Archive API](https://exoplanetarchive.ipac.caltech.edu/docs/program_interfaces.html)
- [dbt Documentation](https://docs.getdbt.com/)
- [Apache Airflow (Astronomer)](https://www.astronomer.io/docs/)
- [Snowflake Documentation](https://docs.snowflake.com/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/use-the-medallion-lakehouse-architecture-to-build-data-platforms-on-databricks.html)
