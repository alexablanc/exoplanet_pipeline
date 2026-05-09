# NASA Exoplanet Pipeline

Processing 6,150+ confirmed exoplanets from NASA's Exoplanet Archive. Orchestrated using Airflow and dbt.

---

## Project Overview

### What This Project Does

1. **Ingests** exoplanet data from NASA's public Exoplanet Archive API (6,150+ records, continuously updated)
2. **Transforms** raw data through a medallion architecture (staging → intermediate → marts)
3. **Orchestrates** the entire pipeline with Apache Airflow DAGs
4. **Validates** data quality with dbt tests and custom checks
5. **Documents** all models, tests, and lineage via dbt

---

### Data Flow

```
NASA Exoplanet Archive API
         ↓
    [Fetch Task]
         ↓
    Raw CSV Data
         ↓
    [Validate Task]
         ↓
    Staging Layer (stg_exoplanets)
         ↓
    Intermediate Layer (int_exoplanets_enriched, int_discovery_trends)
         ↓
    Mart Layer (fct_exoplanets, dim_discovery_analysis)
         ↓
    [dbt Tests & Quality Checks]
         ↓
    Analytics-Ready Tables
```
---

## Data Models

### Staging Layer: 
#### `stg_exoplanets`

- **Purpose**: Ingest raw NASA API data with minimal transformation
- **Operations**: Column standardization, type casting, deduplication
- **Grain**: One row per exoplanet

### Intermediate Layer

#### `int_exoplanets_enriched`
- Adds derived metrics: planet density, habitability classification, planet type
- Enriches with calculated fields and business logic
- Maintains grain: one row per exoplanet

#### `int_discovery_trends`
- Aggregates discovery statistics by year, method, and stellar host type
- Provides insights into discovery velocity and patterns
- Grain: one row per discovery trend dimension

### Mart Layer

#### `fct_exoplanets` (Fact Table)
- **Grain**: One row per exoplanet
- **Key fields**: planet properties, orbital characteristics, thermal properties, discovery info
- **Indexes**: planet_name, host_star_name, discovery_year, habitability_classification
- **Purpose**: Core analytics table for exoplanet queries

#### `dim_discovery_analysis` (Dimension Table)
- **Grain**: One row per discovery trend aggregation
- **Key fields**: trend type, dimension, aggregated metrics, discovery intensity
- **Purpose**: Support discovery analytics and reporting

---

## Setup & Installation

### Prerequisites

- Python 3.9+
- pip or conda
- SQL app for production

### 1. Clone & Navigate

```bash
git clone https://github.com/alexablanc/exoplanet-pipeline.git
cd exoplanet-pipeline
```

### 2. Create Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure dbt

```bash
# Copy profiles.yml to dbt config directory (if needed)
mkdir -p ~/.dbt
cp dbt/profiles.yml ~/.dbt/profiles.yml
```

### 5. Fetch Initial Data

```bash
python scripts/fetch_nasa_data.py --output data/raw/exoplanets.csv
```

### 6. Run dbt

```bash
cd dbt
dbt deps
dbt debug 
dbt run  
dbt test 
```

---

## Running the Pipeline

### Option 1: Manual Execution (Development)

```bash
# Fetch data
python scripts/fetch_nasa_data.py

# Run dbt transformations
cd dbt && dbt run && dbt test

# Generate documentation
dbt docs generate
dbt docs serve  # View at http://localhost:8000
```

### Option 2: Airflow Orchestration

```bash
# Initialize Airflow
export AIRFLOW_HOME=./airflow
airflow db init

# Create admin user
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com

# Start Airflow scheduler and webserver
airflow scheduler &
airflow webserver --port 8080

# Access at http://localhost:8080
# Trigger DAG: exoplanet_pipeline
```

---

## Example Queries

Once the pipeline runs, you can query the analytics-ready tables:

### Find habitable exoplanets

```sql
select
  planet_name,
  host_star_name,
  equilibrium_temperature_k,
  planet_radius_earth_radii,
  orbital_period_days
from fct_exoplanets
where habitability_classification = 'Habitable Zone'
order by equilibrium_temperature_k desc;
```

### Discovery trends by year

```sql
select
  dimension as discovery_year,
  planets_discovered,
  unique_stars,
  avg_equilibrium_temp_k,
  discovery_intensity
from dim_discovery_analysis
where trend_type = 'by_year'
order by dimension desc;
```

### Most common discovery methods

```sql
select
  dimension as discovery_method,
  planets_discovered,
  first_discovery_year,
  most_recent_discovery_year
from dim_discovery_analysis
where trend_type = 'by_method'
order by planets_discovered desc;
```

---

## Data Quality & Testing

### dbt Tests Included

- **Uniqueness**: Exoplanet names are unique in fact table
- **Not Null**: Critical fields (planet name, host star, discovery year)
- **Range Validation**: Discovery years between 1992-2026
- **Orbital Period**: Positive values within realistic range

### Running Tests

```bash
cd dbt
dbt test  # Run all tests
dbt test --select stg_exoplanets  # Run tests for specific model
dbt test --select test_exoplanets_unique  # Run specific test
```

---

## Configuration

### Environment Variables

Create a `.env` file for production configuration:

```env
# Database (PostgreSQL`)
DBT_POSTGRES_HOST=localhost
DBT_POSTGRES_USER=postgres
DBT_POSTGRES_PASSWORD=your_password
DBT_POSTGRES_PORT=5432

# Airflow
AIRFLOW_HOME=./airflow
AIRFLOW__CORE__DAGS_FOLDER=./airflow/dags
AIRFLOW__CORE__LOAD_EXAMPLES=false
```

### dbt Variables

Modify `dbt/dbt_project.yml` to adjust:

- `nasa_api_base_url`: NASA API endpoint
- `min_discovery_year`, `max_discovery_year`: Validation ranges
- `data_freshness_days`: Alert threshold for stale data

---

## Performance & Scalability

### Current Capacity

- **Records**: 6,150+ exoplanets
- **Execution Time**: ~2-5 minutes (API fetch + transformation)
- **Database**: SQLite (dev) or PostgreSQL (prod)

### Scaling Strategies

1. **Incremental Models**: Add `+incremental_strategy: merge` to fact tables
2. **Partitioning**: Partition by `discovery_year` for large datasets
3. **Distributed Execution**: Run Airflow on Kubernetes or Celery
4. **Caching**: Implement dbt snapshot for historical tracking

---

## Resources

- [NASA Exoplanet Archive API](https://exoplanetarchive.ipac.caltech.edu/docs/program_interfaces.html)
- [dbt Documentation](https://docs.getdbt.com/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Medallion Architecture](https://www.databricks.com/blog/2022/06/24/use-the-medallion-lakehouse-architecture-to-build-data-platforms-on-databricks.html)

---
