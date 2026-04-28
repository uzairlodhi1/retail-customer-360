# retail-customer-360

A production-grade Customer 360° analytics data warehouse built with Snowflake, dbt, Apache Airflow, and PySpark. Consolidates transactional, behavioral, and reference data across channels into a unified customer view for retail analytics.

## Architecture

```
Raw Sources (CSV/API/DB)
        │
        ▼
  [Extraction Layer]
  PySpark / Python
        │
        ▼
  [Staging Layer]
  Snowflake Raw Schema
        │
        ▼
  [dbt Transformation]
  Staging → Intermediate → Marts
        │
        ▼
  [Analytics Mart]
  customer_360 / order_summary
        │
        ▼
  [Orchestration]
  Apache Airflow DAG
```

## Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | PySpark, Python |
| Storage | Snowflake |
| Transformation | dbt Core |
| Orchestration | Apache Airflow |
| Testing | pytest, dbt tests |
| Config | YAML |

## Project Structure

```
retail-customer-360/
├── config/
│   └── config.yaml           # Environment configuration
├── data/
│   └── sample/               # Sample seed data
├── dbt_models/
│   ├── sources.yml
│   ├── staging/              # stg_* models
│   └── marts/                # Customer 360 and order mart models
├── etl/
│   ├── extract.py            # Data extraction layer
│   ├── transform.py          # PySpark transformations
│   └── load.py               # Snowflake load utilities
├── airflow/
│   └── dags/
│       └── customer_360_dag.py
├── scripts/
│   ├── generate_sample_data.py
│   └── run_pipeline.py
└── tests/
    ├── test_transform.py
    └── test_data_quality.py
```

## Setup

### Prerequisites

- Python 3.9+
- Snowflake account (or use sample mode)
- Apache Airflow 2.x
- dbt-snowflake

### Installation

```bash
pip install -r requirements.txt
```

### Configuration

Copy and edit the config file:

```bash
cp config/config.yaml config/config.local.yaml
```

Set your Snowflake credentials via environment variables:

```bash
export SNOWFLAKE_ACCOUNT=your_account
export SNOWFLAKE_USER=your_user
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_DATABASE=RETAIL_DW
export SNOWFLAKE_WAREHOUSE=COMPUTE_WH
export SNOWFLAKE_SCHEMA=RAW
```

### Generate Sample Data

```bash
python scripts/generate_sample_data.py
```

### Run Pipeline (Sample Mode)

```bash
python scripts/run_pipeline.py --mode sample
```

### Run dbt Models

```bash
cd dbt_models
dbt deps
dbt run --profiles-dir .
dbt test
```

## Key Features

- **Incremental loading** — supports full and incremental ingestion strategies
- **Schema evolution** — handles new columns and changed data types gracefully
- **Data quality gates** — enforces null checks, uniqueness, referential integrity before loading
- **Partitioned Snowflake tables** — optimized for query performance using clustering keys
- **Idempotent pipelines** — safe to re-run without duplicating data
- **Modular dbt models** — staging → intermediate → marts pattern

## Data Models

### `customer_360`
Unified customer profile combining registration, purchase history, channel preference, and lifetime value metrics.

| Column | Type | Description |
|---|---|---|
| customer_id | VARCHAR | Primary key |
| full_name | VARCHAR | Customer full name |
| email | VARCHAR | Email address |
| country | VARCHAR | Country of residence |
| total_orders | INTEGER | Lifetime order count |
| total_spend | FLOAT | Lifetime spend (USD) |
| avg_order_value | FLOAT | Average order value |
| days_since_last_order | INTEGER | Recency metric |
| customer_segment | VARCHAR | VIP / Regular / At-Risk |
| first_seen | DATE | First purchase date |
| last_seen | DATE | Most recent purchase date |

### `order_summary`
Aggregated order metrics by date, country, and product category.

## License

MIT
