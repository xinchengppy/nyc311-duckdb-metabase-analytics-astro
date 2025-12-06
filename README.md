# NYC 311 Analytics with DuckDB Metabase, Airflow (Astro)

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A data analytics pipeline for NYC 311 service requests using Apache Airflow (Astro), DuckDB, and Metabase. Automates daily ETL from the [NYC Open Data API](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9) for daily/weekly/monthly urban insights.

![NYC 311 Analytics Pipeline](NYC311_Analytics.svg)

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Usage](#usage)
- [Data Model](#data-model)
- [Visualizations](#visualizations)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- **Automated ETL**: Daily data ingestion from NYC Open Data API with error handling.
- **High-Performance DB**: DuckDB for fast OLAP queries on large datasets.
- **Interactive Dashboards**: Metabase visualizations for trends, metrics, heatmaps, and filters.
- **Containerized**: Docker setup for easy deployment.
- **Modular**: Extensible SQL models and Python scripts.

## Architecture

```
NYC311 Open Data API
       ↓
   Airflow DAG (Astro)
       ↓
   Data Ingestion and Transformation (Python)
       ↓
   DuckDB Data Storage (Staging & Models)
       ↓
   Read-Only DuckDB Copy
       ↓
   Metabase Dashboards
```


- **Orchestration**: Astro (Airflow) manages daily ETL DAGs.
- **Database**: Primary DuckDB for processing; read-only copy for Metabase to avoid locks.
- **Visualization**: Metabase connects via JDBC to read-only DuckDB.
- **Data Flow**: API → Staging → Models → Metabase sync.

## Prerequisites

- Docker and Docker Compose (for containerized deployment)
- Astronomer CLI (`astro`) for Airflow management
- Python 3.8+
- Git
- At least 4GB RAM and 10GB disk space for data processing

## Setup

### 1. Clone the Repository

```bash
git clone https://github.com/xinchengppy/nyc311-duckdb-metabase-analytics-astro.git
cd nyc311-duckdb-metabase-analytics-astro
```

### 2. Install Dependencies

Python packages (in `requirements.txt`):
OS packages in `packages.txt` for Docker.

### 3. Configure Environment

Create `.env` in `include/` with:

```
# NYC Open Data App Token (optional)
NYC_OPEN_DATA_APP_TOKEN=your_token

# Database paths
DB_DIR=include/db
DB_FILE=include/db/analytics.duckdb

# Metabase
METABASE_URL=http://host.docker.internal:3000
METABASE_USER=your_username
METABASE_PASSWORD=your_password
METABASE_DB_ID=ID
```
NYC OPEN DATA APP token you can get from: [HERE](https://data.cityofnewyork.us/profile/edit/developer_settings) (optional, for higher rate limits)

We use these Metabase .env variables to automatically refresh Metabase, as defined in `include/scripts/metabase_refresh.py`.

- `METABASE_USER` and `METABASE_PASSWORD` are your email and password when logging into Metabase.
- `METABASE_DB_ID` is the database ID in Metabase (found in Admin > Databases > your database > URL last number = ID).

### 4. Start Metabase

In a separate terminal, start Metabase:

```bash
cd metabase
docker-compose up -d
```

This launches Metabase on `http://localhost:3000` with the read-only DuckDB database mounted.

### 5. Start Airflow (Astro)

Back in the root project directory:

```bash
astro dev start
```

This starts the Airflow services on `http://localhost:8080`.

### 6. Initial Data Load

- In Airflow UI (`http://localhost:8080`), trigger the `nyc311_daily_etl` DAG manually for the first run.
- By default, this ingests the past 60 days of NYC 311 data. You can modify the `days_back` parameter in `dags/nyc311_daily_etl.py` to change the data range.
- **Note**: Larger data volumes (e.g., more than 90 days) may take significant time to ingest and process. Monitor the Airflow logs for progress.
- The DAG will download data, update DuckDB, create a read-only copy, restart Metabase, and refresh its schema.
- Monitor logs in Airflow UI to ensure successful completion.

## Usage

- **Monitor Pipeline**: Airflow UI for DAG runs and logs.
- **Explore Data**: Query DuckDB files with CLI or Python.
- **Dashboards**: Metabase for visualizations.
- **Customize**: Edit DAGs, models, or scripts.

## Data Model

Star schema:

- **Staging**: `staging.nyc_311` (raw API data with data cleaning, standarlisation and filter)
- **Facts**: `models.fct_requests` (requests with status/timestamps)
- **Dimensions**: `models.dim_complaint_type`
- **Views**: Complaint volume, resolution metrics, complaint heatmaps, trends...

Models in `include/sql/models/`.

## Visualizations
*Dashboards not persisted; recreate in Metabase using SQL models.*

Here are my visualisations: 

Metabase dashboards with filters for Borough, Complaint Types, and Dynamic time ranges:

- **Complaint Volume Trends**: Line charts showing daily complaint counts over time
- **Top Complaint Types**: Bar charts of most frequent complaint categories
- **Complaint Heatmaps**: Time-of-day and day-of-week patterns (DOW = Day of Week)
- **Resolution Metrics**: Average resolution times by complaint type
- **Resolution Rates**: Percentage of complaints resolved within target timeframes
![Complaint Analysis](https://github.com/xinchengppy/nyc311-duckdb-metabase-analytics-astro/blob/main/screenshots/complaint_analysis.gif)
![Resolution Analysis](https://github.com/xinchengppy/nyc311-duckdb-metabase-analytics-astro/blob/main/screenshots/resolution_analysis.gif)





## Troubleshooting

- **Astro Start Fails**: Check Docker/port 8080; `docker system prune`.
- **Metabase Issues**: `cd metabase && docker-compose up -d`; check logs.
- **DuckDB Locks**: Solved by read-only copy.
- **Data Errors**: Check API limits/network; monitor Airflow logs.
- **Memory**: DuckDB efficient, but large data needs more RAM.

**Logs**: Airflow UI, `astro dev logs`, `docker logs`, DuckDB CLI.

See docs: [Astro](https://www.astronomer.io/docs/), [DuckDB](https://duckdb.org/docs/), [Metabase](https://www.metabase.com/docs/).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

Built with [Astro](https://www.astronomer.io/), [DuckDB](https://duckdb.org/), [Metabase](https://www.metabase.com/). Data from [NYC 311 Open Data](https://data.cityofnewyork.us/Social-Services/311-Service-Requests-from-2010-to-Present/erm2-nwe9/about_data).

*This project follows the guidance from [DataSkew's Analytics Dashboard project](https://dataskew.io/projects/analytics-dashboard/) with personal modifications and enhancements.*

