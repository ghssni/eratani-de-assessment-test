# Eratani Data Engineer Technical Assessment

This project ingests agriculture production data from a CSV into Postgres via Airflow, models it with dbt, and produces daily agriculture KPIs. The Airflow DAG `agriculture_pipeline` is scheduled for 06:00 UTC and orchestrates ingestion plus dbt builds.

## Project structure
- `dags/eratani_pipeline.py` – Airflow DAG that creates the staging table, ingests the CSV idempotently, logs row counts, and runs dbt.
- `data/agriculture_dataset.csv` – Example CSV input file expected by the DAG.
- `dbt_project/` – dbt project containing staging, fact, and metrics models.
- `requirements.txt` – Python dependencies for Airflow and dbt.

## Quickstart (local without containers)
1. **Set up Postgres**
   - Create a database (example): `createdb airflow`
   - Ensure the Airflow connection `postgres_default` (or set `AG_POSTGRES_CONN_ID`) points to this DB.

2. **Install dependencies** (prefer virtualenv):
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure dbt profile**
   - Copy `dbt_project/profiles.yml.example` to `~/.dbt/profiles.yml` (or set `DBT_PROFILES_DIR`).
   - Set env vars for Postgres if needed: `DBT_POSTGRES_HOST`, `DBT_POSTGRES_USER`, `DBT_POSTGRES_PASSWORD`, `DBT_POSTGRES_DB`, `DBT_POSTGRES_PORT`.

4. **Airflow variables/env**
   - `AG_DATA_PATH` (optional): override path to the CSV (defaults to `data/agriculture_dataset.csv`).
   - `AG_DBT_PROJECT_DIR` (optional): defaults to `/opt/airflow/dbt_project`; adjust if running directly from the repo.
   - `AG_DBT_TARGET` (optional): dbt target name (defaults to `dev`).

5. **Run the Airflow DAG manually** (after starting the scheduler/webserver):
   - The DAG `agriculture_pipeline` is scheduled at `0 6 * * *` (06:00 UTC) with `catchup=False`.
   - Tasks:
     1. Create staging table `public.stg_agriculture_raw` if missing.
     2. Truncate and load the CSV (idempotent) using `COPY`.
     3. Log ingested row count.
     4. Run dbt models `fact_farm_production` and `agriculture_metrics` (materialized as `agriculture_metrics_daily`), then dbt tests.

6. **Run dbt directly** (optional for quick verification):
   ```bash
   cd dbt_project
   dbt run --select fact_farm_production agriculture_metrics
   dbt test --select fact_farm_production agriculture_metrics
   ```

## Data modeling
- **Staging (`stg_agriculture`)**: trims text, casts numerics, and removes null keys from `stg_agriculture_raw`.
- **Fact (`fact_farm_production`)**: cleansed, deduplicated production records.
- **Metrics (`agriculture_metrics` -> `agriculture_metrics_daily` table)**: KPI rows with `as_of_date`, `metric`, dimensions, `value`, `unit`, and `rank` for top-3 lists. Metrics include total yield by crop & season, yield per acre, fertilizer efficiency, water productivity, top crops by yield per acre, and top irrigation methods by average yield.

## Sample metrics output (from the sample CSV)
1. Total Yield (tons) per Crop & Season: 
- Rice→ Wet 45, Dry 44; 
- Corn→ Wet 61, Dry 52; S
- oybean→ Wet 34; 
- Wheat→ Dry 48.
2. Yield per Acre (tons/acre) per Crop: 
- Rice 4.0455; 
- Corn 2.6905; 
- Wheat 2.6667; 
- Soybean 2.2667.
3. Fertilizer Efficiency (tons/ton): 
- Rice 18.94; 
- Corn 16.87; 
- Wheat 18.46; 
- Soybean 17.89.
4. Water Productivity (tons/m³): 
- Rice 0.0147; 
- Corn 0.0185; 
- Wheat 0.0178; 
- Soybean 0.0131.
5. Top 3 Crops by Yield per Acre: 
1) Rice 4.0455, 
2) Corn 2.6905, 
3) Wheat 2.6667.
6. Top 3 Irrigation Methods by Avg Yield: 
1) Center Pivot 61.0, 
2) Sprinkler 50.0, 
3) Drip 44.5.


## Notes
- Replace the sample CSV with the real `agriculture_dataset.csv` before running (in this case I use sample dataset).
- The ingestion is idempotent because the staging table is truncated before loading; downstream dbt models are table materializations, rebuilt on each run.
- If you prefer containers, add a `docker-compose.yml` with Airflow, Postgres, and a dbt runner mounting this repo; set env vars accordingly.
