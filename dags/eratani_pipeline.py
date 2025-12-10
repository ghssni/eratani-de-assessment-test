from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# This DAG creates a staging table, ingests the agriculture CSV into PostgreSQL
# in an idempotent way (truncate + load), logs the ingested row count,
# and then runs dbt models to build the fact table and metrics.
# The pipeline is scheduled to run daily at 06:00 UTC.

DEFAULT_CONN_ID = os.environ.get("AG_POSTGRES_CONN_ID", "postgres_default")

DATA_PATH = Path(
    os.environ.get(
        "AG_DATA_PATH",
        str(Path(__file__).resolve().parents[2] / "data" / "agriculture_dataset.csv"),
    )
)

DBT_PROJECT_DIR = Path(
    os.environ.get(
        "AG_DBT_PROJECT_DIR",
        str(Path(__file__).resolve().parents[2] / "dbt_project"),
    )
)

DBT_TARGET = os.environ.get("AG_DBT_TARGET", "dev")


def _load_csv_to_staging(**_: object) -> None:
    """
    Load the agriculture CSV into the staging table in an idempotent way:
    - truncate the existing rows
    - copy all rows from the CSV file using PostgreSQL COPY.
    """
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"CSV not found at {DATA_PATH}")

    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)

    with hook.get_conn() as conn, conn.cursor() as cur, open(
        DATA_PATH, "r", encoding="utf-8"
    ) as f:
        # Idempotent load: always truncate before inserting new data
        cur.execute("TRUNCATE TABLE public.stg_agriculture_raw")

        copy_sql = """
        COPY public.stg_agriculture_raw (
            farm_id,
            crop_type,
            farm_area_acres,
            irrigation_type,
            fertilizer_used_tons,
            pesticide_used_kg,
            yield_tons,
            soil_type,
            season,
            water_usage_cubic_meters
        ) FROM STDIN WITH CSV HEADER
        """
        hook.copy_expert(copy_sql, f)
        conn.commit()


def _log_row_count(**_: object) -> None:
    """
    Log the number of rows currently stored in the staging table.
    """
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    records = hook.get_first("SELECT COUNT(*) FROM public.stg_agriculture_raw")
    row_count = records[0] if records else 0
    logging.info("Ingested %s rows into stg_agriculture_raw", row_count)


def _dbt_command(task_id: str, command: str) -> BashOperator:
    """
    Helper to create a BashOperator for running dbt commands with the
    proper environment variables.
    """
    env = {
        "DBT_PROFILES_DIR": str(DBT_PROJECT_DIR),
        "DBT_TARGET": DBT_TARGET,
    }

    return BashOperator(
        task_id=task_id,
        bash_command=command,
        env=env,
    )


def _dbt_run(task_id: str, models: str) -> BashOperator:
    """
    Run dbt models selected by name or tag.
    """
    return _dbt_command(
        task_id=task_id,
        command=f"cd {DBT_PROJECT_DIR} && dbt run --select {models}",
    )


def _dbt_test(task_id: str, models: str) -> BashOperator:
    """
    Run dbt tests for the given models.
    """
    return _dbt_command(
        task_id=task_id,
        command=f"cd {DBT_PROJECT_DIR} && dbt test --select {models}",
    )


def _create_dag() -> DAG:
    with DAG(
        dag_id="agriculture_pipeline",
        start_date=datetime(2024, 1, 1),
        schedule="0 6 * * *",  # run daily at 06:00 UTC
        catchup=False,
        default_args={"owner": "data-eng"},
        description=(
            "Daily pipeline to create the staging table, ingest agriculture CSV "
            "data into PostgreSQL in an idempotent way, log the row count, and "
            "build downstream fact and metrics models via dbt."
        ),
        max_active_runs=1,
    ) as dag:
        create_staging_table = PostgresOperator(
            task_id="create_staging_table",
            postgres_conn_id=DEFAULT_CONN_ID,
            sql="""
            CREATE TABLE IF NOT EXISTS public.stg_agriculture_raw (
                farm_id TEXT,
                crop_type TEXT,
                farm_area_acres NUMERIC,
                irrigation_type TEXT,
                fertilizer_used_tons NUMERIC,
                pesticide_used_kg NUMERIC,
                yield_tons NUMERIC,
                soil_type TEXT,
                season TEXT,
                water_usage_cubic_meters NUMERIC
            );
            """,
        )

        load_csv = PythonOperator(
            task_id="load_csv_to_staging",
            python_callable=_load_csv_to_staging,
        )

        log_count = PythonOperator(
            task_id="log_row_count",
            python_callable=_log_row_count,
        )

        dbt_run_models = _dbt_run(
            task_id="dbt_run_models",
            models="fact_farm_production agriculture_metrics",
        )

        dbt_tests = _dbt_test(
            task_id="dbt_tests",
            models="fact_farm_production agriculture_metrics",
        )

        create_staging_table >> load_csv >> log_count >> dbt_run_models >> dbt_tests

    return dag


dag = _create_dag()
