import json
import os
from datetime import datetime
from typing import Any


import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator


def _get_required_var(name: str) -> str:
    value = Variable.get(name)
    if not value:
        raise AirflowException(f"Variable `{name}` must be set")
    return value


def _parse_cities(raw_cities: str) -> list[str]:
    value = raw_cities.strip()
    if not value:
        raise AirflowException("`WEATHER_API_CITIES` cannot be empty")

    cities = [city.strip() for city in value.split(",") if city.strip()]

    if not cities:
        raise AirflowException("No valid city names were found in `WEATHER_API_CITIES`")
    return cities


def create_measures_table() -> None:
    db_conn_id = _get_required_var("WEATHER_DB")
    hook = PostgresHook(postgres_conn_id=db_conn_id)
    hook.run(
        """
        create table if not exists measures (
            name text,
            timestamp timestamp,
            temp_c float,
            humidity float,
            visibility float,
            wind_speed_kph float,
            primary key (name, timestamp)
        )
        """
    )


def fetch_weather_payloads(execution_date: str) -> list[dict[str, Any]]:
    base_url = _get_required_var("WEATHER_API_BASE_URL").rstrip("/")
    api_key = _get_required_var("WEATHER_API_KEY")
    cities = _parse_cities(_get_required_var("WEATHER_API_CITIES"))

    endpoint = f"{base_url}/v1/history.json"
    payloads: list[dict[str, Any]] = []

    for city in cities:
        response = requests.get(
            endpoint,
            params={"q": city, "dt": execution_date, "key": api_key},
            timeout=30,
        )
        response.raise_for_status()
        payloads.append(response.json())

    return payloads


def map_weather_payloads(payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = []

    for payload in payloads:
        forecast_day = payload["forecast"]["forecastday"][0]
        day = forecast_day["day"]
        mapped.append(
            {
                "name": payload["location"]["name"],
                "timestamp": forecast_day["date"],
                "temp_c": day["avgtemp_c"],
                "humidity": day["avghumidity"],
                "visibility": day["avgvis_km"],
                "wind_speed_kph": day["maxwind_kph"],
            }
        )

    return mapped


def insert_measures(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    db_conn_id = _get_required_var("WEATHER_DB")
    hook = PostgresHook(postgres_conn_id=db_conn_id)
    conn = hook.get_conn()
    values = [
        (
            row["timestamp"],
            row["temp_c"],
            row["humidity"],
            row["visibility"],
            row["wind_speed_kph"],
            row["name"],
        )
        for row in rows
    ]

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            insert into measures (timestamp, temp_c, humidity, visibility, wind_speed_kph, name)
            values (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """,
            values,
        )
    conn.commit()
    conn.close()


with DAG(
    dag_id="weather_history_fetcher",
    description="Fetches historical weather from API and stores it in the DB",
    start_date=datetime(2026, 3, 27),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
) as dag:
    create_table_task = PythonOperator(
        task_id="create_measures_table",
        python_callable=create_measures_table,
    )

    fetch_task = PythonOperator(
        task_id="fetch_weather_payloads",
        python_callable=fetch_weather_payloads,
        op_kwargs={"execution_date": "{{ ds }}"},
    )

    map_task = PythonOperator(
        task_id="map_weather_payloads",
        python_callable=map_weather_payloads,
        op_args=[fetch_task.output],
    )

    load_task = PythonOperator(
        task_id="insert_measures",
        python_callable=insert_measures,
        op_args=[map_task.output],
    )

    create_table_task >> fetch_task >> map_task >> load_task
