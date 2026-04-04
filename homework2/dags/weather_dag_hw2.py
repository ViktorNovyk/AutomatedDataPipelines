import re
import logging
from datetime import datetime, timedelta


import requests
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.taskinstance import TaskInstance

FETCH_XCOM_KEY = "weather_payload"
MAP_XCOM_KEY = "mapped_measure"

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


def _build_city_configs(cities: list[str]) -> list[tuple[str, str]]:
    city_configs: list[tuple[str, str]] = []

    for city in cities:
        group_id = f"city_{city}"
        city_configs.append((city, group_id))

    return city_configs


def fetch_weather_payload(city: str, execution_date: str, ti: TaskInstance) -> None:
    base_url = _get_required_var("WEATHER_API_BASE_URL").rstrip("/")
    api_key = _get_required_var("WEATHER_API_KEY")

    endpoint = f"{base_url}/v1/history.json"
    try:
        response = requests.get(
            endpoint,
            params={"q": city, "dt": execution_date, "key": api_key},
            timeout=30,
        )
    except requests.RequestException as exc:
        raise AirflowException(f"Weather API request failed for city `{city}`: {exc}") from exc

    status_code = response.status_code

    if 200 <= status_code < 300:
        ti.xcom_push(key=FETCH_XCOM_KEY, value=response.json())
        return

    if 400 <= status_code < 500:
        raise AirflowFailException(
            f"Weather API returned client error {status_code} for city `{city}`"
        )

    if 500 <= status_code < 600:
        logging.warning(
            "Weather API returned server error %s for city `%s`; task will be retried by Airflow.",
            status_code,
            city,
        )
        raise AirflowException(f"Weather API returned server error {status_code} for city `{city}`")

    raise AirflowFailException(f"Weather API returned unexpected status {status_code} for city `{city}`")


def map_weather_payload(fetch_task_id: str, ti: TaskInstance) -> None:
    payload = ti.xcom_pull(task_ids=fetch_task_id, key=FETCH_XCOM_KEY)
    if not payload:
        raise AirflowException(f"XCom payload is empty for task `{fetch_task_id}`")

    forecast_day = payload["forecast"]["forecastday"][0]
    day = forecast_day["day"]
    mapped_row = {
        "name": payload["location"]["name"],
        "timestamp": forecast_day["date"],
        "temp_c": day["avgtemp_c"],
        "humidity": day["avghumidity"],
        "visibility": day["avgvis_km"],
        "wind_speed_kph": day["maxwind_kph"],
    }
    ti.xcom_push(key=MAP_XCOM_KEY, value=mapped_row)


def insert_measure(map_task_id: str, ti: TaskInstance) -> None:
    row = ti.xcom_pull(task_ids=map_task_id, key=MAP_XCOM_KEY)
    if not row:
        return

    db_conn_id = _get_required_var("WEATHER_DB")
    hook = PostgresHook(postgres_conn_id=db_conn_id)
    conn = hook.get_conn()
    values = (
        row["timestamp"],
        row["temp_c"],
        row["humidity"],
        row["visibility"],
        row["wind_speed_kph"],
        row["name"],
    )

    with conn.cursor() as cursor:
        cursor.execute(
            """
            insert into measures (timestamp, temp_c, humidity, visibility, wind_speed_kph, name)
            values (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
            """,
            values,
        )
    conn.commit()
    conn.close()


def choose_load_path(
    map_task_id: str,
    normal_task_id: str,
    alert_task_id: str,
    threshold: float,
    ti: TaskInstance,
) -> str | list[str]:
    row = ti.xcom_pull(task_ids=map_task_id, key=MAP_XCOM_KEY)
    if not row:
        raise AirflowException(f"Mapped row is missing for task `{map_task_id}`")

    wind_speed = row["wind_speed_kph"]
    if wind_speed > threshold:
        return [alert_task_id, normal_task_id]
    return normal_task_id


def log_high_wind_alert(map_task_id: str, threshold: float, ti: TaskInstance) -> None:
    row = ti.xcom_pull(task_ids=map_task_id, key=MAP_XCOM_KEY)
    if not row:
        raise AirflowException(f"Mapped row is missing for task `{map_task_id}`")

    wind_speed = row["wind_speed_kph"]
    logging.warning("wind speed %s is greater than %s", wind_speed, threshold)


with DAG(
    dag_id="weather_history_fetcher_hw2",
    description="Fetches historical weather from API and stores it in the DB",
    start_date=datetime(2026, 4, 4),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
) as dag:
    city_configs = _build_city_configs(_parse_cities(_get_required_var("WEATHER_API_CITIES")))
    wind_threshold = float(_get_required_var("WIND_SPEED_ALERT_THRESHOLD"))

    create_table_task = PythonOperator(
        task_id="create_measures_table",
        python_callable=create_measures_table,
    )

    for city, group_id in city_configs:
        with TaskGroup(group_id=group_id, tooltip=f"Pipeline for {city}") as city_group:
            fetch_task = PythonOperator(
                task_id="fetch_weather_payloads",
                python_callable=fetch_weather_payload,
                op_kwargs={"city": city, "execution_date": "{{ ds }}"},
                retries=2,
                retry_delay=timedelta(seconds=2),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(seconds=8),
            )

            map_task = PythonOperator(
                task_id="map_weather_payloads",
                python_callable=map_weather_payload,
                op_kwargs={"fetch_task_id": fetch_task.task_id},
            )

            normal_load_task = PythonOperator(
                task_id="insert_measures_normal",
                python_callable=insert_measure,
                op_kwargs={"map_task_id": map_task.task_id},
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            )

            alert_task = PythonOperator(
                task_id="log_alert",
                python_callable=log_high_wind_alert,
                op_kwargs={
                    "map_task_id": map_task.task_id,
                    "threshold": wind_threshold,
                },
            )

            branch_task = BranchPythonOperator(
                task_id="branch_on_wind_speed",
                python_callable=choose_load_path,
                op_kwargs={
                    "map_task_id": map_task.task_id,
                    "normal_task_id": normal_load_task.task_id,
                    "alert_task_id": alert_task.task_id,
                    "threshold": wind_threshold,
                },
            )

            fetch_task >> map_task >> branch_task
            branch_task >> normal_load_task
            branch_task >> alert_task >> normal_load_task

        create_table_task >> city_group
