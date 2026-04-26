import json
import logging
import re
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, Param, Variable, task
from airflow.utils.trigger_rule import TriggerRule

CITY_SLUG_RE = re.compile(r"[^a-z0-9]+")
DB_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MAPPED_REQUIRED_KEYS = {
    "name",
    "timestamp",
    "temp_c",
    "humidity",
    "visibility",
    "wind_speed_kph",
}


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


def _slugify_city(city: str) -> str:
    normalized = city.strip().lower()
    if not normalized:
        raise AirflowException("City cannot be empty")

    slug = CITY_SLUG_RE.sub("_", normalized).strip("_")
    if not slug:
        raise AirflowException(f"City `{city}` cannot be converted to a safe slug")
    return slug


def _validate_table_name(table_name: str) -> str:
    if not DB_IDENTIFIER_RE.fullmatch(table_name):
        raise AirflowException(f"Invalid table name `{table_name}`")
    return table_name


def _city_date_dir(storage_root: str, city: str, execution_date: str) -> Path:
    return (
        Path(storage_root)
        / "weather_pipeline"
        / f"city={_slugify_city(city)}"
        / f"date={execution_date}"
    )


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")
    temp_path.replace(path)


def _read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _is_valid_raw_payload(payload: dict) -> bool:
    try:
        forecast_day = payload["forecast"]["forecastday"][0]
        day = forecast_day["day"]
        _ = payload["location"]["name"]
        _ = forecast_day["date"]
        _ = day["avgtemp_c"]
        _ = day["avghumidity"]
        _ = day["avgvis_km"]
        _ = day["maxwind_kph"]
        return True
    except (KeyError, IndexError, TypeError):
        return False


def _is_valid_mapped_payload(payload: dict) -> bool:
    return isinstance(payload, dict) and MAPPED_REQUIRED_KEYS.issubset(payload.keys())


def _create_measures_table(hook: PostgresHook, table_name: str) -> None:
    hook.run(
        f"""
        create table if not exists {table_name} (
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


def _build_weather_city_dag(city: str) -> DAG:
    city_slug = _slugify_city(city)
    dag_id = f"weather_etl_{city_slug}"

    with DAG(
        dag_id=dag_id,
        description=f"Weather ETL with checkpoint storage for {city}",
        start_date=datetime(2026, 4, 26),
        schedule="@daily",
        catchup=True,
        max_active_runs=1,
        render_template_as_native_obj=True,
        params={
            "target_date": Param(None, type=["null", "string"], format="date"),
            "api_path": Param("/v1/history.json", type="string", minLength=1),
            "request_timeout_sec": Param(30, type="integer", minimum=1),
            "storage_root": Param("/opt/airflow/storage", type="string", minLength=1),
            "measures_table": Param("measures", type="string", minLength=1),
            "wind_speed_alert_threshold": Param(20.0, type="number"),
            "quality_dag_id": Param("weather_quality_dag", type="string", minLength=1),
        },
    ) as dag:
        @task(
            task_id="extract_weather_to_storage",
            retries=2,
            retry_delay=timedelta(seconds=2),
            retry_exponential_backoff=True,
            max_retry_delay=timedelta(seconds=8),
        )
        def extract_weather_to_storage(
            city_name: str,
            execution_date: str,
            api_path: str,
            request_timeout_sec: int | str,
            storage_root: str,
        ) -> str:
            base_dir = _city_date_dir(
                storage_root=storage_root,
                city=city_name,
                execution_date=execution_date,
            )
            raw_path = base_dir / "01_raw.json"

            if raw_path.exists():
                try:
                    existing_payload = _read_json(raw_path)
                    if _is_valid_raw_payload(existing_payload):
                        logging.info(
                            "Resume checkpoint hit for extract step; reusing existing raw payload: %s",
                            raw_path,
                        )
                        return str(raw_path)
                except (OSError, json.JSONDecodeError):
                    logging.warning("Raw checkpoint file is invalid and will be replaced: %s", raw_path)

            base_url = _get_required_var("WEATHER_API_BASE_URL").rstrip("/")
            api_key = _get_required_var("WEATHER_API_KEY")

            timeout_sec = int(request_timeout_sec)
            if timeout_sec <= 0:
                raise AirflowException("`request_timeout_sec` must be greater than 0")

            endpoint = f"{base_url}/{api_path.lstrip('/')}"
            try:
                response = requests.get(
                    endpoint,
                    params={"q": city_name, "dt": execution_date, "key": api_key},
                    timeout=timeout_sec,
                )
            except requests.RequestException as exc:
                raise AirflowException(
                    f"Weather API request failed for city `{city_name}`: {exc}"
                ) from exc

            status_code = response.status_code
            if 200 <= status_code < 300:
                payload = response.json()
                if not _is_valid_raw_payload(payload):
                    raise AirflowException(
                        f"Weather API payload structure is invalid for city `{city_name}`"
                    )
                _write_json(raw_path, payload)
                return str(raw_path)

            if 400 <= status_code < 500:
                raise AirflowFailException(
                    f"Weather API returned client error {status_code} for city `{city_name}`"
                )

            if 500 <= status_code < 600:
                raise AirflowException(
                    f"Weather API returned server error {status_code} for city `{city_name}`"
                )

            raise AirflowFailException(
                f"Weather API returned unexpected status {status_code} for city `{city_name}`"
            )

        @task(task_id="transform_weather_to_storage")
        def transform_weather_to_storage(raw_payload_path: str) -> str:
            raw_path = Path(raw_payload_path)
            mapped_path = raw_path.with_name("02_mapped.json")

            if mapped_path.exists():
                try:
                    existing_mapped = _read_json(mapped_path)
                    if _is_valid_mapped_payload(existing_mapped):
                        logging.info(
                            "Resume checkpoint hit for transform step; reusing existing mapped payload: %s",
                            mapped_path,
                        )
                        return str(mapped_path)
                except (OSError, json.JSONDecodeError):
                    logging.warning(
                        "Mapped checkpoint file is invalid and will be replaced: %s",
                        mapped_path,
                    )

            payload = _read_json(raw_path)
            if not _is_valid_raw_payload(payload):
                raise AirflowException(f"Raw payload has invalid structure: `{raw_path}`")

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
            _write_json(mapped_path, mapped_row)
            return str(mapped_path)

        @task(task_id="log_high_wind_alert")
        def log_high_wind_alert(mapped_payload_path: str, threshold: float | str) -> None:
            mapped_row = _read_json(Path(mapped_payload_path))
            wind_speed = float(mapped_row["wind_speed_kph"])
            threshold_value = float(threshold)
            if wind_speed > threshold_value:
                logging.warning(
                    "wind speed %s is greater than %s for city `%s`",
                    wind_speed,
                    threshold_value,
                    mapped_row["name"],
                )

        @task(task_id="load_weather_to_db")
        def load_weather_to_db(mapped_payload_path: str, table_name: str) -> str:
            mapped_path = Path(mapped_payload_path)
            load_result_path = mapped_path.with_name("03_load_result.json")

            if load_result_path.exists():
                try:
                    load_result = _read_json(load_result_path)
                    if load_result.get("status") == "loaded":
                        logging.info(
                            "Resume checkpoint hit for load step; reusing existing load result: %s",
                            load_result_path,
                        )
                        return str(load_result_path)
                except (OSError, json.JSONDecodeError):
                    logging.warning(
                        "Load checkpoint file is invalid and will be replaced: %s",
                        load_result_path,
                    )

            mapped_row = _read_json(mapped_path)
            if not _is_valid_mapped_payload(mapped_row):
                raise AirflowException(f"Mapped payload has invalid structure: `{mapped_path}`")

            validated_table_name = _validate_table_name(table_name)
            db_conn_id = _get_required_var("WEATHER_DB")
            hook = PostgresHook(postgres_conn_id=db_conn_id)
            _create_measures_table(hook, validated_table_name)

            conn = hook.get_conn()
            values = (
                mapped_row["timestamp"],
                mapped_row["temp_c"],
                mapped_row["humidity"],
                mapped_row["visibility"],
                mapped_row["wind_speed_kph"],
                mapped_row["name"],
            )
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    insert into {validated_table_name} (timestamp, temp_c, humidity, visibility, wind_speed_kph, name)
                    values (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                    """,
                    values,
                )
            conn.commit()
            conn.close()

            _write_json(
                load_result_path,
                {
                    "status": "loaded",
                    "table": validated_table_name,
                    "name": mapped_row["name"],
                    "timestamp": mapped_row["timestamp"],
                },
            )
            return str(load_result_path)

        extracted_path = extract_weather_to_storage(
            city_name=city,
            execution_date="{{ params.target_date or ds }}",
            api_path="{{ params.api_path }}",
            request_timeout_sec="{{ params.request_timeout_sec }}",
            storage_root="{{ params.storage_root }}",
        )
        transformed_path = transform_weather_to_storage(raw_payload_path=extracted_path)
        alert_logged = log_high_wind_alert(
            mapped_payload_path=transformed_path,
            threshold="{{ params.wind_speed_alert_threshold }}",
        )
        loaded_path = load_weather_to_db(
            mapped_payload_path=transformed_path,
            table_name="{{ params.measures_table }}",
        )

        trigger_quality = TriggerDagRunOperator(
            task_id="trigger_quality_checks",
            trigger_dag_id="{{ params.quality_dag_id }}",
            trigger_run_id="quality__{{ dag.dag_id }}__{{ run_id }}",
            conf={
                "source_dag_id": "{{ dag.dag_id }}",
                "source_run_id": "{{ run_id }}",
                "city": city,
                "execution_date": "{{ params.target_date or ds }}",
                "raw_payload_path": "{{ ti.xcom_pull(task_ids='extract_weather_to_storage') }}",
                "extract_succeeded": "{{ ti.xcom_pull(task_ids='extract_weather_to_storage') is not none }}",
            },
            skip_when_already_exists=True,
            wait_for_completion=False,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        extracted_path >> trigger_quality

    return dag


for _city in _parse_cities(_get_required_var("WEATHER_API_CITIES")):
    _generated_dag = _build_weather_city_dag(_city)
    globals()[_generated_dag.dag_id] = _generated_dag
