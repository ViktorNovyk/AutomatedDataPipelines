import json
import re
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.sdk import DAG, Asset, Param, Variable, task

RAW_WEATHER_ASSET = Asset("file:///opt/airflow/storage/weather_raw/single_city")
CITY_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _get_required_var(name: str) -> str:
    value = Variable.get(name)
    if not value:
        raise AirflowException(f"Variable `{name}` must be set")
    return value


def _slugify_city(city: str) -> str:
    normalized = city.strip().lower()
    if not normalized:
        raise AirflowException("City cannot be empty")

    slug = CITY_SLUG_RE.sub("_", normalized).strip("_")
    if not slug:
        raise AirflowException(f"City `{city}` cannot be converted to a safe file path")
    return slug


def _build_raw_payload_path(storage_root: str, city: str, execution_date: str, run_id: str) -> Path:
    safe_city = _slugify_city(city)
    safe_run_id = run_id.replace(":", "_").replace("+", "_").replace("/", "_")
    return (
        Path(storage_root)
        / "weather_raw"
        / f"city={safe_city}"
        / f"date={execution_date}"
        / f"run={safe_run_id}.json"
    )


with DAG(
    dag_id="weather_ingestion_dag",
    description="Fetches weather data and stores raw payload into shared storage",
    start_date=datetime(2026, 4, 4),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "city": Param("Kyiv", type="string", minLength=1),
        "target_date": Param(None, type=["null", "string"], format="date"),
        "api_path": Param("/v1/history.json", type="string", minLength=1),
        "request_timeout_sec": Param(30, type="integer", minimum=1),
        "raw_storage_root": Param("/opt/airflow/storage", type="string", minLength=1),
    },
) as dag:
    @task(
        task_id="fetch_weather_payload",
        retries=2,
        retry_delay=timedelta(seconds=2),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(seconds=8),
    )
    def fetch_weather_payload(
        city: str,
        execution_date: str,
        api_path: str,
        request_timeout_sec: int | str,
    ) -> dict:
        base_url = _get_required_var("WEATHER_API_BASE_URL").rstrip("/")
        api_key = _get_required_var("WEATHER_API_KEY")

        timeout_sec = int(request_timeout_sec)
        if timeout_sec <= 0:
            raise AirflowException("`request_timeout_sec` must be greater than 0")

        endpoint = f"{base_url}/{api_path.lstrip('/')}"
        try:
            response = requests.get(
                endpoint,
                params={"q": city, "dt": execution_date, "key": api_key},
                timeout=timeout_sec,
            )
        except requests.RequestException as exc:
            raise AirflowException(f"Weather API request failed for city `{city}`: {exc}") from exc

        status_code = response.status_code
        if 200 <= status_code < 300:
            return response.json()

        if 400 <= status_code < 500:
            raise AirflowFailException(
                f"Weather API returned client error {status_code} for city `{city}`"
            )

        if 500 <= status_code < 600:
            raise AirflowException(f"Weather API returned server error {status_code} for city `{city}`")

        raise AirflowFailException(
            f"Weather API returned unexpected status {status_code} for city `{city}`"
        )

    @task(task_id="store_raw_payload", outlets=[RAW_WEATHER_ASSET])
    def store_raw_payload(
        payload: dict,
        city: str,
        execution_date: str,
        storage_root: str,
        run_id: str,
        *,
        outlet_events=None,
    ) -> str:
        raw_path = _build_raw_payload_path(
            storage_root=storage_root,
            city=city,
            execution_date=execution_date,
            run_id=run_id,
        )
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        raw_path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")

        if outlet_events is None:
            raise AirflowException("`outlet_events` is required to emit asset updates")

        outlet_events[RAW_WEATHER_ASSET].extra = {
            "local_path": str(raw_path),
            "city": city,
            "execution_date": execution_date,
            "run_id": run_id,
            "uri": raw_path.as_uri(),
        }
        return str(raw_path)

    weather_payload = fetch_weather_payload(
        city="{{ params.city }}",
        execution_date="{{ params.target_date or ds }}",
        api_path="{{ params.api_path }}",
        request_timeout_sec="{{ params.request_timeout_sec }}",
    )

    store_raw_payload(
        payload=weather_payload,
        city="{{ params.city }}",
        execution_date="{{ params.target_date or ds }}",
        storage_root="{{ params.raw_storage_root }}",
        run_id="{{ run_id }}",
    )
