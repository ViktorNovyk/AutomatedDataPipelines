import json
import logging
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.sdk import DAG, Param, task


def _read_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def _as_bool(value: bool | str) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes", "y"}


with DAG(
    dag_id="weather_quality_dag",
    description="Runs quality checks for weather ETL outputs",
    start_date=datetime(2026, 4, 26),
    schedule=None,
    catchup=False,
    max_active_runs=10,
    render_template_as_native_obj=True,
    params={
        "strict_mode": Param(True, type="boolean"),
    },
) as dag:
    @task(task_id="check_extracted_json_quality")
    def check_extracted_json_quality(
        city: str,
        execution_date: str,
        raw_payload_path: str,
        extract_succeeded: bool | str,
        strict_mode: bool | str,
    ) -> dict:
        strict = _as_bool(strict_mode)
        extract_ok = _as_bool(extract_succeeded)
        if not city:
            raise AirflowException("Quality run conf is missing `city`")
        if not execution_date:
            raise AirflowException("Quality run conf is missing `execution_date`")

        if not extract_ok or not raw_payload_path:
            metrics = {
                "quality_status": "skipped_no_raw_payload",
                "city": city,
                "execution_date": execution_date,
                "extract_succeeded": extract_ok,
            }
            logging.warning("weather_quality_metrics=%s", json.dumps(metrics, sort_keys=True))
            return metrics

        raw_path = Path(raw_payload_path)
        if not raw_path.exists():
            raise AirflowException(f"Extracted raw payload file was not found: `{raw_path}`")

        raw_payload = _read_json(raw_path)

        failed_checks: list[str] = []
        try:
            forecast_days = raw_payload["forecast"]["forecastday"]
            first_day = forecast_days[0]
            day_data = first_day["day"]
            payload_city = raw_payload["location"]["name"]
            payload_date = first_day["date"]
            avg_temp_c = float(day_data["avgtemp_c"])
            avg_humidity = float(day_data["avghumidity"])
            avg_visibility_km = float(day_data["avgvis_km"])
            max_wind_kph = float(day_data["maxwind_kph"])
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            raise AirflowException(f"Raw payload structure is invalid: {exc}") from exc

        city_match = payload_city.strip().lower() == city.strip().lower()
        date_match = payload_date == execution_date
        humidity_in_range = 0 <= avg_humidity <= 100
        visibility_non_negative = avg_visibility_km >= 0
        wind_non_negative = max_wind_kph >= 0
        temp_reasonable = -90 <= avg_temp_c <= 65

        if not city_match:
            failed_checks.append("city_match")
        if not date_match:
            failed_checks.append("date_match")
        if not humidity_in_range:
            failed_checks.append("humidity_range")
        if not visibility_non_negative:
            failed_checks.append("visibility_non_negative")
        if not wind_non_negative:
            failed_checks.append("wind_non_negative")
        if not temp_reasonable:
            failed_checks.append("temperature_reasonable")

        metrics = {
            "quality_status": "ok" if not failed_checks else "failed_checks",
            "city_expected": city,
            "city_payload": payload_city,
            "execution_date_expected": execution_date,
            "execution_date_payload": payload_date,
            "payload_size_bytes": raw_path.stat().st_size,
            "forecast_days_count": len(forecast_days),
            "avgtemp_c": avg_temp_c,
            "avghumidity": avg_humidity,
            "avgvis_km": avg_visibility_km,
            "maxwind_kph": max_wind_kph,
            "checks": {
                "city_match": city_match,
                "date_match": date_match,
                "humidity_range_0_100": humidity_in_range,
                "visibility_non_negative": visibility_non_negative,
                "wind_non_negative": wind_non_negative,
                "temperature_reasonable_minus90_65": temp_reasonable,
            },
            "failed_checks": failed_checks,
        }
        logging.info("weather_quality_metrics=%s", json.dumps(metrics, sort_keys=True))

        if strict and failed_checks:
            raise AirflowException(f"Raw payload quality checks failed: {', '.join(failed_checks)}")

        return metrics

    check_extracted_json_quality(
        city="{{ dag_run.conf.get('city') }}",
        execution_date="{{ dag_run.conf.get('execution_date') }}",
        raw_payload_path="{{ dag_run.conf.get('raw_payload_path') }}",
        extract_succeeded="{{ dag_run.conf.get('extract_succeeded', False) }}",
        strict_mode="{{ params.strict_mode }}",
    )
