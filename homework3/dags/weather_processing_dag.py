import json
import re
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, Asset, Param, Variable, task

RAW_WEATHER_ASSET = Asset("file:///opt/airflow/storage/weather_raw/single_city")
DB_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _get_required_var(name: str) -> str:
    value = Variable.get(name)
    if not value:
        raise AirflowException(f"Variable `{name}` must be set")
    return value


def _validate_table_name(table_name: str) -> str:
    if not DB_IDENTIFIER_RE.fullmatch(table_name):
        raise AirflowException(f"Invalid table name `{table_name}`")
    return table_name


with DAG(
    dag_id="weather_processing_dag",
    description="Processes raw weather payload from shared storage and loads final dataset",
    start_date=datetime(2026, 4, 26),
    schedule=[RAW_WEATHER_ASSET],
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    params={
        "measures_table": Param("measures", type="string", minLength=1),
    },
) as dag:
    @task(task_id="load_raw_payload", inlets=[RAW_WEATHER_ASSET])
    def load_raw_payload(*, inlet_events=None) -> dict:
        if inlet_events is None:
            raise AirflowException("`inlet_events` is missing")

        try:
            events = inlet_events[RAW_WEATHER_ASSET]
        except KeyError:
            events = []
        if not events:
            raise AirflowException("No triggering asset events were provided")

        latest_event = events[-1]
        local_path = None
        if latest_event.extra:
            local_path = latest_event.extra.get("local_path")

        if not local_path:
            raise AirflowException("Asset event does not contain `local_path` metadata")

        path = Path(local_path)
        if not path.exists():
            raise AirflowException(f"Raw payload file does not exist: `{path}`")

        with path.open("r", encoding="utf-8") as payload_file:
            return json.load(payload_file)

    @task(task_id="map_weather_payload")
    def map_weather_payload(payload: dict) -> dict:
        forecast_day = payload["forecast"]["forecastday"][0]
        day = forecast_day["day"]
        return {
            "name": payload["location"]["name"],
            "timestamp": forecast_day["date"],
            "temp_c": day["avgtemp_c"],
            "humidity": day["avghumidity"],
            "visibility": day["avgvis_km"],
            "wind_speed_kph": day["maxwind_kph"],
        }

    @task(task_id="create_measures_table")
    def create_measures_table(table_name: str) -> None:
        db_conn_id = _get_required_var("WEATHER_DB")
        hook = PostgresHook(postgres_conn_id=db_conn_id)
        validated_table_name = _validate_table_name(table_name)
        hook.run(
            f"""
            create table if not exists {validated_table_name} (
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

    @task(task_id="insert_measure")
    def insert_measure(row: dict, table_name: str) -> None:
        db_conn_id = _get_required_var("WEATHER_DB")
        hook = PostgresHook(postgres_conn_id=db_conn_id)
        validated_table_name = _validate_table_name(table_name)

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
                f"""
                insert into {validated_table_name} (timestamp, temp_c, humidity, visibility, wind_speed_kph, name)
                values (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING
                """,
                values,
            )
        conn.commit()
        conn.close()

    raw_payload = load_raw_payload()
    mapped_row = map_weather_payload(raw_payload)
    create_table = create_measures_table(table_name="{{ params.measures_table }}")
    load_measure = insert_measure(row=mapped_row, table_name="{{ params.measures_table }}")

    create_table >> load_measure
