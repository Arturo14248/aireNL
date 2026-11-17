"""ingest_openaq: hourly pull of Monterrey metro air quality from OpenAQ.

Flow:
    discover_stations  ->  extract_measurements

* ``discover_stations`` asks OpenAQ which official (SINAICA) monitors inside the
  metro bounding box are currently reporting, and keeps the NOM pollutants.
  It also writes a dated snapshot of the station catalogue (seed material for
  ``analytics/seeds/stations.csv``). Only that small list goes through XCom.
* ``extract_measurements`` fetches the hourly averages whose hour ends in
  ``(data_interval_end - lookback_hours, data_interval_end]`` and lands them as
  JSON Lines, one file per run, overwritten on rerun (idempotent).

Why a lookback instead of "only this hour": SINAICA data reaches OpenAQ with a
delay of up to several days, so asking only for the last hour would almost always
return nothing. Overlapping windows produce repeated records by design; silver
deduplicates them on ``sensor_id`` + ``measured_at``.

Kafka is not deployed yet: when it is, replace ``write_jsonl`` with a producer to
``aq.measurements.raw`` keyed by ``station_id``. The client does not change.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pendulum
from airflow.sdk import Param, Variable, dag, task
from airflow.timetables.interval import CronDataIntervalTimetable

from ingestion.clients.openaq import OpenAQClient, Station, to_utc
from ingestion.sinks import write_jsonl

LANDING_DIR = Path(os.environ.get("AIRENL_LANDING_DIR", "/opt/airflow/data/landing"))
API_KEY_VARIABLE = "openaq_api_key"  # resolved from AIRFLOW_VAR_OPENAQ_API_KEY


def _window_end(context) -> datetime:
    """End of the run's data interval as a plain UTC ``datetime``.

    Falls back to the current hour for manual runs triggered without a logical
    date (allowed in Airflow 3). ``to_utc`` converts Airflow's pendulum value to
    a stdlib datetime so ``window_end - timedelta(...)`` stays timezone-aware.
    """
    end = context.get("data_interval_end")
    if end is None:
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return to_utc(end)


@dag(
    dag_id="ingest_openaq",
    description="OpenAQ (SINAICA monitors, Monterrey metro) -> landing zone",
    schedule=CronDataIntervalTimetable("0 * * * *", timezone="UTC"),
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "execution_timeout": timedelta(minutes=30),
    },
    params={
        "lookback_hours": Param(
            120,
            type="integer",
            minimum=1,
            maximum=720,
            description="Hours before data_interval_end to (re)request.",
        ),
    },
    tags=["ingestion", "openaq", "air-quality"],
)
def ingest_openaq():
    @task
    def discover_stations(**context) -> list[dict]:
        with OpenAQClient(api_key=Variable.get(API_KEY_VARIABLE)) as client:
            stations = client.find_stations()
        if not stations:
            raise RuntimeError("OpenAQ returned no active stations for the metro bbox")

        snapshot = [s.model_dump(mode="json") for s in stations]
        day = _window_end(context).strftime("%Y-%m-%d")
        path = LANDING_DIR / "openaq" / "stations" / f"snapshot_date={day}"
        path.mkdir(parents=True, exist_ok=True)
        (path / "stations.json").write_text(
            json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return snapshot

    @task
    def extract_measurements(stations: list[dict], **context) -> dict:
        window_end = _window_end(context)
        lookback = int(context["params"]["lookback_hours"])
        window_start = window_end - timedelta(hours=lookback)
        parsed = [Station.model_validate(s) for s in stations]

        with OpenAQClient(api_key=Variable.get(API_KEY_VARIABLE)) as client:
            result = client.fetch_measurements(parsed, window_start, window_end)
            requests = client.request_count

        path = (
            LANDING_DIR
            / "openaq"
            / "measurements"
            / f"window_end={window_end:%Y-%m-%dT%H}"
            / "part-0.jsonl"
        )
        count = write_jsonl(result.measurements, path)
        summary = {
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "records": count,
            "requests": requests,
            "failed_sensors": result.failed_sensors,
            "path": str(path),
        }
        print(json.dumps(summary, indent=2))

        # Data from the healthy sensors is already landed; failing the task makes
        # the broken sensors visible and the retry rewrites the same file.
        if result.failed_sensors:
            raise RuntimeError(
                f"{len(result.failed_sensors)} sensors failed: {summary}"
            )
        return summary

    extract_measurements(discover_stations())


ingest_openaq()
