from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx
import pytest

from ingestion.clients.openaq import (
    OpenAQAuthError,
    OpenAQClient,
    OpenAQError,
    SensorRef,
    Station,
    normalize_unit,
    to_measurement,
)
from ingestion.models import Measurement
from ingestion.sinks import write_jsonl

UTC = timezone.utc
NOW = datetime(2026, 11, 16, 12, tzinfo=UTC)


def _location(loc_id, *, provider_id=223, last="2026-11-12T16:00:00Z", params=None):
    params = params or ["pm25", "o3", "no"]
    units = {"pm25": "µg/m³", "o3": "ppm", "no": "ppm"}
    return {
        "id": loc_id,
        "name": f"Station {loc_id} ",
        "timezone": "America/Monterrey",
        "provider": {"id": provider_id, "name": "Sinaica Mexico"},
        "isMonitor": True,
        "coordinates": {"latitude": 25.67, "longitude": -100.33},
        "datetimeLast": {"utc": last} if last else None,
        "sensors": [
            {
                "id": loc_id * 10 + i,
                "parameter": {"name": p, "units": units[p]},
            }
            for i, p in enumerate(params)
        ],
    }


def _hour_row(end: str, value=18.4, parameter="pm25", units="µg/m³"):
    end_dt = datetime.fromisoformat(end.replace("Z", "+00:00"))
    start = (end_dt - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "value": value,
        "parameter": {"id": 2, "name": parameter, "units": units},
        "period": {
            "datetimeFrom": {"utc": start},
            "datetimeTo": {"utc": end},
        },
        "coverage": {"percentComplete": 100.0},
    }


def _client(handler) -> tuple[OpenAQClient, list[float]]:
    sleeps: list[float] = []
    client = OpenAQClient(
        "test-key",
        transport=httpx.MockTransport(handler),
        sleep=sleeps.append,
        min_interval=0,
    )
    return client, sleeps


def _ok(results):
    return httpx.Response(
        200, json={"meta": {"found": len(results)}, "results": results}
    )


# --------------------------------------------------------------------------- #


def test_normalize_unit_handles_both_micro_signs():
    assert normalize_unit("µg/m³") == "ug/m3"
    assert normalize_unit("μg/m³") == "ug/m3"
    assert normalize_unit(" ppm ") == "ppm"
    assert normalize_unit("weird") == "weird"


def test_empty_api_key_is_rejected():
    with pytest.raises(OpenAQAuthError):
        OpenAQClient("")


def test_find_stations_filters_inactive_foreign_and_unwanted_parameters():
    seen = {}

    def handler(request: httpx.Request):
        seen.update(request.url.params)
        assert request.headers["X-API-Key"] == "test-key"
        return _ok(
            [
                _location(2),
                _location(1),
                _location(3, last="2025-05-15T17:00:00Z"),  # retired duplicate
                _location(4, provider_id=119),  # AirNow
                _location(5, params=["no"]),  # nothing we want
                _location(6, last=None),
            ]
        )

    client, _ = _client(handler)
    stations = client.find_stations(now=NOW)

    assert [s.location_id for s in stations] == [1, 2]
    assert {s.parameter for s in stations[0].sensors} == {"pm25", "o3"}
    assert stations[0].name == "Station 1"
    assert stations[0].station_id == "openaq-1"
    assert seen["bbox"] == "-100.75,25.4,-99.9,26.0"
    assert seen["providers_id"] == "223"
    assert seen["monitor"] == "true"


def test_paginates_until_short_page():
    pages = []

    def handler(request: httpx.Request):
        page = int(request.url.params["page"])
        pages.append(page)
        size = int(request.url.params["limit"])
        n = size if page < 3 else 1
        return _ok([_location(page * 10_000 + i) for i in range(n)])

    client, _ = _client(handler)
    stations = client.find_stations(now=NOW)
    assert pages == [1, 2, 3]
    assert len(stations) == 2001


def test_retries_429_using_reset_header_then_succeeds():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(429, headers={"x-ratelimit-reset": "7"})
        return _ok([])

    client, sleeps = _client(handler)
    assert client.find_stations(now=NOW) == []
    assert calls["n"] == 3
    assert sleeps == [8.0, 8.0]


def test_gives_up_after_max_retries():
    client, _ = _client(lambda r: httpx.Response(503))
    with pytest.raises(OpenAQError, match="503"):
        client.find_stations(now=NOW)
    assert client.request_count == 6


def test_auth_error_is_not_retried():
    client, _ = _client(lambda r: httpx.Response(401))
    with pytest.raises(OpenAQAuthError):
        client.find_stations(now=NOW)
    assert client.request_count == 1


def test_transport_errors_are_retried():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("boom")
        return _ok([])

    client, _ = _client(handler)
    assert client.find_stations(now=NOW) == []


def test_iter_hours_keeps_only_hours_ending_inside_window():
    def handler(request):
        assert request.url.path == "/v3/sensors/42/hours"
        assert request.url.params["datetime_from"] == "2026-11-12T10:00:00Z"
        assert request.url.params["datetime_to"] == "2026-11-12T13:00:00Z"
        return _ok(
            [
                _hour_row("2026-11-12T10:00:00Z"),  # ends on the open edge -> out
                _hour_row("2026-11-12T11:00:00Z"),
                _hour_row("2026-11-12T13:00:00Z"),  # closed edge -> in
                _hour_row("2026-11-12T14:00:00Z"),  # after window -> out
            ]
        )

    client, _ = _client(handler)
    rows = list(
        client.iter_hours(
            42,
            datetime(2026, 11, 12, 10, tzinfo=UTC),
            datetime(2026, 11, 12, 13, tzinfo=UTC),
        )
    )
    ends = [r["period"]["datetimeTo"]["utc"] for r in rows]
    assert ends == ["2026-11-12T11:00:00Z", "2026-11-12T13:00:00Z"]


def test_iter_hours_rejects_naive_datetimes():
    client, _ = _client(lambda r: _ok([]))
    with pytest.raises(ValueError):
        list(client.iter_hours(1, datetime(2026, 1, 1), datetime(2026, 1, 2)))


def test_to_measurement_maps_hour_ending_and_prefixes_ids():
    station = Station(
        location_id=4408711,
        name="Obispado",
        provider="Sinaica Mexico",
        latitude=25.67,
        longitude=-100.33,
        timezone="America/Monterrey",
        datetime_last=None,
        sensors=(),
    )
    sensor = SensorRef(sensor_id=13051191, parameter="pm25", unit="ug/m3")
    m = to_measurement(_hour_row("2026-11-12T11:00:00Z"), station, sensor, NOW)

    assert m.station_id == "openaq-4408711"
    assert m.sensor_id == "openaq-13051191"
    assert m.parameter == "pm25"
    assert m.value == 18.4
    assert m.unit == "ug/m3"
    assert m.measured_at == datetime(2026, 11, 12, 11, tzinfo=UTC)
    assert m.period_start == datetime(2026, 11, 12, 10, tzinfo=UTC)
    assert m.coverage_pct == 100.0
    assert m.source == "openaq"


def test_fetch_measurements_skips_failing_sensor_and_keeps_the_rest():
    def handler(request):
        if request.url.path.endswith("/locations"):
            return _ok([_location(1)])
        if request.url.path == "/v3/sensors/10/hours":
            return httpx.Response(404, text="not found")
        return _ok([_hour_row("2026-11-12T11:00:00Z", parameter="o3", units="ppm")])

    client, _ = _client(handler)
    stations = client.find_stations(now=NOW)
    result = client.fetch_measurements(
        stations,
        datetime(2026, 11, 12, 10, tzinfo=UTC),
        datetime(2026, 11, 12, 13, tzinfo=UTC),
        ingested_at=NOW,
    )
    assert list(result.failed_sensors) == [10]
    assert [m.sensor_id for m in result.measurements] == ["openaq-11"]
    assert result.measurements[0].unit == "ppm"


def test_write_jsonl_is_atomic_and_overwrites(tmp_path: Path):
    m = Measurement(
        station_id="s",
        sensor_id="x",
        parameter="pm25",
        value=None,
        unit="ug/m3",
        measured_at=NOW,
        source="test",
    )
    path = tmp_path / "a" / "part-0.jsonl"
    assert write_jsonl([m, m], path) == 2
    assert write_jsonl([m], path) == 1
    lines = path.read_text().splitlines()
    assert len(lines) == 1
    assert json.loads(lines[0])["value"] is None
    assert list(path.parent.iterdir()) == [path]


def test_to_utc_returns_plain_utc_datetime():
    from ingestion.clients.openaq import to_utc

    mty = timezone(timedelta(hours=-6))
    out = to_utc(datetime(2026, 11, 16, 21, tzinfo=mty))
    assert type(out) is datetime
    assert out == datetime(2026, 11, 17, 3, tzinfo=UTC)
    assert out.tzinfo is UTC
    with pytest.raises(ValueError):
        to_utc(datetime(2026, 11, 16))


def test_pendulum_window_from_airflow_context_stays_aware():
    """Regression: Airflow passes pendulum DateTimes; with pendulum 3.2,
    ``.astimezone(timezone.utc) - timedelta`` returned a *naive* DateTime and
    extract_measurements failed with 'datetime must be timezone-aware'."""
    pendulum = pytest.importorskip("pendulum")
    from ingestion.clients.openaq import to_utc

    end = pendulum.datetime(2026, 11, 17, 3, tz="UTC")
    start = to_utc(end) - timedelta(hours=120)
    assert start.tzinfo is not None
    assert start == datetime(2026, 11, 12, 3, tzinfo=UTC)

    seen = {}

    def handler(request):
        seen.update(request.url.params)
        return _ok([_hour_row("2026-11-12T11:00:00Z")])

    client, _ = _client(handler)
    station = Station(
        location_id=1,
        name="x",
        provider="Sinaica Mexico",
        latitude=None,
        longitude=None,
        timezone=None,
        datetime_last=None,
        sensors=(SensorRef(sensor_id=7, parameter="pm25", unit="ug/m3"),),
    )
    result = client.fetch_measurements([station], end - timedelta(hours=120), end)
    assert len(result.measurements) == 1
    assert seen["datetime_from"] == "2026-11-12T03:00:00Z"
    assert seen["datetime_to"] == "2026-11-17T03:00:00Z"
