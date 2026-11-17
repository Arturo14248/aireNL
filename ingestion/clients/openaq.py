"""Reusable OpenAQ v3 client for the AireNL ingestion layer.

Talks to the REST API directly with ``httpx`` (no ``openaq`` SDK) so it runs inside
the stock Airflow image, which already ships ``httpx`` and ``pydantic``.

Responsibilities:

* find the active government monitoring stations inside a bounding box;
* fetch hourly averages for each sensor in a time window, with pagination,
  client-side throttling, exponential-backoff retries and rate-limit handling;
* translate every row into the standard ``Measurement`` contract.

Window semantics: OpenAQ's ``/sensors/{id}/hours`` endpoint matches on the *end*
of each hour (``datetime_from < period_end <= datetime_to``), so a window
``(start, end]`` returns exactly the hours that finished inside it. The client
re-applies that filter locally so the guarantee does not depend on the API.
"""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

from ingestion.models import Measurement

log = logging.getLogger(__name__)

BASE_URL = "https://api.openaq.org/v3"

# (min_lon, min_lat, max_lon, max_lat). Covers the Monterrey metro area from
# García / Santa Catarina in the west to Cadereyta / Pesquería in the east.
AMM_BBOX: tuple[float, float, float, float] = (-100.75, 25.40, -99.90, 26.00)

# OpenAQ provider id for "Sinaica Mexico" (the official state network, SIMA).
SINAICA_PROVIDER_ID = 223

# Pollutants with a Mexican health standard (NOM-020/021/022/023/025-SSA1).
NOM_PARAMETERS: tuple[str, ...] = ("pm25", "pm10", "o3", "no2", "so2", "co")

SOURCE = "openaq"
MAX_PAGE_SIZE = 1000
RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})

_UNIT_ALIASES = {
    "µg/m³": "ug/m3",  # micro sign U+00B5
    "μg/m³": "ug/m3",  # greek mu U+03BC
    "mg/m³": "mg/m3",
    "ppm": "ppm",
    "ppb": "ppb",
}


class OpenAQError(RuntimeError):
    """The API could not be queried successfully."""


class OpenAQAuthError(OpenAQError):
    """Missing or rejected API key. Never retried."""


class SensorRef(BaseModel):
    model_config = ConfigDict(frozen=True)

    sensor_id: int
    parameter: str
    unit: str


class Station(BaseModel):
    """An OpenAQ location reduced to what ingestion needs. JSON-serialisable."""

    model_config = ConfigDict(frozen=True)

    location_id: int
    name: str
    provider: str
    latitude: float | None
    longitude: float | None
    timezone: str | None
    datetime_last: datetime | None
    sensors: tuple[SensorRef, ...]

    @property
    def station_id(self) -> str:
        return f"{SOURCE}-{self.location_id}"


@dataclass
class FetchResult:
    measurements: list[Measurement] = field(default_factory=list)
    failed_sensors: dict[int, str] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# Pure helpers (no I/O)
# --------------------------------------------------------------------------- #


def normalize_unit(unit: str) -> str:
    """Return an ASCII spelling of the unit. Values are never converted here."""
    unit = unit.strip()
    return _UNIT_ALIASES.get(unit, unit)


def parse_ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _iso(dt: datetime) -> str:
    return to_utc(dt).strftime("%Y-%m-%dT%H:%M:%SZ")


def to_utc(dt: datetime) -> datetime:
    """Return ``dt`` as a plain stdlib ``datetime`` in UTC.

    Rejects naive values. Rebuilding the object (instead of returning a
    ``datetime`` subclass such as pendulum's ``DateTime``, which Airflow puts in
    the task context) matters: pendulum 3.2 drops a stdlib ``tzinfo`` when a
    ``timedelta`` is subtracted, silently turning the window naive.
    """
    if dt.tzinfo is None or dt.utcoffset() is None:
        raise ValueError(f"datetime must be timezone-aware, got {dt!r}")
    u = dt.astimezone(timezone.utc)
    return datetime(
        u.year,
        u.month,
        u.day,
        u.hour,
        u.minute,
        u.second,
        u.microsecond,
        tzinfo=timezone.utc,
    )


def station_from_location(loc: Mapping[str, Any]) -> Station:
    coords = loc.get("coordinates") or {}
    provider = loc.get("provider") or {}
    last = loc.get("datetimeLast") or {}
    sensors = tuple(
        SensorRef(
            sensor_id=s["id"],
            parameter=s["parameter"]["name"],
            unit=normalize_unit(s["parameter"]["units"]),
        )
        for s in loc.get("sensors") or []
    )
    return Station(
        location_id=loc["id"],
        name=(loc.get("name") or "").strip(),
        provider=provider.get("name", ""),
        latitude=coords.get("latitude"),
        longitude=coords.get("longitude"),
        timezone=loc.get("timezone"),
        datetime_last=parse_ts(last.get("utc")),
        sensors=sensors,
    )


def to_measurement(
    row: Mapping[str, Any],
    station: Station,
    sensor: SensorRef,
    ingested_at: datetime,
) -> Measurement:
    """Translate one ``/sensors/{id}/hours`` row into the standard contract."""
    period = row["period"]
    parameter = row.get("parameter") or {}
    coverage = row.get("coverage") or {}
    return Measurement(
        station_id=station.station_id,
        sensor_id=f"{SOURCE}-{sensor.sensor_id}",
        parameter=parameter.get("name") or sensor.parameter,
        value=row.get("value"),
        unit=normalize_unit(parameter.get("units") or sensor.unit),
        measured_at=parse_ts(period["datetimeTo"]["utc"]),
        period_start=parse_ts(period["datetimeFrom"]["utc"]),
        coverage_pct=coverage.get("percentComplete"),
        source=SOURCE,
        ingested_at=ingested_at,
    )


# --------------------------------------------------------------------------- #
# HTTP client
# --------------------------------------------------------------------------- #


class OpenAQClient:
    """Thin, retrying OpenAQ v3 client. Use as a context manager.

    ``min_interval`` spaces requests so a run stays under the free tier's
    60 requests/minute; the ``x-ratelimit-*`` headers are honoured on top of it.
    ``transport`` and ``sleep`` exist so tests can run offline and instantly.
    """

    def __init__(
        self,
        api_key: str,
        *,
        base_url: str = BASE_URL,
        timeout: float = 30.0,
        max_retries: int = 5,
        backoff_base: float = 2.0,
        max_backoff: float = 120.0,
        min_interval: float = 1.1,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if not api_key:
            raise OpenAQAuthError("OpenAQ API key is empty (set OPENAQ_API_KEY)")
        self._http = httpx.Client(
            base_url=base_url,
            headers={
                "X-API-Key": api_key,
                "Accept": "application/json",
                "User-Agent": "airenl-ingestion",
            },
            timeout=timeout,
            transport=transport,
        )
        self._max_retries = max_retries
        self._backoff_base = backoff_base
        self._max_backoff = max_backoff
        self._min_interval = min_interval
        self._sleep = sleep
        self._last_request = float("-inf")
        self.request_count = 0

    # -- lifecycle ---------------------------------------------------------- #

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> OpenAQClient:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # -- low level ---------------------------------------------------------- #

    def _throttle(self) -> None:
        wait = self._min_interval - (time.monotonic() - self._last_request)
        if wait > 0:
            self._sleep(wait)
        self._last_request = time.monotonic()

    def _backoff(self, attempt: int) -> float:
        return min(self._max_backoff, self._backoff_base**attempt) + random.uniform(
            0, 1
        )

    def _retry_delay(self, resp: httpx.Response, attempt: int) -> float:
        for header in ("retry-after", "x-ratelimit-reset"):
            raw = resp.headers.get(header)
            if raw and raw.isdigit():
                return min(self._max_backoff, float(raw) + 1)
        return self._backoff(attempt)

    def _respect_rate_limit(self, resp: httpx.Response) -> None:
        remaining = resp.headers.get("x-ratelimit-remaining")
        reset = resp.headers.get("x-ratelimit-reset", "")
        if remaining == "0" and reset.isdigit():
            log.info("OpenAQ rate limit reached; sleeping %ss", reset)
            self._sleep(float(reset) + 1)

    def _get(self, path: str, params: Mapping[str, Any]) -> dict[str, Any]:
        attempts = self._max_retries + 1
        for attempt in range(attempts):
            last_try = attempt == attempts - 1
            self._throttle()
            self.request_count += 1
            try:
                resp = self._http.get(path, params=dict(params))
            except httpx.TransportError as exc:
                if last_try:
                    raise OpenAQError(f"GET {path} failed: {exc}") from exc
                delay = self._backoff(attempt)
                log.warning(
                    "GET %s transport error (%s); retry in %.1fs", path, exc, delay
                )
                self._sleep(delay)
                continue

            if resp.status_code in (401, 403):
                raise OpenAQAuthError(f"GET {path} -> HTTP {resp.status_code}")
            if resp.status_code in RETRYABLE_STATUS:
                if last_try:
                    raise OpenAQError(
                        f"GET {path} -> HTTP {resp.status_code} after {attempts} tries"
                    )
                delay = self._retry_delay(resp, attempt)
                log.warning(
                    "GET %s -> HTTP %s; retry in %.1fs", path, resp.status_code, delay
                )
                self._sleep(delay)
                continue
            if resp.is_error:
                raise OpenAQError(
                    f"GET {path} -> HTTP {resp.status_code}: {resp.text[:300]}"
                )

            self._respect_rate_limit(resp)
            return resp.json()
        raise AssertionError("unreachable")  # pragma: no cover

    def _paginate(
        self, path: str, params: Mapping[str, Any], page_size: int = MAX_PAGE_SIZE
    ) -> Iterator[dict[str, Any]]:
        page = 1
        while True:
            payload = self._get(path, {**params, "limit": page_size, "page": page})
            results = payload.get("results") or []
            yield from results
            if len(results) < page_size:
                return
            page += 1

    # -- stations ----------------------------------------------------------- #

    def find_stations(
        self,
        bbox: Sequence[float] = AMM_BBOX,
        *,
        providers_id: Sequence[int] | None = (SINAICA_PROVIDER_ID,),
        parameters: Iterable[str] | None = NOM_PARAMETERS,
        monitor_only: bool = True,
        active_within: timedelta | None = timedelta(days=30),
        now: datetime | None = None,
    ) -> list[Station]:
        """Active stations in ``bbox``, keeping only the requested parameters.

        OpenAQ lists the same physical station several times (old AirNow ids,
        retired SINAICA ids, the current SINAICA id). Filtering by provider and by
        ``datetime_last`` keeps only the location that is currently reporting.
        """
        if len(bbox) != 4:
            raise ValueError("bbox must be (min_lon, min_lat, max_lon, max_lat)")
        params: dict[str, Any] = {"bbox": ",".join(str(c) for c in bbox)}
        if providers_id:
            params["providers_id"] = ",".join(str(p) for p in providers_id)
        if monitor_only:
            params["monitor"] = "true"

        wanted = set(parameters) if parameters is not None else None
        cutoff = None
        if active_within is not None:
            cutoff = (now or datetime.now(timezone.utc)) - active_within
        allowed_providers = set(providers_id) if providers_id else None

        stations: list[Station] = []
        for loc in self._paginate("/locations", params):
            provider_id = (loc.get("provider") or {}).get("id")
            if allowed_providers and provider_id not in allowed_providers:
                continue
            if monitor_only and not loc.get("isMonitor", False):
                continue
            station = station_from_location(loc)
            if cutoff and (
                station.datetime_last is None or station.datetime_last < cutoff
            ):
                continue
            sensors = tuple(
                s for s in station.sensors if wanted is None or s.parameter in wanted
            )
            if not sensors:
                continue
            stations.append(station.model_copy(update={"sensors": sensors}))

        stations.sort(key=lambda s: s.location_id)
        log.info(
            "Found %d active stations with %d sensors",
            len(stations),
            sum(len(s.sensors) for s in stations),
        )
        return stations

    # -- measurements ------------------------------------------------------- #

    def iter_hours(
        self, sensor_id: int, window_start: datetime, window_end: datetime
    ) -> Iterator[dict[str, Any]]:
        """Raw hourly rows whose period ends in ``(window_start, window_end]``."""
        window_start, window_end = to_utc(window_start), to_utc(window_end)
        if window_start >= window_end:
            raise ValueError("window_start must be before window_end")
        params = {"datetime_from": _iso(window_start), "datetime_to": _iso(window_end)}
        for row in self._paginate(f"/sensors/{sensor_id}/hours", params):
            end = parse_ts(row["period"]["datetimeTo"]["utc"])
            if end is not None and window_start < end <= window_end:
                yield row

    def fetch_measurements(
        self,
        stations: Iterable[Station],
        window_start: datetime,
        window_end: datetime,
        *,
        ingested_at: datetime | None = None,
        skip_failed_sensors: bool = True,
    ) -> FetchResult:
        """Hourly measurements of every sensor of every station in the window.

        With ``skip_failed_sensors`` a sensor that keeps failing after retries is
        recorded in ``FetchResult.failed_sensors`` and the rest are still fetched,
        so one broken sensor cannot block the whole metro area. Auth errors are
        always raised.
        """
        window_start, window_end = to_utc(window_start), to_utc(window_end)
        ingested_at = to_utc(ingested_at or datetime.now(timezone.utc))
        result = FetchResult()
        for station in stations:
            for sensor in station.sensors:
                try:
                    rows = list(
                        self.iter_hours(sensor.sensor_id, window_start, window_end)
                    )
                except OpenAQAuthError:
                    raise
                except OpenAQError as exc:
                    if not skip_failed_sensors:
                        raise
                    log.error("Sensor %s failed: %s", sensor.sensor_id, exc)
                    result.failed_sensors[sensor.sensor_id] = str(exc)
                    continue
                result.measurements.extend(
                    to_measurement(row, station, sensor, ingested_at) for row in rows
                )
        return result
