"""Shared data contracts for the ingestion layer.

``Measurement`` is the standard record every source (OpenAQ, SINAICA, the Flask
push API and the simulator) is translated into before it is published. It must
stay field-for-field aligned with ``schemas/measurement.avsc``; a test enforces it.
"""

from __future__ import annotations

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class Measurement(BaseModel):
    """One pollutant reading from one sensor, as published to ``aq.measurements.raw``.

    ``measured_at`` is the *end* of the averaging period (hour-ending convention,
    the same one OpenAQ uses to filter its ``/hours`` endpoint and the one Mexican
    networks use to report hourly averages). ``value`` is nullable on purpose:
    invalid readings must reach silver so they can be routed to ``aq.dlq`` with a
    reason, instead of being silently dropped at ingestion.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    station_id: str = Field(min_length=1)
    sensor_id: str = Field(min_length=1)
    parameter: str = Field(min_length=1)
    value: float | None
    unit: str = Field(min_length=1)
    measured_at: AwareDatetime
    source: str = Field(min_length=1)
    period_start: AwareDatetime | None = None
    coverage_pct: float | None = None
    ingested_at: AwareDatetime | None = None
