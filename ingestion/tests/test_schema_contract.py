"""The Pydantic model and the Avro schema must describe the same record."""

from __future__ import annotations

import json
from pathlib import Path

from ingestion.models import Measurement

AVSC = Path(__file__).resolve().parents[2] / "schemas" / "measurement.avsc"


def _is_nullable(avro_type) -> bool:
    return isinstance(avro_type, list) and "null" in avro_type


def test_avro_and_pydantic_fields_match_in_order():
    schema = json.loads(AVSC.read_text())
    avro_fields = [f["name"] for f in schema["fields"]]
    assert avro_fields == list(Measurement.model_fields)


def test_nullability_matches():
    schema = json.loads(AVSC.read_text())
    for f in schema["fields"]:
        py_field = Measurement.model_fields[f["name"]]
        py_nullable = py_field.annotation is not None and "None" in str(
            py_field.annotation
        )
        assert _is_nullable(f["type"]) == py_nullable, f["name"]


def test_doc_example_payload_still_validates():
    """The push example in docs/architecture.md section 5.2.2 must stay valid."""
    Measurement.model_validate(
        {
            "station_id": "nl-centro-01",
            "sensor_id": "pm25-01",
            "parameter": "pm25",
            "value": 48.0,
            "unit": "ug/m3",
            "measured_at": "2027-01-15T14:00:00-06:00",
            "source": "simulator",
        }
    )
