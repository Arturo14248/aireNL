"""Where ingestion writes its records.

Kafka is not deployed yet, so the ingestion DAGs land records as JSON Lines in a
local landing zone. When the ``core`` profile (Kafka + Apicurio) exists, add a
Kafka sink next to this one and switch the DAG; the client does not change.
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Iterable
from pathlib import Path

from pydantic import BaseModel


def write_jsonl(records: Iterable[BaseModel], path: Path) -> int:
    """Atomically (re)write ``path`` with one JSON object per line.

    Writing to a temp file and renaming means a crashed task never leaves a
    half-written file, and rerunning the same window overwrites instead of
    appending, which keeps the task idempotent.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            for record in records:
                fh.write(record.model_dump_json())
                fh.write("\n")
                count += 1
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return count
