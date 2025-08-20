# excel_event_schema.py
"""
Minimal, stable JSON "schema" for Excel row change events, declared in Python
via Pydantic models. Use the Event model to validate payloads and (optionally)
export a JSON Schema for registries or docs.

- Kafka key: str  (use the row_key)
- Kafka value: ExcelRowPayloadV1 (below)

If you later need CloudEvents or Avro, you can wrap/convert easily.
"""

from __future__ import annotations
from typing import Dict, Optional, Union, Literal
from datetime import datetime
from pydantic import BaseModel, Field

# Keep cell values simple & JSON-friendly
JsonCell = Union[str, int, float, bool, None]


class ExcelRowPayloadV1(BaseModel):
    """
    Simple JSON payload describing a single row change.
    Add/remove optional fields freely without breaking producers/consumers.
    """
    action: Literal["row_added", "row_updated", "row_deleted"] = Field(..., description="Type of change")
    file: str = Field(..., description="File name, e.g., Sales.xlsx")
    path: str = Field(..., description="Absolute or logical path to the file")
    sheet: str = Field(..., description="Worksheet name")
    row_key: str = Field(..., description="Stable identifier for the row (e.g., invoice no.). Also used as Kafka key")
    row_hash: str = Field(..., description="Hash of row values for change detection")
    observed_at: datetime = Field(..., description="When the watcher observed the change")
    file_mtime: datetime = Field(..., description="File modification time from the filesystem")
    row_number: Optional[int] = Field(None, description="1-based row number if known")
    row: Dict[str, JsonCell] = Field(..., description="Header -> cell value map")
    column_types: Optional[Dict[str, str]] = Field(
        None, description="Optional hints, e.g., {'amount': 'number', 'date': 'date'}"
    )


class ExcelRowEventV1(BaseModel):
    """
    Kafka message shape for convenience with your bus:
    - `key` is what goes to Kafka's message key (partitioning & per-key ordering).
    - `payload` is the JSON value produced to the topic.
    """
    key: str = Field(..., description="Kafka message key (recommend same as row_key)")
    payload: ExcelRowPayloadV1


# ---- Optional helpers --------------------------------------------------------

def event_json_schema() -> dict:
    """
    Export JSON Schema for the Kafka value if you ever want to register it.
    Works with Pydantic v2 (.model_json_schema) and v1 (.schema).
    """
    m = ExcelRowPayloadV1
    if hasattr(m, "model_json_schema"):
        return m.model_json_schema()  # pydantic v2
    return m.schema()                 # pydantic v1
