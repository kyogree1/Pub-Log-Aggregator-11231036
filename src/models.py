from __future__ import annotations
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import Any, Dict, List, Union
from datetime import datetime

class Event(BaseModel):
    topic: str = Field(..., min_length=1, description="Nama topik, misalnya 'orders'")
    event_id: str = Field(..., min_length=1, description="ID unik event, misalnya 'ev-001'")
    timestamp: str = Field(..., description="Waktu event dalam format ISO8601, contoh: 2025-10-21T00:00:00Z")
    source: str = Field(..., min_length=1, description="Sumber event, misalnya 'manual-test'")
    payload: Dict[str, Any] = Field(..., description="Data isi event, harus berupa objek JSON")

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str):
        try:
            # pastikan format ISO8601 (contoh: 2025-10-21T00:00:00Z)
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception:
            raise ValueError("❌ Format timestamp tidak valid. Gunakan format ISO8601, contoh: 2025-10-21T00:00:00Z")
        return v

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, v: dict):
        if not isinstance(v, dict):
            raise ValueError("❌ Payload harus berupa objek JSON, bukan string atau array.")
        if len(v) == 0:
            raise ValueError("⚠️ Payload tidak boleh kosong — minimal harus ada satu key.")
        return v


class PublishBatch(BaseModel):
    events: List[Event] = Field(..., min_length=1, description="Daftar event untuk batch publish")

PublishBody = Union[Event, PublishBatch, List[Event]]

class Stats(BaseModel):
    received: int
    unique_processed: int
    duplicate_dropped: int
    topics: List[str]
    uptime_seconds: float
