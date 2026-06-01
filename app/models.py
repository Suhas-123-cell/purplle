from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class EventType(str, Enum):
    ENTRY = "ENTRY"
    EXIT = "EXIT"
    ZONE_ENTER = "ZONE_ENTER"
    ZONE_EXIT = "ZONE_EXIT"
    ZONE_DWELL = "ZONE_DWELL"
    BILLING_QUEUE_JOIN = "BILLING_QUEUE_JOIN"
    BILLING_QUEUE_ABANDON = "BILLING_QUEUE_ABANDON"
    REENTRY = "REENTRY"
    GROUP_ENTRY = "GROUP_ENTRY"


class EventMetadata(BaseModel):
    queue_depth: Optional[int] = Field(default=None, ge=0)
    sku_zone: Optional[str] = None
    session_seq: Optional[int] = Field(default=None, ge=0)
    review_required: bool = False
    review_flags: List[str] = Field(default_factory=list)
    confidence_bucket: Optional[str] = None
    confidence_reason: Optional[str] = None
    reentry_match_confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    staff_reason: Optional[str] = None

    model_config = {"extra": "allow"}


class Event(BaseModel):
    event_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="UUID v4 identifying this event uniquely",
    )
    store_id: str = Field(min_length=1)
    camera_id: str = Field(min_length=1)
    visitor_id: str = Field(min_length=1)
    event_type: EventType
    timestamp: datetime
    zone_id: Optional[str] = None
    dwell_ms: int = Field(default=0, ge=0)
    is_staff: bool = False
    confidence: float = Field(ge=0.0, le=1.0)
    metadata: EventMetadata = Field(default_factory=EventMetadata)

    @field_validator("event_id")
    @classmethod
    def validate_uuid(cls, v: str) -> str:
        try:
            uuid.UUID(v, version=4)
        except (ValueError, AttributeError):
            raise ValueError(f"event_id must be a valid UUID v4, got: {v!r}")
        return v

    @model_validator(mode="after")
    def zone_required_for_zone_events(self) -> "Event":
        zone_types = {
            EventType.ZONE_ENTER,
            EventType.ZONE_EXIT,
            EventType.ZONE_DWELL,
        }
        if self.event_type in zone_types and not self.zone_id:
            raise ValueError(f"zone_id is required for event_type {self.event_type}")
        return self


class IngestRequest(BaseModel):
    events: List[Event] = Field(min_length=1, max_length=500)


class IngestResponse(BaseModel):
    accepted: int
    rejected: int
    duplicate: int
    errors: List[Dict[str, Any]] = Field(default_factory=list)


class ZoneDwellStat(BaseModel):
    zone_id: str
    avg_dwell_ms: float
    visit_count: int


class StoreMetrics(BaseModel):
    store_id: str
    as_of: datetime
    unique_visitors: int
    conversion_rate: float = Field(ge=0.0, le=1.0)
    avg_dwell_per_zone: List[ZoneDwellStat]
    queue_depth: int
    abandonment_rate: float = Field(ge=0.0, le=1.0)


class FunnelStage(BaseModel):
    stage: str
    count: int
    drop_off_pct: float = Field(ge=0.0, le=100.0)


class FunnelData(BaseModel):
    store_id: str
    as_of: datetime
    date: str
    stages: List[FunnelStage]


class HeatmapCell(BaseModel):
    zone_id: str
    visit_count: int
    avg_dwell_ms: float
    intensity: float = Field(ge=0.0, le=100.0)
    data_confidence: bool = Field(
        description="False if fewer than 20 sessions contributed to this cell"
    )


class HeatmapData(BaseModel):
    store_id: str
    as_of: datetime
    date: str
    cells: List[HeatmapCell]


class AnomalyType(str, Enum):
    BILLING_QUEUE_SPIKE = "BILLING_QUEUE_SPIKE"
    CONVERSION_DROP = "CONVERSION_DROP"
    DEAD_ZONE = "DEAD_ZONE"
    STALE_FEED = "STALE_FEED"


class Severity(str, Enum):
    INFO = "INFO"
    WARN = "WARN"
    CRITICAL = "CRITICAL"


class Anomaly(BaseModel):
    anomaly_type: AnomalyType
    severity: Severity
    description: str
    suggested_action: str
    detected_at: datetime
    context: Dict[str, Any] = Field(default_factory=dict)


class AnomalyData(BaseModel):
    store_id: str
    as_of: datetime
    anomalies: List[Anomaly]


class CameraStatus(BaseModel):
    camera_id: str
    last_event_ts: Optional[datetime]
    is_stale: bool
    lag_seconds: Optional[float]


class HealthResponse(BaseModel):
    status: str
    db_status: str
    store_id: str
    last_event_ts: Optional[datetime]
    camera_statuses: List[CameraStatus]
    checked_at: datetime


class ErrorResponse(BaseModel):
    error: str
    detail: Optional[str] = None
    trace_id: Optional[str] = None
