from datetime import datetime
from typing import Any
from pydantic import BaseModel,Field




class NetworkSummaryResponse(BaseModel):
    total_activity: float
    active_grids: int
    peak_hour: int
    top_grid: int
    as_of: datetime



class GridActivityPoint(BaseModel):
    timestamp: datetime
    hour_of_day: int
    total_sms: float
    total_calls: float
    internet_activity: float
    total_activity: float


class GridActivityResponse(BaseModel):
    grid_id: int
    as_of: datetime
    start_time: datetime
    end_time: datetime
    data: list[GridActivityPoint]

class AlertRecord(BaseModel):
    grid_id: int
    timestamp: datetime

    alert_type: str
    severity: str

    current_activity: float
    baseline_activity: float | None

    reason: str


class AlertResponse(BaseModel):
    as_of: datetime
    data: list[AlertRecord]


class GridFeaturesResponse(BaseModel):
    grid_id: int

    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float

    feature_timestamp: datetime

    data_quality: str
    feature_freshness: str

class GridFeaturesResponse2(BaseModel):
    grid_id: int

    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float

    feature_timestamp: datetime

    trailing_median_24h: float
    hour_of_day: int    
    day_of_week: int


class RiskPredictionRequest(BaseModel):
    grid_id: int = Field(
        ...,
        ge=1,
        le=10000,
        description="Network grid identifier",
    )
    feature_timestamp: datetime = Field(
        ...,
        description="Timestamp of the ML feature vector",
    )
    avg_activity: float = Field(
        ...,
        ge=0,
    )

    activity_growth: float

    peak_ratio: float = Field(
        ...,
        ge=0,
    )

    variability: float = Field(
        ...,
        ge=0,
    )

    internet_share: float = Field(
        ...,
        ge=0,
        le=1,
    )

    trailing_median_24h: float = Field(
        ...,
        ge=0,
    )

    hour_of_day: int = Field(
        ...,
        ge=0,
        le=23,
    )

    day_of_week: int = Field(
        ...,
        ge=0,
        le=6,
    )
class RiskPredictionResponse(BaseModel):
    grid_id: int
    feature_timestamp: datetime

    risk_score: float
    risk_level: str

    model_version: str
    explanation_note: str


class PipelineTaskStatus(BaseModel):
    ingest: str
    validate: str
    route: str
    spark_process: str
    load_warehouse: str
    quality_check: str


class PipelineFreshness(BaseModel):
    latest_data_timestamp: datetime
    age_hours: float
    indicator: str


class PipelineStatusResponse(BaseModel):
    run_id: str
    timestamp: datetime
    status: str

    tasks: PipelineTaskStatus

    rows_in: int
    rows_rejected: int
    nulls_handled: int | None
    rows_published: int

    current_as_of: datetime

    freshness: PipelineFreshness

    healthy: bool
    reasons: list[str]

class GridLocationResponse(BaseModel):
    grid_id: int
    centroid_latitude: float | None
    centroid_longitude: float | None
    geometry_ref: str | None

class AnomalyResponse(BaseModel):
    grid_id: int
    timestamp: datetime
    hour_of_day: int
    total_activity: float
    historical_baseline: float | None
    baseline_count: int | None
    deviation: float | None
    anomaly_score: float | None
    anomaly_direction: str | None
    anomaly_flag: bool
    reason: str | None
    recommended_action: str | None

class ClaudeAssistantRequest(BaseModel):
    message: str = Field(
        ...,
        min_length=1,
        description="Natural-language NOC question",
    )
    session_id: str | None = Field(
        default=None,
        description="Conversation session identifier",
    )


class ClaudeAssistantResponse(BaseModel):
    model: str
    answer: str
    tool_calls: list[dict[str, Any]]
    session_id: str