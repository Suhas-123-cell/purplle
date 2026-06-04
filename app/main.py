from __future__ import annotations

import logging
import os
import re
import time
import uuid
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import Depends, FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.middleware.base import BaseHTTPMiddleware

try:
    from .anomalies import compute_anomalies
    from .database import get_session, init_db
    from .funnel import compute_funnel
    from .health import compute_health
    from .heatmap import compute_heatmap
    from .ingestion import CANONICAL_STORE_ID, ingest_events, load_pos_from_csv, normalize_store_id
    from .metrics import compute_store_metrics
    from .models import (
        AnomalyData,
        ErrorResponse,
        FunnelData,
        HeatmapData,
        HealthResponse,
        IngestRequest,
        IngestResponse,
        StoreMetrics,
    )
except ImportError:  # pragma: no cover - used when uvicorn imports main.py directly
    from anomalies import compute_anomalies
    from database import get_session, init_db
    from funnel import compute_funnel
    from health import compute_health
    from heatmap import compute_heatmap
    from ingestion import CANONICAL_STORE_ID, ingest_events, load_pos_from_csv, normalize_store_id
    from metrics import compute_store_metrics
    from models import (
        AnomalyData,
        ErrorResponse,
        FunnelData,
        HeatmapData,
        HealthResponse,
        IngestRequest,
        IngestResponse,
        StoreMetrics,
    )

POS_CSV_PATH = os.getenv("POS_CSV_PATH", "/data/pos_transactions.csv")

_INGEST_LIMIT = int(os.getenv("INGEST_RATE_LIMIT_PER_MIN", "60"))
_INGEST_WINDOW = 60.0  # seconds
_ingest_hits: dict[str, list[float]] = defaultdict(list)


def _allow_ingest(ip: str) -> bool:
    now = time.monotonic()
    bucket = _ingest_hits[ip]
    cutoff = now - _INGEST_WINDOW
    while bucket and bucket[0] < cutoff:
        bucket.pop(0)
    if len(bucket) >= _INGEST_LIMIT:
        return False
    bucket.append(now)
    return True

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": %(message)s}',
)
logger = logging.getLogger("store_intelligence")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    try:
        await init_db()
    except Exception:
        logger.error('{"event": "init_db_failed"}', exc_info=True)
        raise
    try:
        loaded, skipped = await load_pos_from_csv(POS_CSV_PATH)
    except Exception:
        logger.error('{"event": "load_pos_failed", "path": "%s"}', POS_CSV_PATH, exc_info=True)
        raise
    logger.info(
        '{"event": "startup", "pos_loaded": %d, "pos_skipped": %d}', loaded, skipped
    )
    yield
    logger.info('{"event": "shutdown"}')


app = FastAPI(
    title="Store Intelligence API",
    description="Real-time store analytics for Purplle Brigade Road Bangalore",
    version="1.0.0",
    lifespan=lifespan,
)

ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "http://localhost:3000").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=False,
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type"],
)

_MAX_BODY_SIZE = 2 * 1024 * 1024  # 2MB


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        content_length = request.headers.get("Content-Length")
        if content_length is not None:
            try:
                if int(content_length) > _MAX_BODY_SIZE:
                    return JSONResponse(
                        {"error": "payload_too_large", "detail": "Request body exceeds 2MB limit."},
                        status_code=413,
                    )
            except ValueError:
                pass
        return await call_next(request)


app.add_middleware(BodySizeLimitMiddleware)


@app.middleware("http")
async def structured_logging_middleware(request: Request, call_next) -> Response:
    trace_id = request.headers.get("X-Trace-Id", str(uuid.uuid4()))
    request.state.trace_id = trace_id
    start_ts = time.monotonic()

    response = await call_next(request)

    latency_ms = round((time.monotonic() - start_ts) * 1000, 2)
    store_id = request.path_params.get("store_id", "")

    logger.info(
        '{"trace_id": "%s", "store_id": "%s", "endpoint": "%s %s", '
        '"latency_ms": %s, "status_code": %d}',
        trace_id,
        store_id,
        request.method,
        request.url.path,
        latency_ms,
        response.status_code,
    )
    response.headers["X-Trace-Id"] = trace_id
    return response


_STORE_ID_RE = re.compile(r'^[\w\-]+$')

def _validate_store(store_id: str) -> str:
    if not store_id or len(store_id) > 64 or not _STORE_ID_RE.match(store_id):
        raise HTTPException(status_code=400, detail="Invalid store_id")
    return normalize_store_id(store_id)


def _db_error_response(trace_id: str) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content=ErrorResponse(
            error="service_unavailable",
            detail="Database is temporarily unavailable. Please retry.",
            trace_id=trace_id,
        ).model_dump(mode="json"),
    )


@app.post(
    "/events/ingest",
    response_model=IngestResponse,
    summary="Batch ingest camera events (idempotent)",
    status_code=200,
)
async def ingest_endpoint(
    request: Request,
    payload: IngestRequest,
) -> IngestResponse:
    client_ip = request.client.host if request.client else "127.0.0.1"
    if not _allow_ingest(client_ip):
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded — max 60 ingest batches per minute.",
            headers={"Retry-After": "60"},
        )
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    try:
        result = await ingest_events(payload.events)
        logger.info(
            '{"trace_id": "%s", "event": "ingest", "accepted": %d, "rejected": %d, "duplicate": %d}',
            trace_id,
            result.accepted,
            result.rejected,
            result.duplicate,
        )
        return result
    except SQLAlchemyError:
        logger.error('{"trace_id": "%s", "event": "db_error"}', trace_id, exc_info=True)
        return _db_error_response(trace_id)


@app.get(
    "/stores/{store_id}/metrics",
    response_model=StoreMetrics,
    summary="Real-time store metrics",
)
async def store_metrics(
    request: Request,
    store_id: str,
    session: AsyncSession = Depends(get_session),
) -> StoreMetrics:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    canonical = _validate_store(store_id)
    try:
        return await compute_store_metrics(session, canonical)
    except SQLAlchemyError:
        logger.error('{"trace_id": "%s", "event": "db_error"}', trace_id, exc_info=True)
        return _db_error_response(trace_id)


@app.get(
    "/stores/{store_id}/funnel",
    response_model=FunnelData,
    summary="Conversion funnel (session-based, no double-counting)",
)
async def store_funnel(
    request: Request,
    store_id: str,
    session: AsyncSession = Depends(get_session),
) -> FunnelData:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    canonical = _validate_store(store_id)
    try:
        return await compute_funnel(session, canonical)
    except SQLAlchemyError:
        logger.error('{"trace_id": "%s", "event": "db_error"}', trace_id, exc_info=True)
        return _db_error_response(trace_id)


@app.get(
    "/stores/{store_id}/heatmap",
    response_model=HeatmapData,
    summary="Zone visit frequency heatmap (intensity normalized 0-100)",
)
async def store_heatmap(
    request: Request,
    store_id: str,
    session: AsyncSession = Depends(get_session),
) -> HeatmapData:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    canonical = _validate_store(store_id)
    try:
        return await compute_heatmap(session, canonical)
    except SQLAlchemyError:
        logger.error('{"trace_id": "%s", "event": "db_error"}', trace_id, exc_info=True)
        return _db_error_response(trace_id)


@app.get(
    "/stores/{store_id}/anomalies",
    response_model=AnomalyData,
    summary="Detected anomalies: queue spike, conversion drop, dead zone, stale feed",
)
async def store_anomalies(
    request: Request,
    store_id: str,
    session: AsyncSession = Depends(get_session),
) -> AnomalyData:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    canonical = _validate_store(store_id)
    try:
        return await compute_anomalies(session, canonical)
    except SQLAlchemyError:
        logger.error('{"trace_id": "%s", "event": "db_error"}', trace_id, exc_info=True)
        return _db_error_response(trace_id)


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Service health including camera feed staleness",
)
async def health_check(
    request: Request,
    store_id: str = CANONICAL_STORE_ID,
    session: AsyncSession = Depends(get_session),
) -> HealthResponse:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    canonical = _validate_store(store_id)
    try:
        return await compute_health(session, canonical)
    except SQLAlchemyError:
        logger.error('{"trace_id": "%s", "event": "db_error"}', trace_id, exc_info=True)
        return _db_error_response(trace_id)


@app.exception_handler(ValidationError)
async def validation_exception_handler(request: Request, exc: ValidationError) -> JSONResponse:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            error="validation_error",
            detail=f"{exc.error_count()} validation error(s). Check request schema.",
            trace_id=trace_id,
        ).model_dump(mode="json"),
    )


@app.exception_handler(RequestValidationError)
async def request_validation_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    return JSONResponse(
        status_code=422,
        content=ErrorResponse(
            error="validation_error",
            detail=f"{len(exc.errors())} validation error(s). Check request schema.",
            trace_id=trace_id,
        ).model_dump(mode="json"),
    )


@app.exception_handler(Exception)
async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    trace_id = getattr(request.state, "trace_id", str(uuid.uuid4()))
    logger.error(
        '{"trace_id": "%s", "event": "unhandled_error", "type": "%s"}',
        trace_id,
        type(exc).__name__,
        exc_info=True,
    )
    return JSONResponse(
        status_code=500,
        content=ErrorResponse(
            error="internal_server_error",
            detail="An unexpected error occurred.",
            trace_id=trace_id,
        ).model_dump(mode="json"),
    )
