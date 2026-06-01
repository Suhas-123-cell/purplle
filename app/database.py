import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, mapped_column, Mapped
from sqlalchemy import String, Integer, Float, Boolean, Text, DateTime, BigInteger, Index
from datetime import datetime
from typing import Optional

DB_PATH = os.getenv("DB_PATH", "/data/store_intelligence.db")
DATABASE_URL = f"sqlite+aiosqlite:///{DB_PATH}"

engine = create_async_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False},
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


class EventRow(Base):
    __tablename__ = "events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    store_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    camera_id: Mapped[str] = mapped_column(String(64), nullable=False)
    visitor_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    zone_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    dwell_ms: Mapped[int] = mapped_column(BigInteger, default=0)
    is_staff: Mapped[bool] = mapped_column(Boolean, default=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    queue_depth: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    sku_zone: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    session_seq: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    ingested_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index("ix_events_store_ts", "store_id", "timestamp"),
        Index("ix_events_store_type_ts", "store_id", "event_type", "timestamp"),
        Index("ix_events_camera_ts", "camera_id", "timestamp"),
    )


class VisitorSession(Base):
    __tablename__ = "visitor_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    store_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    visitor_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    session_start: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    session_end: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    entered_zone: Mapped[bool] = mapped_column(Boolean, default=False)
    entered_billing_queue: Mapped[bool] = mapped_column(Boolean, default=False)
    completed_purchase: Mapped[bool] = mapped_column(Boolean, default=False)
    is_staff: Mapped[bool] = mapped_column(Boolean, default=False)

    __table_args__ = (
        Index("ix_sessions_store_visitor", "store_id", "visitor_id"),
        Index("ix_sessions_store_start", "store_id", "session_start"),
    )


class POSTransaction(Base):
    __tablename__ = "pos_transactions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    order_id: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    order_date: Mapped[str] = mapped_column(String(16), nullable=False)
    order_time: Mapped[str] = mapped_column(String(16), nullable=False)
    store_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    store_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    customer_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    customer_number: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    sku: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    product_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    brand_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    dep_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    sub_category: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    qty: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    gmv: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    nmv: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    total_amount: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    salesperson_id: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    salesperson_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    transaction_ts: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)

    __table_args__ = (
        Index("ix_pos_store_ts", "store_id", "transaction_ts"),
    )


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_session() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
