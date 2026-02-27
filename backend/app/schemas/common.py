from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, Field


class PaginatedQuery(BaseModel):
    limit: int = Field(default=50, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class EngineStatus(BaseModel):
    running: bool
    mode: str
    tick: int
    last_event_at: datetime | None


class ReplayStartRequest(BaseModel):
    csv_path: str | None = None
    dataset_id: UUID | None = None
    filename: str | None = None
    speed: int | None = Field(default=None, gt=0)
    speed_multiplier: float = Field(default=1.0, gt=0)
    resume: bool = False


class TradeRead(BaseModel):
    id: UUID
    symbol: str
    side: str
    quantity: float
    entry_price: float
    exit_price: float | None
    stop_price: float | None = None
    tp1_price: float | None = None
    tp2_price: float | None = None
    time_stop_bars: int | None = None
    strategy_name: str | None = None
    setup_name: str | None = None
    regime_at_entry: str | None = None
    score_at_entry: float | None = None
    opened_at: datetime | None = None
    closed_at: datetime | None = None
    close_reason: str | None = None
    fee_entry: float | None = None
    fee_exit: float | None = None
    fees_total: float | None = None
    pnl_gross: float | None = None
    pnl_net: float | None = None
    leverage: float | None = None
    notional: float | None = None
    pnl: float | None
    fees: float | None = None
    status: str
    created_at: datetime


class ExecutionRead(BaseModel):
    id: UUID
    provider: str
    status: str
    payload: dict
    created_at: datetime


class PositionRead(BaseModel):
    id: UUID
    symbol: str
    side: str
    quantity: float
    average_price: float
    unrealized_pnl: float
    is_open: bool
    created_at: datetime
