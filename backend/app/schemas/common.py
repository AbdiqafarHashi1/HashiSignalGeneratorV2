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
    csv_path: str
    speed_multiplier: float = Field(default=1.0, gt=0)
    resume: bool = False


class TradeRead(BaseModel):
    id: UUID
    symbol: str
    side: str
    quantity: float
    entry_price: float
    exit_price: float | None
    pnl: float | None
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
