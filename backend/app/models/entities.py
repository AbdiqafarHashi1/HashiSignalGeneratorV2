import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, Integer, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base
from app.models.base import TimestampUUIDMixin


class Account(TimestampUUIDMixin, Base):
    __tablename__ = 'accounts'

    name: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    equity: Mapped[Decimal] = mapped_column(Numeric(20, 8), default=0)
    leverage: Mapped[float] = mapped_column(Numeric(8, 2), default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class Trade(TimestampUUIDMixin, Base):
    __tablename__ = 'trades'

    account_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey('accounts.id'), index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(8), index=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    entry_price: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    exit_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    stop_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    tp1_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    tp2_price: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    time_stop_bars: Mapped[int | None] = mapped_column(Integer, nullable=True)
    strategy_name: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    setup_name: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    regime_at_entry: Mapped[str | None] = mapped_column(String(32), nullable=True)
    score_at_entry: Mapped[Decimal | None] = mapped_column(Numeric(12, 6), nullable=True)
    opened_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    close_reason: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    fee_entry: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    fee_exit: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    fees_total: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    pnl_gross: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    pnl_net: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    leverage: Mapped[Decimal | None] = mapped_column(Numeric(12, 4), nullable=True)
    notional: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    pnl: Mapped[Decimal | None] = mapped_column(Numeric(20, 8), nullable=True)
    status: Mapped[str] = mapped_column(String(24), default='OPEN', index=True)


class Execution(TimestampUUIDMixin, Base):
    __tablename__ = 'executions'

    trade_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), ForeignKey('trades.id'), index=True)
    provider: Mapped[str] = mapped_column(String(32), index=True)
    status: Mapped[str] = mapped_column(String(24), index=True)
    payload: Mapped[dict] = mapped_column(JSONB, default=dict)


class Position(TimestampUUIDMixin, Base):
    __tablename__ = 'positions'

    symbol: Mapped[str] = mapped_column(String(32), index=True)
    side: Mapped[str] = mapped_column(String(8), index=True)
    quantity: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    average_price: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    unrealized_pnl: Mapped[Decimal] = mapped_column(Numeric(20, 8), default=0)
    is_open: Mapped[bool] = mapped_column(Boolean, default=True, index=True)


class Candle(TimestampUUIDMixin, Base):
    __tablename__ = 'candles'

    symbol: Mapped[str] = mapped_column(String(32), index=True)
    timeframe: Mapped[str] = mapped_column(String(16), index=True)
    ts: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    open: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    high: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    low: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    close: Mapped[Decimal] = mapped_column(Numeric(20, 8))
    volume: Mapped[Decimal] = mapped_column(Numeric(20, 8))


class ReplaySession(TimestampUUIDMixin, Base):
    __tablename__ = 'replay_sessions'

    name: Mapped[str] = mapped_column(String(128), index=True)
    csv_path: Mapped[str] = mapped_column(Text)
    speed_multiplier: Mapped[float] = mapped_column(Numeric(8, 2), default=1)
    cursor: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String(24), default='STOPPED', index=True)


class RiskEvent(TimestampUUIDMixin, Base):
    __tablename__ = 'risk_events'

    level: Mapped[str] = mapped_column(String(16), index=True)
    message: Mapped[str] = mapped_column(Text)
    context: Mapped[dict] = mapped_column(JSONB, default=dict)


class ReplayDataset(TimestampUUIDMixin, Base):
    __tablename__ = 'replay_datasets'

    filename: Mapped[str] = mapped_column(String(255))
    stored_path: Mapped[str] = mapped_column(Text, unique=True)
    symbol: Mapped[str | None] = mapped_column(String(32), nullable=True)
    timeframe: Mapped[str | None] = mapped_column(String(16), nullable=True)
    rows_count: Mapped[int] = mapped_column(BigInteger, default=0)
    start_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    end_ts: Mapped[int | None] = mapped_column(BigInteger, nullable=True)


class DecisionEvent(TimestampUUIDMixin, Base):
    __tablename__ = 'decision_events'

    ts: Mapped[int] = mapped_column(BigInteger, index=True)
    symbol: Mapped[str] = mapped_column(String(32), index=True)
    regime: Mapped[str] = mapped_column(String(64), default='unknown')
    signal_score: Mapped[float] = mapped_column(Numeric(12, 6), default=0)
    decision: Mapped[str] = mapped_column(String(24), index=True)
    blockers: Mapped[list] = mapped_column(JSONB, default=list)
    rationale: Mapped[str] = mapped_column(Text, default='')
    risk_state_snapshot: Mapped[dict] = mapped_column(JSONB, default=dict)


class TelegramLog(TimestampUUIDMixin, Base):
    __tablename__ = 'telegram_logs'

    message_type: Mapped[str] = mapped_column(String(32), index=True)
    body: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(24), index=True)


Index('idx_trades_created_at_desc', Trade.created_at.desc())
Index('idx_executions_created_at_desc', Execution.created_at.desc())
Index('idx_positions_created_at_desc', Position.created_at.desc())
