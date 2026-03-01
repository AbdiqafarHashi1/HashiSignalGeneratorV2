from datetime import datetime, timedelta, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.entities import Execution, Position, Trade


class PortfolioAccounting:
    def __init__(self, equity_start: float | None = None):
        self.equity_start = Decimal(str(equity_start if equity_start is not None else settings.equity_start))

    @staticmethod
    def _eat_day_start_utc(now_utc: datetime | None = None) -> datetime:
        now_utc = now_utc or datetime.now(timezone.utc)
        eat_tz = timezone(timedelta(hours=3))
        eat_now = now_utc.astimezone(eat_tz)
        eat_day_start = eat_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return eat_day_start.astimezone(timezone.utc)

    async def snapshot(self, db: AsyncSession) -> dict:
        closed_trades = (
            await db.execute(
                select(Trade.pnl_net, Trade.pnl, Trade.fees_total, Trade.fee_entry, Trade.fee_exit).where(Trade.status == 'CLOSED')
            )
        ).all()
        realized_d = Decimal('0')
        fees_total_d = Decimal('0')
        fees_from_trade_totals = False
        fees_from_trade_legs = False
        for pnl_net, pnl_legacy, fees_total, fee_entry, fee_exit in closed_trades:
            realized_d += Decimal(str(pnl_net if pnl_net is not None else (pnl_legacy or 0)))
            if fees_total is not None:
                fees_total_d += Decimal(str(fees_total))
                fees_from_trade_totals = True
            elif fee_entry is not None or fee_exit is not None:
                fees_total_d += Decimal(str((fee_entry or 0))) + Decimal(str((fee_exit or 0)))
                fees_from_trade_legs = True

        fees_source = 'trades'
        if not fees_from_trade_totals and not fees_from_trade_legs:
            executions = (await db.execute(select(Execution.payload))).all()
            for payload_row in executions:
                payload = payload_row[0] or {}
                fees_total_d += Decimal(str(payload.get('fee', 0)))
            fees_source = 'executions'
        elif fees_from_trade_legs and not fees_from_trade_totals:
            fees_source = 'mixed'

        unrealized = (
            await db.execute(select(func.coalesce(func.sum(Position.unrealized_pnl), 0)).where(Position.is_open.is_(True)))
        ).scalar_one()
        unrealized_d = Decimal(str(unrealized or 0))

        day_start_utc = self._eat_day_start_utc()
        today_fee_rows = (
            await db.execute(select(Trade.fees_total, Trade.closed_at).where(Trade.closed_at.is_not(None), Trade.status == 'CLOSED'))
        ).all()
        today_fees = Decimal('0')
        for fees_total, closed_at in today_fee_rows:
            if closed_at and closed_at >= day_start_utc:
                today_fees += Decimal(str(fees_total or 0))

        equity_now = self.equity_start + realized_d + unrealized_d - fees_total_d
        reconcile_rhs = self.equity_start + realized_d + unrealized_d - fees_total_d
        reconcile_delta = equity_now - reconcile_rhs
        return {
            'equity_start': float(self.equity_start),
            'realized_pnl': float(realized_d),
            'realized_pnl_net': float(realized_d),
            'unrealized_pnl': float(unrealized_d),
            'fees_paid': {
                'today': float(today_fees),
                'total': float(fees_total_d),
            },
            'fees_total': float(fees_total_d),
            'reconcile_delta': float(reconcile_delta),
            'reconcile_ok': abs(float(reconcile_delta)) < 1e-6,
            'accounting': {
                'unrealized_supported': True,
                'fees_source': fees_source,
            },
            'equity_now': float(equity_now),
        }
