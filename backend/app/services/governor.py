from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from redis.asyncio import Redis
from sqlalchemy import desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models.entities import Trade


class GovernorService:
    def __init__(self, redis_client: Redis):
        self.redis = redis_client

    @staticmethod
    async def compute_hwm(redis_client: Redis, dataset_id: str | None, equity_start: float, equity_now: float) -> float:
        key = f"governor:hwm:{dataset_id or 'live'}"
        hwm = max(equity_start, equity_now)
        try:
            raw = await redis_client.get(key)
            if raw is not None:
                hwm = max(hwm, float(raw))
            await redis_client.set(key, str(hwm))
        except Exception:
            pass
        return hwm

    @staticmethod
    def _default_payload() -> dict:
        return {
            'eligible': True,
            'blockers': [],
            'stats': {
                'trades_today': 0,
                'daily_pnl_net': 0.0,
                'consecutive_losses': 0,
                'cooldown_remaining_sec': 0,
                'cooldown_remaining_minutes': 0.0,
                'global_dd_pct': 0.0,
                'daily_lock_active': False,
            },
            'config': {
                'max_trades_per_day': settings.gov_max_trades_per_day,
                'max_daily_loss_pct': settings.gov_max_daily_loss_pct,
                'max_global_dd_pct': settings.gov_max_global_dd_pct,
                'max_consecutive_losses': settings.gov_max_consecutive_losses,
                'max_consecutive_losses_scope': str(settings.gov_max_consec_losses_scope).lower(),
                'cooldown_minutes': settings.gov_cooldown_minutes,
                'daily_lock_on_breach': settings.gov_daily_lock_on_breach,
            },
            'flags': {
                'daily_anchor_supported': False,
            },
        }

    async def evaluate_entry(
        self,
        db: AsyncSession,
        now_ts: datetime | None,
        dataset_id: str | None,
        equity_start_day: float,
        global_dd_pct: float,
        replay_mode: bool,
    ) -> dict:
        payload = self._default_payload()
        payload['stats']['global_dd_pct'] = float(global_dd_pct)
        if not replay_mode:
            return payload
        now_ts = now_ts or datetime.now(timezone.utc)
        try:
            tz = ZoneInfo(settings.trading_day_tz)
        except Exception:
            tz = timezone.utc
        now_local = now_ts.astimezone(tz)
        day_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)

        opened_at_expr = func.coalesce(Trade.opened_at, Trade.created_at)
        closed_at_expr = func.coalesce(Trade.closed_at, Trade.created_at)

        trades_today = (
            await db.execute(
                select(func.count(Trade.id)).where(
                    opened_at_expr >= day_start,
                    opened_at_expr < day_end,
                )
            )
        ).scalar_one()
        payload['stats']['trades_today'] = int(trades_today or 0)

        daily_pnl = (
            await db.execute(
                select(func.coalesce(func.sum(func.coalesce(Trade.pnl_net, Trade.pnl, 0)), 0)).where(
                    Trade.status == 'CLOSED',
                    closed_at_expr >= day_start,
                    closed_at_expr < day_end,
                )
            )
        ).scalar_one()
        daily_pnl_net = float(daily_pnl or 0.0)
        payload['stats']['daily_pnl_net'] = daily_pnl_net

        consec_scope = str(settings.gov_max_consec_losses_scope).lower()
        consecutive_query = select(Trade.pnl_net, Trade.pnl).where(Trade.status == 'CLOSED')
        if consec_scope == 'daily':
            consecutive_query = consecutive_query.where(
                closed_at_expr >= day_start,
                closed_at_expr < day_end,
            )
        closed_rows = (
            await db.execute(
                consecutive_query
                .order_by(desc(closed_at_expr))
                .limit(max(settings.gov_max_consecutive_losses * 5, 20))
            )
        ).all()
        consecutive_losses = 0
        for pnl_net, pnl_legacy in closed_rows:
            value = float(pnl_net if pnl_net is not None else (pnl_legacy or 0.0))
            if value < 0:
                consecutive_losses += 1
            else:
                break
        payload['stats']['consecutive_losses'] = consecutive_losses

        last_closed_at = (
            await db.execute(
                select(closed_at_expr).where(Trade.status == 'CLOSED').order_by(desc(closed_at_expr)).limit(1)
            )
        ).scalar_one_or_none()
        cooldown_remaining_sec = 0
        if last_closed_at:
            elapsed = (now_ts - last_closed_at).total_seconds()
            cooldown_remaining_sec = max(0, int(settings.gov_cooldown_minutes * 60 - elapsed))
        payload['stats']['cooldown_remaining_sec'] = cooldown_remaining_sec
        payload['stats']['cooldown_remaining_minutes'] = round(cooldown_remaining_sec / 60.0, 2)

        blockers: list[dict] = []
        max_trades_hit = payload['stats']['trades_today'] >= settings.gov_max_trades_per_day
        if max_trades_hit:
            blockers.append(
                {
                    'name': 'max_trades_per_day',
                    'reason': 'daily trade count limit reached',
                    'threshold': settings.gov_max_trades_per_day,
                    'current': payload['stats']['trades_today'],
                    'detail': f"Reached daily trade cap for {day_start.date().isoformat()}",
                }
            )

        daily_loss_limit = -(equity_start_day * settings.gov_max_daily_loss_pct / 100.0)
        daily_loss_hit = daily_pnl_net <= daily_loss_limit
        if daily_loss_hit:
            blockers.append(
                {
                    'name': 'max_daily_loss_pct',
                    'reason': 'daily net loss limit reached',
                    'threshold': daily_loss_limit,
                    'current': daily_pnl_net,
                    'detail': 'Daily anchor uses equity_start (conservative mode)',
                }
            )

        if global_dd_pct >= settings.gov_max_global_dd_pct:
            blockers.append(
                {
                    'name': 'max_global_dd_pct',
                    'reason': 'global drawdown limit reached',
                    'threshold': settings.gov_max_global_dd_pct,
                    'current': global_dd_pct,
                    'detail': 'HWM vs equity_now gate',
                }
            )

        if consecutive_losses >= settings.gov_max_consecutive_losses:
            blockers.append(
                {
                    'name': 'max_consecutive_losses',
                    'reason': 'consecutive loss streak exceeded',
                    'threshold': settings.gov_max_consecutive_losses,
                    'current': consecutive_losses,
                    'detail': f"Closed-trade loss streak gate (scope={consec_scope})",
                }
            )

        if cooldown_remaining_sec > 0:
            blockers.append(
                {
                    'name': 'cooldown_minutes',
                    'reason': 'cooldown after close active',
                    'threshold': settings.gov_cooldown_minutes * 60,
                    'current': cooldown_remaining_sec,
                    'detail': 'Entry blocked until cooldown expires',
                }
            )

        if settings.gov_daily_lock_on_breach and (max_trades_hit or daily_loss_hit):
            payload['stats']['daily_lock_active'] = True
            blockers.append(
                {
                    'name': 'daily_lock',
                    'reason': 'daily lock active after breach',
                    'threshold': True,
                    'current': True,
                    'detail': f"Day key {day_start.date().isoformat()} locked",
                }
            )

        payload['blockers'] = blockers
        payload['eligible'] = len(blockers) == 0
        return payload
