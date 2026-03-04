from __future__ import annotations

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from app.config import settings
from app.providers.csv_replay import CSVReplayProvider
from app.profiles import get_profile_manager
from app.strategies.trend_v1 import TrendPullbackStrategyV1


@dataclass
class SimTrade:
    id: str
    symbol: str
    side: str
    quantity: float
    entry_price: float
    stop_price: float | None
    tp1_price: float | None
    tp2_price: float | None
    time_stop_bars: int
    opened_at: str | None
    score_at_entry: float | None
    regime_at_entry: str | None
    strategy_name: str | None
    strategy_profile: str | None
    notional: float
    fee_entry: float
    tp1_be_armed: bool = False
    entry_index: int = 0


class InstantBacktestService:
    def __init__(self) -> None:
        self._runs: dict[str, dict] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._profile_manager = get_profile_manager(settings)

    async def run(self, payload: dict) -> str:
        enabled = bool(getattr(settings, 'instant_backtest_enabled', False))
        if not enabled:
            raise ValueError('Instant backtest is disabled. Set INSTANT_BACKTEST_ENABLED=true to enable it.')
        csv_path = str(payload.get('dataset_path') or '').strip()
        if not csv_path:
            raise ValueError('dataset_path is required')
        if not Path(csv_path).exists():
            raise ValueError(f'dataset_path not found: {csv_path}')

        run_id = str(uuid.uuid4())
        self._runs[run_id] = {'state': 'running', 'progress_pct': 0, 'message': 'queued', 'result': None}
        task = asyncio.create_task(self._execute(run_id=run_id, payload=payload))
        self._tasks[run_id] = task
        return run_id

    async def status(self, run_id: str) -> dict:
        run = self._runs.get(run_id)
        if run is None:
            return {'state': 'error', 'progress_pct': 0, 'message': 'run_id not found'}
        return {'state': run['state'], 'progress_pct': run['progress_pct'], 'message': run['message']}

    async def result(self, run_id: str) -> dict:
        run = self._runs.get(run_id)
        if run is None:
            raise ValueError('run_id not found')
        if run['state'] != 'done':
            raise ValueError(f'run is not done (state={run["state"]})')
        return run['result']

    async def _execute(self, run_id: str, payload: dict) -> None:
        run = self._runs[run_id]
        try:
            result = await self._simulate(payload=payload, run=run)
            run.update({'state': 'done', 'progress_pct': 100, 'message': 'completed', 'result': result})
        except Exception as exc:
            run.update({'state': 'error', 'message': str(exc), 'progress_pct': run.get('progress_pct', 0)})

    async def _simulate(self, payload: dict, run: dict) -> dict:
        csv_path = str(payload.get('dataset_path'))
        provider = CSVReplayProvider(csv_path)
        rows = provider.load_rows()
        if not rows:
            raise ValueError('dataset contains no rows')

        profile = self._profile_manager.apply(str(payload.get('profile') or settings.active_profile or 'TREND_STABLE'))
        symbols_filter = {str(s).upper() for s in (payload.get('symbols') or []) if str(s).strip()}
        sample_every = max(1, int(payload.get('equity_sample_every') or 25))

        start_bound = self._to_dt(payload.get('start'))
        end_bound = self._to_dt(payload.get('end'))

        strategy = TrendPullbackStrategyV1()
        history_by_symbol: dict[str, list[dict]] = {}
        open_trade_by_symbol: dict[str, SimTrade] = {}
        closed: list[dict] = []
        total_fees = 0.0
        net_cum = 0.0
        equity_curve: list[dict] = []

        old_profile = settings.active_profile
        settings.active_profile = profile
        try:
            total_rows = len(rows)
            for idx, row in enumerate(rows):
                tick = provider.normalize_row(row)
                symbol = str(tick.get('symbol') or settings.default_symbol).upper()
                if symbols_filter and symbol not in symbols_filter:
                    continue
                ts = self._to_dt(tick.get('timestamp'))
                if start_bound and (not ts or ts < start_bound):
                    continue
                if end_bound and ts and ts > end_bound:
                    continue

                history = history_by_symbol.setdefault(symbol, [])
                history.append({
                    'timestamp': tick.get('timestamp'),
                    'open': float(tick.get('open', 0) or 0),
                    'high': float(tick.get('high', 0) or 0),
                    'low': float(tick.get('low', 0) or 0),
                    'close': float(tick.get('close', 0) or 0),
                    'volume': float(tick.get('volume', 0) or 0),
                })
                plan = strategy.build_plan(history, payload.get('timeframe'), idx)
                price_close = float(tick.get('close', tick.get('price', 0)) or 0)
                price_high = float(tick.get('high', price_close) or price_close)
                price_low = float(tick.get('low', price_close) or price_close)

                trade = open_trade_by_symbol.get(symbol)
                if trade:
                    exited, event = self._manage_open_trade(trade, plan, price_high, price_low, price_close, idx, ts)
                    if event and event.get('kind') == 'risk_update':
                        pass
                    if exited:
                        total_fees += float(event['fees_total'])
                        net_cum += float(event['pnl_net'])
                        closed.append(event)
                        open_trade_by_symbol.pop(symbol, None)
                elif plan.side in {'BUY', 'SELL'} and plan.entry_price is not None:
                    qty = float(plan.qty)
                    fee_entry = abs(float(plan.entry_price) * qty * float(settings.taker_fee_rate))
                    total_fees += fee_entry
                    open_trade_by_symbol[symbol] = SimTrade(
                        id=str(uuid.uuid4()),
                        symbol=symbol,
                        side=str(plan.side),
                        quantity=qty,
                        entry_price=float(plan.entry_price),
                        stop_price=float(plan.stop_price) if plan.stop_price is not None else None,
                        tp1_price=float(plan.tp1_price) if plan.tp1_price is not None else None,
                        tp2_price=float(plan.tp2_price) if plan.tp2_price is not None else None,
                        time_stop_bars=int(plan.time_stop_bars),
                        opened_at=ts.isoformat() if ts else None,
                        score_at_entry=float(plan.score_total),
                        regime_at_entry=str(plan.regime),
                        strategy_name=str(plan.strategy_name),
                        strategy_profile=profile,
                        notional=abs(float(plan.entry_price) * qty),
                        fee_entry=fee_entry,
                        entry_index=idx,
                    )

                if idx % sample_every == 0:
                    equity_curve.append({'x': idx, 'ts': ts.isoformat() if ts else None, 'equity': float(settings.equity_start + net_cum)})
                if idx % 100 == 0:
                    run['progress_pct'] = min(99, int((idx / max(1, total_rows)) * 100))
                    run['message'] = f'processed {idx}/{total_rows} bars'
                    await asyncio.sleep(0)
        finally:
            settings.active_profile = old_profile

        max_dd = self._max_drawdown_pct([p['equity'] for p in equity_curve])
        wins = [t for t in closed if t['pnl_net'] > 0]
        losses = [t for t in closed if t['pnl_net'] < 0]
        trades_count = len(closed)
        gross_profit = sum(t['pnl_net'] for t in wins)
        gross_loss = abs(sum(t['pnl_net'] for t in losses))
        avg_win = (gross_profit / len(wins)) if wins else 0.0
        avg_loss = (sum(t['pnl_net'] for t in losses) / len(losses)) if losses else 0.0
        win_rate = (len(wins) / trades_count * 100.0) if trades_count else 0.0
        profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (999.0 if gross_profit > 0 else 0.0)
        expectancy = (sum(t['pnl_net'] for t in closed) / trades_count) if trades_count else 0.0

        return {
            'run_meta': {
                'dataset_path': csv_path,
                'profile': profile,
                'symbols': sorted(symbols_filter) if symbols_filter else [],
                'start': payload.get('start'),
                'end': payload.get('end'),
            },
            'summary': {
                'win_rate': win_rate,
                'profit_factor': profit_factor,
                'expectancy': expectancy,
                'avg_win': avg_win,
                'avg_loss': avg_loss,
                'max_dd': max_dd,
                'max_dd_definition': 'Peak-to-trough decline of sampled equity curve, percent.',
                'fees_total': total_fees,
                'trades_count': trades_count,
            },
            'trades': closed,
            'equity_curve': equity_curve,
        }

    def _manage_open_trade(self, trade: SimTrade, plan, high: float, low: float, close: float, idx: int, ts: datetime | None):
        stop = trade.stop_price or 0.0
        tp1 = trade.tp1_price or 0.0
        tp2 = trade.tp2_price or 0.0
        growth_trade = str(trade.strategy_profile or '').upper() == 'GROWTH_HUNTER'
        if trade.side == 'BUY':
            stop_hit = stop > 0 and low <= stop
            tp1_hit = (not growth_trade) and tp1 > 0 and high >= tp1
            tp2_hit = tp2 > 0 and high >= tp2
        else:
            stop_hit = stop > 0 and high >= stop
            tp1_hit = (not growth_trade) and tp1 > 0 and low <= tp1
            tp2_hit = tp2 > 0 and low <= tp2

        if stop_hit:
            return True, self._close_trade(trade, stop, 'sl_close', ts)
        if tp2_hit:
            if (not growth_trade) and settings.tp1_be_enabled and (not trade.tp1_be_armed) and tp1 > 0:
                self._arm_tp1_to_be(trade)
            return True, self._close_trade(trade, tp2, 'tp_close', ts)
        if (not growth_trade) and tp1_hit and settings.tp1_be_enabled and (not trade.tp1_be_armed):
            self._arm_tp1_to_be(trade)
            return False, {'kind': 'risk_update'}

        if (idx - trade.entry_index) >= max(1, int(trade.time_stop_bars or settings.time_stop_bars)):
            return True, self._close_trade(trade, close, 'time_close', ts)
        opposite = (trade.side == 'BUY' and plan.side == 'SELL') or (trade.side == 'SELL' and plan.side == 'BUY')
        if opposite and float(plan.score_total) >= float(settings.score_min):
            return True, self._close_trade(trade, close, 'signal_close', ts)
        return False, None

    def _arm_tp1_to_be(self, trade: SimTrade) -> None:
        old_sl = float(trade.stop_price or 0.0)
        offset = float(settings.tp1_be_offset)
        new_sl = trade.entry_price + offset if trade.side == 'BUY' else trade.entry_price - offset
        if trade.side == 'BUY' and new_sl < old_sl:
            return
        if trade.side == 'SELL' and new_sl > old_sl:
            return
        trade.stop_price = new_sl
        trade.tp1_be_armed = True

    def _close_trade(self, trade: SimTrade, exit_price: float, reason: str, ts: datetime | None) -> dict:
        pnl_gross = (exit_price - trade.entry_price) * trade.quantity if trade.side == 'BUY' else (trade.entry_price - exit_price) * trade.quantity
        fee_exit = abs(exit_price * trade.quantity * float(settings.taker_fee_rate))
        fees_total = trade.fee_entry + fee_exit
        pnl_net = pnl_gross - fees_total
        return {
            'id': trade.id,
            'symbol': trade.symbol,
            'side': trade.side,
            'quantity': trade.quantity,
            'entry_price': trade.entry_price,
            'exit_price': exit_price,
            'stop_price': trade.stop_price,
            'tp1_price': trade.tp1_price,
            'tp2_price': trade.tp2_price,
            'notional': trade.notional,
            'fees': fees_total,
            'fees_total': fees_total,
            'pnl_gross': pnl_gross,
            'pnl_net': pnl_net,
            'pnl': pnl_net,
            'result': 'WIN' if pnl_net > 0 else ('LOSS' if pnl_net < 0 else 'BE'),
            'reason': reason,
            'close_reason': reason,
            'opened_at': trade.opened_at,
            'closed_at': ts.isoformat() if ts else None,
            'status': 'CLOSED',
            'score_at_entry': trade.score_at_entry,
            'regime_at_entry': trade.regime_at_entry,
        }

    @staticmethod
    def _to_dt(value) -> datetime | None:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        with contextlib.suppress(Exception):
            return datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        return None

    @staticmethod
    def _max_drawdown_pct(values: list[float]) -> float:
        if not values:
            return 0.0
        hwm = values[0]
        worst = 0.0
        for v in values:
            hwm = max(hwm, v)
            if hwm <= 0:
                continue
            dd = (hwm - v) / hwm * 100.0
            worst = max(worst, dd)
        return worst
