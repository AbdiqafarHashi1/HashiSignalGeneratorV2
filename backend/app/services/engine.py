from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID
from zoneinfo import ZoneInfo

from redis.asyncio import Redis
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import settings
from app.execution.providers import BybitExecution
from app.models.entities import DecisionEvent, Execution, Position, Trade
from app.providers.market_data import BybitProvider
from app.replay.replay_engine import ReplayEngine
from app.risk.manager import RiskManager
from app.services.accounting import PortfolioAccounting
from app.services.governor import GovernorService
from app.services.safety import KillSwitchMode, SafetyService
from app.strategies.base import MomentumStrategy
from app.strategies.trend_v1 import TradePlan, TrendPullbackStrategyV1


class EngineService:
    def __init__(self, redis_client: Redis, session_factory: async_sessionmaker | None = None):
        if session_factory is None:
            from app.db.session import SessionLocal

            session_factory = SessionLocal
        self.redis = redis_client
        self.session_factory = session_factory
        self.running = False
        self.mode = settings.engine_mode
        self.tick = 0
        self.task: asyncio.Task | None = None
        self.last_event_at: datetime | None = None
        self.market_provider = BybitProvider()
        self.execution_provider = BybitExecution()
        self.strategy = MomentumStrategy()
        self.strategy_v1 = TrendPullbackStrategyV1()
        self.risk = RiskManager(leverage=settings.leverage)
        self.replay: ReplayEngine | None = None
        self.replay_dataset_id: str | None = None
        self.replay_symbol: str = settings.default_symbol
        self.replay_timeframe: str | None = None
        self._open_trade_id_by_symbol: dict[str, UUID] = {}
        self._open_bar_by_symbol: dict[str, int] = {}
        self._tp1_done_by_trade: dict[UUID, bool] = {}
        self._history_by_symbol: dict[str, list[dict]] = {}
        self.governor = GovernorService(redis_client=self.redis)
        self.safety = SafetyService()
        self.active_profile: str = str(settings.strategy_profile).upper()
        self.last_pre_trade_decision: dict = {
            'allowed': True,
            'reasonCode': None,
            'reasonDetail': None,
            'metrics': {},
        }
        try:
            self._trading_tz = ZoneInfo(settings.trading_day_tz)
        except Exception:
            self._trading_tz = timezone.utc
        self.last_day_key: str | None = None
        self.day_rollover_in_effect: bool = False
        self.daily_state: dict[str, float | int] = {
            'daily_consecutive_losses': 0,
            'daily_realized_pnl': 0.0,
            'daily_fees': 0.0,
            'daily_trade_count': 0,
        }

    async def start(self, mode: str = 'live') -> dict:
        if self.running:
            return self.status()
        self.mode = mode
        self.running = True
        self.task = asyncio.create_task(self._loop())
        return self.status()

    async def set_profile(self, profile: str) -> dict:
        allowed = {'TREND_STABLE', 'SCALPER_STABLE'}
        normalized = str(profile).upper()
        if normalized not in allowed:
            raise ValueError(f'Unsupported profile: {profile}')
        self.active_profile = normalized
        try:
            await self.redis.set('engine:active_profile', normalized)
        except Exception:
            pass
        return {'active_profile': self.active_profile}

    def safety_status(self) -> dict:
        return self.safety.status()

    def safety_arm(self, mode: str) -> dict:
        return self.safety.arm(mode)

    async def safety_trip(self, mode: str, reason: str, evidence: dict | None = None) -> dict:
        normalized = KillSwitchMode.SOFT if str(mode).lower() == 'soft' else KillSwitchMode.HARD
        return await self._trip_kill_switch(mode=normalized, reason=reason, evidence=evidence or {})

    def safety_incidents(self) -> list[dict]:
        return self.safety.list_incidents()

    def safety_incident(self, incident_id: str) -> dict | None:
        return self.safety.get_incident(incident_id)

    async def stop(self) -> dict:
        self.running = False
        if self.replay:
            await self.replay.stop()
        if self.task:
            await asyncio.wait([self.task], timeout=1)
        return self.status()

    async def start_replay(
        self,
        csv_path: str,
        speed_multiplier: float = 1.0,
        resume: bool = False,
        dataset_id: str | None = None,
        dataset_symbol: str | None = None,
        dataset_timeframe: str | None = None,
    ) -> dict:
        cursor = self.replay.cursor if (self.replay and resume) else 0
        self.replay_symbol = dataset_symbol or settings.default_symbol
        self.replay_timeframe = dataset_timeframe
        self.replay = ReplayEngine(
            csv_path=csv_path,
            speed_multiplier=speed_multiplier,
            resume_cursor=cursor,
            dataset_symbol=self.replay_symbol,
            dataset_timeframe=self.replay_timeframe,
        )
        self.replay_dataset_id = dataset_id
        self._history_by_symbol.clear()
        await self.replay.start()
        return await self.start(mode='replay')

    async def stop_replay(self) -> dict:
        if self.replay:
            await self.replay.stop()
        self._open_trade_id_by_symbol.clear()
        self._open_bar_by_symbol.clear()
        self._tp1_done_by_trade.clear()
        self._history_by_symbol.clear()
        return await self.stop()

    async def pause_replay(self) -> dict:
        if self.replay:
            await self.replay.pause()
        return self.replay_status()

    async def resume_replay(self) -> dict:
        if self.replay:
            await self.replay.resume()
        return self.replay_status()

    async def step_replay(self) -> dict:
        tick = await self._next_tick(force_step=True)
        if tick:
            await self._handle_replay_tick(tick=tick)
        return self.replay_status()

    async def manual_close_trade(self, trade_id: UUID) -> dict:
        if self.mode != 'replay' or not self.replay:
            return {'ok': False, 'detail': 'manual close is replay-only'}
        pointer = max(0, self.replay.cursor - 1)
        if pointer >= len(self.replay.rows):
            return {'ok': False, 'detail': 'replay pointer out of range'}
        tick = self.replay.provider.normalize_row(self.replay.rows[pointer], default_symbol=self.replay_symbol)
        ts_ms = self._tick_timestamp_ms(tick)
        ts_dt = self._tick_timestamp_dt(tick)
        price = Decimal(str(tick.get('close') or tick.get('price') or 0))
        symbol = str(tick.get('symbol') or self.replay_symbol)
        async with self.session_factory() as db:
            trade = await db.get(Trade, trade_id)
            if not trade or trade.status != 'OPEN':
                return {'ok': False, 'detail': 'open trade not found'}
            position = (
                await db.execute(select(Position).where(Position.symbol == trade.symbol, Position.is_open.is_(True)).limit(1))
            ).scalar_one_or_none()
            if not position:
                return {'ok': False, 'detail': 'open position not found'}
            await self._final_close(
                db=db,
                trade=trade,
                position=position,
                exit_price=price,
                exit_ts_ms=ts_ms,
                exit_ts=ts_dt,
                reason='manual_close',
                symbol=symbol,
                replay_clock=int(tick.get('replay_clock') or pointer),
            )
            await db.commit()
        return {'ok': True, 'trade_id': str(trade_id)}

    def replay_status(self) -> dict:
        if not self.replay:
            return {
                'dataset_id': self.replay_dataset_id,
                'running': False,
                'pointer_index': 0,
                'current_ts': None,
                'speed': 1.0,
                'last_error': None,
            }
        status = self.replay.status()
        return {
            'dataset_id': self.replay_dataset_id,
            'running': status['running'] and not status['paused'],
            'pointer_index': status['pointer_index'],
            'current_ts': status['current_ts'],
            'speed': status['speed'],
            'last_error': status['last_error'],
            'symbol': status.get('symbol') or self.replay_symbol,
            'timeframe': status.get('timeframe') or self.replay_timeframe,
        }

    async def _loop(self) -> None:
        while self.running:
            try:
                tick = await self._next_tick()
                self.tick += 1
                self.last_event_at = datetime.now(timezone.utc)
                await self._check_day_rollover(ts=self.last_event_at)
                if tick and self.mode == 'replay':
                    await self._handle_replay_tick(tick=tick)
                elif tick and self.risk.can_trade():
                    signal = await self.strategy.generate_signal(tick)
                    if signal:
                        allowed, safety_reason = self.safety.execution_guard(action='entry', reduce_only=False)
                        if allowed:
                            await self.execution_provider.execute(signal)
                        else:
                            self.safety.record_event(
                                'BLOCKED',
                                {
                                    'reason': 'Entry blocked by kill switch',
                                    'reason_code': safety_reason,
                                    'symbol': signal.get('symbol') if isinstance(signal, dict) else None,
                                },
                            )
                await self._run_reconciler()
                self.safety.record_cycle_ok()
                runtime = self.safety.evaluate_runtime(has_open_positions=bool(self._open_trade_id_by_symbol))
                if runtime and runtime.trip:
                    await self._trip_kill_switch(
                        mode=runtime.mode,
                        reason=str(runtime.reason),
                        evidence=runtime.evidence,
                    )
            except Exception as exc:
                self.safety.record_cycle_error(str(exc))
                await self._run_reconciler()
                runtime = self.safety.evaluate_runtime(has_open_positions=bool(self._open_trade_id_by_symbol))
                if runtime and runtime.trip:
                    await self._trip_kill_switch(
                        mode=runtime.mode,
                        reason=str(runtime.reason),
                        evidence=runtime.evidence,
                    )
            await self._publish_state()
            await asyncio.sleep(0.5)

    async def _next_tick(self, force_step: bool = False) -> dict | None:
        if self.mode == 'replay' and self.replay:
            if force_step:
                return await self.replay.step()
            return await self.replay.next_tick()
        return await self.market_provider.get_tick('BTCUSDT')

    async def _handle_replay_tick(self, tick: dict) -> None:
        symbol = tick.get('symbol') or self.replay_symbol or settings.default_symbol
        tick['symbol'] = symbol
        ts_ms = self._tick_timestamp_ms(tick)
        ts_dt = self._tick_timestamp_dt(tick)
        replay_clock = int(tick.get('replay_clock') or 0)
        self.safety.record_event(
            'TICK',
            {
                'symbol': symbol,
                'ts': ts_ms,
                'replay_clock': replay_clock,
            },
        )
        price_close = Decimal(str(tick.get('close', tick.get('price', 0)) or 0))
        price_high = Decimal(str(tick.get('high', tick.get('close', tick.get('price', 0))) or 0))
        price_low = Decimal(str(tick.get('low', tick.get('close', tick.get('price', 0))) or 0))

        self._history_by_symbol.setdefault(symbol, []).append(
            {
                'timestamp': tick.get('timestamp'),
                'open': float(tick.get('open', 0) or 0),
                'high': float(tick.get('high', 0) or 0),
                'low': float(tick.get('low', 0) or 0),
                'close': float(tick.get('close', 0) or 0),
                'volume': float(tick.get('volume', 0) or 0),
            }
        )
        history = self._history_by_symbol[symbol]
        plan = self.strategy_v1.build_plan(history, self.replay_timeframe)

        async with self.session_factory() as db:
            await self._check_day_rollover(ts=ts_dt, db=db, symbol=symbol, ts_ms=ts_ms)
            position = (
                await db.execute(select(Position).where(Position.symbol == symbol, Position.is_open.is_(True)).limit(1))
            ).scalar_one_or_none()
            trade = await self._get_open_trade_for_symbol(db=db, symbol=symbol)
            blockers: list[dict] = []
            decision = plan.decision
            rationale = ', '.join(plan.reasons) if plan.reasons else 'strategy_evaluated'

            if position and trade:
                position.unrealized_pnl = self._calc_unrealized(position=position, mark_price=price_close)
                await self._evaluate_open_trade(
                    db=db,
                    trade=trade,
                    position=position,
                    high=price_high,
                    low=price_low,
                    close=price_close,
                    ts_ms=ts_ms,
                    ts_dt=ts_dt,
                    replay_clock=replay_clock,
                    plan=plan,
                )
                decision = 'hold'
            elif plan.side in ('BUY', 'SELL') and self.risk.can_trade():
                allowed_guard, guard_reason = self.safety.execution_guard(action='entry', reduce_only=False)
                if not allowed_guard:
                    decision = 'BLOCKED'
                    blockers = [
                        {
                            'name': 'kill_switch',
                            'reason': guard_reason,
                            'threshold': self.safety.effective_mode().value,
                            'current': self.safety.effective_mode().value,
                            'detail': f'Kill switch mode is {self.safety.effective_mode().value}',
                        }
                    ]
                    self.last_pre_trade_decision = {
                        'allowed': False,
                        'reasonCode': guard_reason,
                        'reasonDetail': 'Entry blocked by kill switch',
                        'metrics': self._pretrade_metrics_from_governor(None),
                    }
                    await self._emit_blocked_event(
                        db=db,
                        ts_ms=ts_ms,
                        symbol=symbol,
                        side=plan.side,
                        qty=Decimal(str(plan.qty)),
                        price=price_close,
                        blockers=blockers,
                    )
                    await self._record_decision_event(
                        db=db,
                        ts_ms=ts_ms,
                        symbol=symbol,
                        decision='BLOCKED',
                        plan=plan,
                        blockers=blockers,
                        rationale='Entry blocked by kill switch',
                    )
                    await db.commit()
                    return
                accounting = await PortfolioAccounting().snapshot(db)
                hwm = await self.governor.compute_hwm(
                    redis_client=self.redis,
                    dataset_id=self.replay_dataset_id,
                    equity_start=float(accounting['equity_start']),
                    equity_now=float(accounting['equity_now']),
                )
                global_dd_pct = ((hwm - float(accounting['equity_now'])) / hwm * 100) if hwm > 0 else 0.0
                gov = await self.governor.evaluate_entry(
                    db=db,
                    now_ts=ts_dt,
                    dataset_id=self.replay_dataset_id,
                    equity_start_day=float(accounting['equity_start']),
                    global_dd_pct=global_dd_pct,
                    replay_mode=True,
                )
                self.safety.set_governor_status(gov)
                self.last_pre_trade_decision = self._build_pretrade_decision(gov=gov)
                if gov['eligible']:
                    await self._open_position(
                        db=db,
                        symbol=symbol,
                        entry_price=Decimal(str(plan.entry_price or price_close)),
                        entry_ts_ms=ts_ms,
                        entry_ts=ts_dt,
                        replay_clock=replay_clock,
                        side=plan.side,
                        plan=plan,
                    )
                else:
                    decision = 'BLOCKED'
                    blockers = gov['blockers']
                    await self._emit_blocked_event(
                        db=db,
                        ts_ms=ts_ms,
                        symbol=symbol,
                        side=plan.side,
                        qty=Decimal(str(plan.qty)),
                        price=price_close,
                        blockers=gov['blockers'],
                    )
            else:
                decision = 'hold'

            await self._record_decision_event(
                db=db,
                ts_ms=ts_ms,
                symbol=symbol,
                decision=decision.upper() if decision != 'hold' else 'HOLD',
                plan=plan,
                blockers=blockers,
                rationale=rationale,
            )
            await db.commit()

    async def _get_open_trade_for_symbol(self, db: AsyncSession, symbol: str) -> Trade | None:
        trade = (
            await db.execute(select(Trade).where(Trade.symbol == symbol, Trade.status == 'OPEN').order_by(desc(Trade.created_at)).limit(1))
        ).scalar_one_or_none()
        if trade:
            self._open_trade_id_by_symbol[symbol] = trade.id
            self._tp1_done_by_trade.setdefault(trade.id, False)
        return trade

    async def _open_position(
        self,
        db: AsyncSession,
        symbol: str,
        entry_price: Decimal,
        entry_ts_ms: int,
        entry_ts: datetime,
        replay_clock: int,
        side: str,
        plan: TradePlan,
    ) -> None:
        qty = Decimal(str(plan.qty))
        fee_rate = Decimal(str(settings.taker_fee_rate))
        fee = abs(entry_price * qty * fee_rate)
        notional = abs(entry_price * qty)
        stop_price = Decimal(str(plan.stop_price)) if plan.stop_price is not None else None
        tp1_price = Decimal(str(plan.tp1_price)) if plan.tp1_price is not None else None
        tp2_price = Decimal(str(plan.tp2_price)) if plan.tp2_price is not None else None

        position = Position(
            symbol=symbol,
            side=side,
            quantity=qty,
            average_price=entry_price,
            unrealized_pnl=Decimal('0'),
            is_open=True,
        )
        db.add(position)
        trade = Trade(
            symbol=symbol,
            side=side,
            quantity=qty,
            entry_price=entry_price,
            exit_price=None,
            stop_price=stop_price,
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            time_stop_bars=int(plan.time_stop_bars),
            strategy_name=plan.strategy_name,
            setup_name=plan.setup_name,
            regime_at_entry=plan.regime,
            score_at_entry=Decimal(str(plan.score_total)),
            opened_at=entry_ts,
            pnl=Decimal('0'),
            pnl_gross=Decimal('0'),
            pnl_net=Decimal('0'),
            fee_entry=fee,
            fee_exit=Decimal('0'),
            fees_total=fee,
            leverage=Decimal(str(plan.leverage)),
            notional=notional,
            status='OPEN',
        )
        db.add(trade)
        await db.flush()

        db.add(
            Execution(
                trade_id=trade.id,
                provider='replay',
                status='FILLED',
                payload={
                    'symbol': symbol,
                    'side': side,
                    'qty': float(qty),
                    'price': float(entry_price),
                    'ts': entry_ts_ms,
                    'reason': 'entry',
                    'mode': 'replay',
                    'fee': float(fee),
                },
            )
        )

        self._open_trade_id_by_symbol[symbol] = trade.id
        self._open_bar_by_symbol[symbol] = replay_clock
        self._tp1_done_by_trade[trade.id] = False
        self.daily_state['daily_trade_count'] = int(self.daily_state.get('daily_trade_count', 0)) + 1
        await self._emit_trade_event(
            db=db,
            ts_ms=entry_ts_ms,
            symbol=symbol,
            decision='ENTRY',
            reason='entry',
            trade=trade,
            position=position,
            side=side,
            qty=qty,
            price=entry_price,
            fee_delta=fee,
            extra={
                'stop_price': float(stop_price) if stop_price is not None else None,
                'tp1_price': float(tp1_price) if tp1_price is not None else None,
                'tp2_price': float(tp2_price) if tp2_price is not None else None,
                'time_stop_bars': int(plan.time_stop_bars),
                'strategy_name': plan.strategy_name,
                'setup_name': plan.setup_name,
                'regime': plan.regime,
                'score_total': float(plan.score_total),
                'score_components': plan.score_components,
                'reasons': plan.reasons,
            },
        )

    async def _evaluate_open_trade(
        self,
        db: AsyncSession,
        trade: Trade,
        position: Position,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        ts_ms: int,
        ts_dt: datetime,
        replay_clock: int,
        plan: TradePlan,
    ) -> None:
        stop = Decimal(str(trade.stop_price or 0))
        tp1 = Decimal(str(trade.tp1_price or 0))
        tp2 = Decimal(str(trade.tp2_price or 0))
        tp1_done = bool(self._tp1_done_by_trade.get(trade.id, False))
        bars_held = max(0, replay_clock - self._open_bar_by_symbol.get(trade.symbol, replay_clock))
        time_stop_limit = int(trade.time_stop_bars or settings.time_stop_bars)

        stop_hit = False
        tp1_hit = False
        tp2_hit = False
        if trade.side == 'BUY':
            stop_hit = low <= stop if stop > 0 else False
            tp1_hit = high >= tp1 if tp1 > 0 else False
            tp2_hit = high >= tp2 if tp2 > 0 else False
        else:
            stop_hit = high >= stop if stop > 0 else False
            tp1_hit = low <= tp1 if tp1 > 0 else False
            tp2_hit = low <= tp2 if tp2 > 0 else False

        if stop_hit:
            await self._final_close(
                db=db,
                trade=trade,
                position=position,
                exit_price=stop,
                exit_ts_ms=ts_ms,
                exit_ts=ts_dt,
                reason='sl_close',
                symbol=trade.symbol,
                replay_clock=replay_clock,
            )
            return

        if tp2_hit:
            if not tp1_done and tp1 > 0:
                await self._partial_close(
                    db=db,
                    trade=trade,
                    position=position,
                    close_price=tp1,
                    close_ts_ms=ts_ms,
                    reason='tp1_close',
                    portion=settings.partial_pct,
                )
                tp1_done = True
                self._tp1_done_by_trade[trade.id] = True
            await self._final_close(
                db=db,
                trade=trade,
                position=position,
                exit_price=tp2,
                exit_ts_ms=ts_ms,
                exit_ts=ts_dt,
                reason='tp_close',
                symbol=trade.symbol,
                replay_clock=replay_clock,
            )
            return

        if tp1_hit and not tp1_done:
            await self._partial_close(
                db=db,
                trade=trade,
                position=position,
                close_price=tp1,
                close_ts_ms=ts_ms,
                reason='tp1_close',
                portion=settings.partial_pct,
            )
            self._tp1_done_by_trade[trade.id] = True

        if bars_held >= time_stop_limit:
            await self._final_close(
                db=db,
                trade=trade,
                position=position,
                exit_price=close,
                exit_ts_ms=ts_ms,
                exit_ts=ts_dt,
                reason='time_close',
                symbol=trade.symbol,
                replay_clock=replay_clock,
            )
            return

        opposite = (trade.side == 'BUY' and plan.side == 'SELL') or (trade.side == 'SELL' and plan.side == 'BUY')
        if opposite and plan.score_total >= settings.score_min:
            await self._final_close(
                db=db,
                trade=trade,
                position=position,
                exit_price=close,
                exit_ts_ms=ts_ms,
                exit_ts=ts_dt,
                reason='signal_close',
                symbol=trade.symbol,
                replay_clock=replay_clock,
            )

    async def _partial_close(
        self,
        db: AsyncSession,
        trade: Trade,
        position: Position,
        close_price: Decimal,
        close_ts_ms: int,
        reason: str,
        portion: float,
    ) -> None:
        current_qty = Decimal(str(position.quantity))
        if current_qty <= 0:
            return
        close_qty = current_qty * Decimal(str(max(0.0, min(0.99, portion))))
        if close_qty <= 0:
            return
        if close_qty >= current_qty:
            close_qty = current_qty
        fee_rate = Decimal(str(settings.taker_fee_rate))
        fee_delta = abs(close_qty * close_price * fee_rate)
        gross_delta = self._gross_delta(trade_side=trade.side, entry=Decimal(str(trade.entry_price)), exit_price=close_price, qty=close_qty)

        db.add(
            Execution(
                trade_id=trade.id,
                provider='replay',
                status='FILLED',
                payload={
                    'symbol': trade.symbol,
                    'side': 'SELL' if trade.side == 'BUY' else 'BUY',
                    'qty': float(close_qty),
                    'price': float(close_price),
                    'ts': close_ts_ms,
                    'reason': reason,
                    'mode': 'replay',
                    'reduceOnly': True,
                    'fee': float(fee_delta),
                },
            )
        )

        remaining_qty = current_qty - close_qty
        position.quantity = remaining_qty
        position.unrealized_pnl = self._calc_unrealized(position=position, mark_price=close_price) if remaining_qty > 0 else Decimal('0')

        fee_exit = Decimal(str(trade.fee_exit or 0)) + fee_delta
        pnl_gross = Decimal(str(trade.pnl_gross or 0)) + gross_delta
        fee_entry = Decimal(str(trade.fee_entry or 0))
        fees_total = fee_entry + fee_exit
        pnl_net = pnl_gross - fees_total
        trade.fee_exit = fee_exit
        trade.fees_total = fees_total
        trade.pnl_gross = pnl_gross
        trade.pnl_net = pnl_net
        trade.pnl = pnl_net
        self.daily_state['daily_realized_pnl'] = float(self.daily_state.get('daily_realized_pnl', 0.0)) + float(gross_delta - fee_delta)
        self.daily_state['daily_fees'] = float(self.daily_state.get('daily_fees', 0.0)) + float(fee_delta)

        await self._emit_trade_event(
            db=db,
            ts_ms=close_ts_ms,
            symbol=trade.symbol,
            decision='PARTIAL',
            reason=reason,
            trade=trade,
            position=position,
            side='SELL' if trade.side == 'BUY' else 'BUY',
            qty=close_qty,
            price=close_price,
            fee_delta=fee_delta,
            pnl_delta=gross_delta - fee_delta,
            extra={'remaining_qty': float(remaining_qty)},
        )

    async def _final_close(
        self,
        db: AsyncSession,
        trade: Trade,
        position: Position,
        exit_price: Decimal,
        exit_ts_ms: int,
        exit_ts: datetime,
        reason: str,
        symbol: str,
        replay_clock: int,
    ) -> None:
        remaining_qty = Decimal(str(position.quantity))
        fee_rate = Decimal(str(settings.taker_fee_rate))
        fee_delta = abs(remaining_qty * exit_price * fee_rate)
        gross_delta = self._gross_delta(
            trade_side=trade.side,
            entry=Decimal(str(trade.entry_price)),
            exit_price=exit_price,
            qty=remaining_qty,
        )
        fee_exit = Decimal(str(trade.fee_exit or 0)) + fee_delta
        fee_entry = Decimal(str(trade.fee_entry or 0))
        fees_total = fee_entry + fee_exit
        pnl_gross = Decimal(str(trade.pnl_gross or 0)) + gross_delta
        pnl_net = pnl_gross - fees_total
        bars_held = max(0, replay_clock - self._open_bar_by_symbol.get(symbol, replay_clock))

        db.add(
            Execution(
                trade_id=trade.id,
                provider='replay',
                status='FILLED',
                payload={
                    'symbol': symbol,
                    'side': 'SELL' if trade.side == 'BUY' else 'BUY',
                    'qty': float(remaining_qty),
                    'price': float(exit_price),
                    'ts': exit_ts_ms,
                    'reason': reason,
                    'mode': 'replay',
                    'reduceOnly': True,
                    'fee': float(fee_delta),
                },
            )
        )

        position.is_open = False
        position.quantity = Decimal('0')
        position.unrealized_pnl = Decimal('0')

        trade.exit_price = exit_price
        trade.closed_at = exit_ts
        trade.close_reason = reason
        trade.fee_exit = fee_exit
        trade.fees_total = fees_total
        trade.pnl_gross = pnl_gross
        trade.pnl_net = pnl_net
        trade.pnl = pnl_net
        trade.status = 'CLOSED'
        self.daily_state['daily_realized_pnl'] = float(self.daily_state.get('daily_realized_pnl', 0.0)) + float(gross_delta - fee_delta)
        self.daily_state['daily_fees'] = float(self.daily_state.get('daily_fees', 0.0)) + float(fee_delta)
        if float(pnl_net) < 0:
            self.daily_state['daily_consecutive_losses'] = int(self.daily_state.get('daily_consecutive_losses', 0)) + 1
        else:
            self.daily_state['daily_consecutive_losses'] = 0

        self._open_trade_id_by_symbol.pop(symbol, None)
        self._open_bar_by_symbol.pop(symbol, None)
        self._tp1_done_by_trade.pop(trade.id, None)

        await self._emit_trade_event(
            db=db,
            ts_ms=exit_ts_ms,
            symbol=symbol,
            decision='EXIT',
            reason=reason,
            trade=trade,
            position=position,
            side='SELL' if trade.side == 'BUY' else 'BUY',
            qty=remaining_qty,
            price=exit_price,
            fee_delta=fee_delta,
            pnl_delta=gross_delta - fee_delta,
            extra={
                'bars_held': bars_held,
                'result': 'WIN' if pnl_net > 0 else ('LOSS' if pnl_net < 0 else 'FLAT'),
                'pnl_net': float(pnl_net),
                'fees_total': float(fees_total),
            },
        )

    async def _emit_trade_event(
        self,
        db: AsyncSession,
        ts_ms: int,
        symbol: str,
        decision: str,
        reason: str,
        trade: Trade,
        position: Position,
        side: str,
        qty: Decimal,
        price: Decimal,
        fee_delta: Decimal,
        pnl_delta: Decimal | None = None,
        extra: dict | None = None,
    ) -> None:
        payload = {
            'event_type': decision,
            'reason': reason,
            'trade_id': str(trade.id),
            'position_id': str(position.id),
            'side': side,
            'qty': float(qty),
            'price': float(price),
            'fee_delta': float(fee_delta),
            'stop_price': float(trade.stop_price) if trade.stop_price is not None else None,
            'tp1_price': float(trade.tp1_price) if trade.tp1_price is not None else None,
            'tp2_price': float(trade.tp2_price) if trade.tp2_price is not None else None,
            'time_stop_bars': int(trade.time_stop_bars or settings.time_stop_bars),
            'strategy_name': trade.strategy_name,
            'setup_name': trade.setup_name,
            'score_at_entry': float(trade.score_at_entry) if trade.score_at_entry is not None else None,
            'reduceOnly': decision in {'EXIT', 'PARTIAL'},
        }
        if pnl_delta is not None:
            payload['pnl_delta'] = float(pnl_delta)
        if extra:
            payload.update(extra)
        db.add(
            DecisionEvent(
                ts=ts_ms,
                symbol=symbol,
                regime='TRADE',
                signal_score=float(price),
                decision=decision,
                blockers=[],
                rationale=reason,
                risk_state_snapshot=payload,
            )
        )
        self.safety.record_event(decision, payload)

    async def _emit_blocked_event(
        self,
        db: AsyncSession,
        ts_ms: int,
        symbol: str,
        side: str,
        qty: Decimal,
        price: Decimal,
        blockers: list[dict],
    ) -> None:
        db.add(
            DecisionEvent(
                ts=ts_ms,
                symbol=symbol,
                regime='GOVERNOR',
                signal_score=float(price),
                decision='BLOCKED',
                blockers=[str(b.get('name')) for b in blockers],
                rationale='Entry blocked by governor',
                risk_state_snapshot={
                    'event_type': 'BLOCKED',
                    'intended_side': side,
                    'qty': float(qty),
                    'price': float(price),
                    'blockers': blockers,
                },
            )
        )
        self.safety.record_event(
            'BLOCKED',
            {
                'symbol': symbol,
                'side': side,
                'qty': float(qty),
                'price': float(price),
                'blockers': blockers,
            },
        )

    async def _record_decision_event(
        self,
        db: AsyncSession,
        ts_ms: int,
        symbol: str,
        decision: str,
        plan: TradePlan,
        blockers: list[dict],
        rationale: str,
    ) -> None:
        db.add(
            DecisionEvent(
                ts=ts_ms,
                symbol=symbol,
                regime=plan.regime,
                signal_score=float(plan.score_total),
                decision='DECISION',
                blockers=[str(b.get('name')) for b in blockers] if blockers else [],
                rationale=rationale,
                risk_state_snapshot={
                    'event_type': 'DECISION',
                    'decision': decision,
                    'score_total': float(plan.score_total),
                    'score_components': plan.score_components,
                    'reasons': plan.reasons,
                    'strategy_name': plan.strategy_name,
                    'setup_name': plan.setup_name,
                    'regime': plan.regime,
                },
            )
        )
        self.safety.record_event(
            'DECISION',
            {
                'symbol': symbol,
                'decision': decision,
                'score_total': float(plan.score_total),
                'blockers': [str(b.get('name')) for b in blockers] if blockers else [],
            },
        )

    async def _trip_kill_switch(self, mode: KillSwitchMode, reason: str, evidence: dict | None = None) -> dict:
        tripped, effective_mode = self.safety.trip(mode=mode.value, reason=reason, evidence=evidence or {})
        if not tripped:
            return {'ok': True, 'mode': effective_mode.value, 'reason': reason, 'already': True}
        flatten_result = {'flattened': False, 'closed': 0}
        if effective_mode == KillSwitchMode.HARD:
            flatten_result = await self._flatten_all_open_positions(reason=reason)
        if settings.incident_snapshot_on_kill:
            snapshot = await self._build_incident_snapshot(reason=reason, mode=effective_mode, evidence=evidence or {}, flatten=flatten_result)
            incident_id = self.safety.create_incident(reason=reason, mode=effective_mode, snapshot=snapshot)
        else:
            incident_id = None
        return {'ok': True, 'mode': effective_mode.value, 'reason': reason, 'incident_id': incident_id, **flatten_result}

    async def _flatten_all_open_positions(self, reason: str) -> dict:
        closed = 0
        async with self.session_factory() as db:
            open_trades = (
                await db.execute(select(Trade).where(Trade.status == 'OPEN').order_by(desc(Trade.created_at)))
            ).scalars().all()
            for trade in open_trades:
                position = (
                    await db.execute(select(Position).where(Position.symbol == trade.symbol, Position.is_open.is_(True)).limit(1))
                ).scalar_one_or_none()
                if not position:
                    continue
                exit_price = Decimal(str(position.average_price or trade.entry_price or 0))
                now_dt = datetime.now(timezone.utc)
                exit_ts_ms = int(now_dt.timestamp() * 1000)
                await self._final_close(
                    db=db,
                    trade=trade,
                    position=position,
                    exit_price=exit_price,
                    exit_ts_ms=exit_ts_ms,
                    exit_ts=now_dt,
                    reason='kill_switch_force_exit',
                    symbol=trade.symbol,
                    replay_clock=self.tick,
                )
                closed += 1
            await db.commit()
        self.safety.record_event('KILL_FLATTEN', {'reason': reason, 'closed': closed})
        return {'flattened': True, 'closed': closed}

    async def _run_reconciler(self) -> None:
        async with self.session_factory() as db:
            open_positions = (
                await db.execute(select(Position).where(Position.is_open.is_(True)))
            ).scalars().all()
            open_trades = (
                await db.execute(select(Trade).where(Trade.status == 'OPEN'))
            ).scalars().all()
            accounting = await PortfolioAccounting().snapshot(db)
        internal_open = len(self._open_trade_id_by_symbol)
        db_open = len(open_trades)
        pos_open = len(open_positions)
        mismatch = internal_open != db_open or db_open != pos_open
        expected_equity = float(accounting.get('equity_start', 0)) + float(accounting.get('realized_pnl_net', 0)) + float(accounting.get('unrealized_pnl', 0))
        actual_equity = float(accounting.get('equity_now', 0))
        divergence = abs(actual_equity - expected_equity)
        if divergence > 1e-6:
            mismatch = True
        self.safety.record_reconciler(
            mismatch=mismatch,
            detail={
                'internal_open_trade_map': internal_open,
                'db_open_trades': db_open,
                'db_open_positions': pos_open,
                'equity_divergence': divergence,
                'expected_equity': expected_equity,
                'actual_equity': actual_equity,
            },
        )

    def _derive_day_key(self, ts: datetime | None = None) -> str:
        current = ts or datetime.now(timezone.utc)
        return current.astimezone(self._trading_tz).strftime('%Y-%m-%d')

    async def _check_day_rollover(self, ts: datetime, db: AsyncSession | None = None, symbol: str | None = None, ts_ms: int | None = None) -> None:
        day_key = self._derive_day_key(ts)
        if self.last_day_key is None:
            self.last_day_key = day_key
            self.day_rollover_in_effect = False
            return
        if day_key == self.last_day_key:
            self.day_rollover_in_effect = False
            return

        old_day = self.last_day_key
        self.last_day_key = day_key
        self.day_rollover_in_effect = True
        self.daily_state = {
            'daily_consecutive_losses': 0,
            'daily_realized_pnl': 0.0,
            'daily_fees': 0.0,
            'daily_trade_count': 0,
        }
        payload = {
            'event_type': 'DAY_ROLLOVER',
            'old_day_key': old_day,
            'new_day_key': day_key,
        }
        self.safety.record_event('DAY_ROLLOVER', payload)

        if db is not None:
            db.add(
                DecisionEvent(
                    ts=int(ts_ms or int(ts.timestamp() * 1000)),
                    symbol=symbol or self.replay_symbol or settings.default_symbol,
                    regime='SYSTEM',
                    signal_score=0.0,
                    decision='INFO',
                    blockers=[],
                    rationale='day_rollover',
                    risk_state_snapshot=payload,
                )
            )

    @staticmethod
    def _pretrade_metrics_from_governor(gov: dict | None) -> dict:
        if not gov:
            return {
                'ddPct': 0.0,
                'dailyLossPct': 0.0,
                'consecutiveLosses': 0,
                'tradesPerDay': 0,
                'stalenessMs': None,
                'errorRate': 0.0,
            }
        config = gov.get('config') or {}
        stats = gov.get('stats') or {}
        daily_limit = float(config.get('max_daily_loss_pct') or 0)
        daily_loss_abs = abs(float(stats.get('daily_pnl_net') or 0))
        daily_loss_pct = daily_limit if daily_limit == 0 else (daily_loss_abs / daily_limit * 100.0)
        return {
            'ddPct': float(stats.get('global_dd_pct') or 0.0),
            'dailyLossPct': daily_loss_pct,
            'consecutiveLosses': int(stats.get('consecutive_losses') or 0),
            'tradesPerDay': int(stats.get('trades_today') or 0),
            'stalenessMs': None,
            'errorRate': 0.0,
        }

    def _build_pretrade_decision(self, gov: dict) -> dict:
        blockers = gov.get('blockers') or []
        allowed = bool(gov.get('eligible', False))
        reason_code = None if allowed else (str(blockers[0].get('name')) if blockers else 'governor_block')
        reason_detail = None if allowed else (str(blockers[0].get('detail') or blockers[0].get('reason')) if blockers else 'blocked')
        metrics = self._pretrade_metrics_from_governor(gov)
        metrics['stalenessMs'] = self.safety.staleness_ms()
        metrics['errorRate'] = self.safety.current_error_rate()
        return {
            'allowed': allowed,
            'reasonCode': reason_code,
            'reasonDetail': reason_detail,
            'metrics': metrics,
        }

    async def _build_incident_snapshot(self, reason: str, mode: KillSwitchMode, evidence: dict, flatten: dict) -> dict:
        async with self.session_factory() as db:
            accounting = await PortfolioAccounting().snapshot(db)
            open_positions = (
                await db.execute(select(Position).where(Position.is_open.is_(True)).order_by(desc(Position.created_at)).limit(200))
            ).scalars().all()
            open_trades = (
                await db.execute(select(Trade).where(Trade.status == 'OPEN').order_by(desc(Trade.created_at)).limit(200))
            ).scalars().all()
        return {
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'run_mode': self.mode,
            'engine': {
                'running': self.running,
                'tick': self.tick,
                'last_event_at': self.last_event_at.isoformat() if self.last_event_at else None,
            },
            'market': {
                'replay_dataset_id': self.replay_dataset_id,
                'replay_status': self.replay_status(),
                'last_symbols': list(self._history_by_symbol.keys())[:20],
            },
            'open_positions': [
                {
                    'id': str(p.id),
                    'symbol': p.symbol,
                    'side': p.side,
                    'quantity': float(p.quantity),
                    'entry': float(p.average_price),
                    'unrealized_pnl': float(p.unrealized_pnl),
                }
                for p in open_positions
            ],
            'open_orders': [],
            'open_trades': [
                {
                    'id': str(t.id),
                    'symbol': t.symbol,
                    'side': t.side,
                    'qty': float(t.quantity),
                    'entry': float(t.entry_price),
                }
                for t in open_trades
            ],
            'accounting': accounting,
            'governor': self.safety.status().get('governor_last'),
            'summary': {
                'last_trades_count': len(open_trades),
                'last_position_snapshot': (
                    {
                        'id': str(open_positions[0].id),
                        'symbol': open_positions[0].symbol,
                        'side': open_positions[0].side,
                        'quantity': float(open_positions[0].quantity),
                        'entry': float(open_positions[0].average_price),
                    }
                    if open_positions
                    else None
                ),
                'last_error': self.safety.status().get('last_error'),
                'staleness_ms': self.safety.status().get('staleness_ms'),
            },
            'kill': {
                'mode': mode.value,
                'reason': reason,
                'evidence': evidence,
                'flatten': flatten,
            },
            'ring_events': self.safety.ring_snapshot(),
        }

    @staticmethod
    def _gross_delta(trade_side: str, entry: Decimal, exit_price: Decimal, qty: Decimal) -> Decimal:
        if trade_side == 'BUY':
            return (exit_price - entry) * qty
        return (entry - exit_price) * qty

    @staticmethod
    def _calc_unrealized(position: Position, mark_price: Decimal) -> Decimal:
        entry = Decimal(str(position.average_price))
        qty = Decimal(str(position.quantity))
        if position.side == 'BUY':
            return (mark_price - entry) * qty
        return (entry - mark_price) * qty

    @staticmethod
    def _tick_timestamp_ms(tick: dict) -> int:
        raw = tick.get('timestamp')
        if not raw:
            return int(datetime.now(timezone.utc).timestamp() * 1000)
        value = str(raw).strip()
        if value.isdigit():
            parsed = int(value)
            return parsed * 1000 if parsed < 1_000_000_000_000 else parsed
        return int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() * 1000)

    @staticmethod
    def _tick_timestamp_dt(tick: dict) -> datetime:
        return datetime.fromtimestamp(EngineService._tick_timestamp_ms(tick) / 1000, tz=timezone.utc)

    async def _publish_state(self) -> None:
        await self.redis.set('engine:state', str(self.status()))

    def status(self) -> dict:
        payload = {
            'running': self.running,
            'mode': self.mode,
            'tick': self.tick,
            'last_event_at': self.last_event_at,
            'risk': self.risk.risk_status(),
            'active_profile': self.active_profile,
            'safety': self.safety.status(),
            'pre_trade_decision': self.last_pre_trade_decision,
            'day': {
                'day_key': self.last_day_key or self._derive_day_key(),
                'rollover_in_effect': self.day_rollover_in_effect,
                'daily_consecutive_losses': int(self.daily_state.get('daily_consecutive_losses', 0)),
                'daily_realized_pnl': float(self.daily_state.get('daily_realized_pnl', 0.0)),
                'daily_fees': float(self.daily_state.get('daily_fees', 0.0)),
                'daily_trade_count': int(self.daily_state.get('daily_trade_count', 0)),
            },
        }
        if self.replay:
            payload['replay'] = self.replay_status()
        return payload
