from __future__ import annotations

import asyncio
from collections import Counter, deque
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
from app.profiles import get_profile_manager
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
        self.profile_manager = get_profile_manager(settings)
        initial_profile = str(settings.active_profile or settings.strategy_profile or 'TREND_STABLE').upper()
        self.active_profile: str = self.profile_manager.apply(initial_profile)
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
        self._decision_traces = deque(maxlen=max(10, int(settings.decision_trace_ring_size)))
        self._lifecycle_events = deque(maxlen=max(20, int(settings.lifecycle_event_ring_size)))
        self._trace_summary: dict[str, object] = {
            'total_bars': 0,
            'regime_pass': 0,
            'regime_fail': 0,
            'setup_confirmed_true': 0,
            'setup_confirmed_false': 0,
            'router_selected': Counter(),
            'blockers': Counter(),
            'governor_blockers': Counter(),
            'score_ge_min_no_trade': Counter(),
            'entry_bars': [],
            'entry_blockers_when_regime_pass': Counter(),
            'manage_reasons': Counter(),
        }

    async def start(self, mode: str = 'live') -> dict:
        await self._sync_profile_from_store()
        if self.running:
            return self.status()
        self.mode = mode
        self.running = True
        self.task = asyncio.create_task(self._loop())
        return self.status()

    async def set_profile(self, profile: str) -> dict:
        self.active_profile = self.profile_manager.apply(profile)
        try:
            await self.redis.set('engine:active_profile', self.active_profile)
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
        await self._sync_profile_from_store()
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
        self._decision_traces.clear()
        self._trace_summary = {
            'total_bars': 0,
            'regime_pass': 0,
            'regime_fail': 0,
            'setup_confirmed_true': 0,
            'setup_confirmed_false': 0,
            'router_selected': Counter(),
            'blockers': Counter(),
            'governor_blockers': Counter(),
            'score_ge_min_no_trade': Counter(),
            'entry_bars': [],
            'entry_blockers_when_regime_pass': Counter(),
            'manage_reasons': Counter(),
        }
        self._lifecycle_events.clear()
        await self.replay.start()
        return await self.start(mode='replay')

    async def stop_replay(self) -> dict:
        if self.replay:
            await self.replay.stop()
        self._open_trade_id_by_symbol.clear()
        self._open_bar_by_symbol.clear()
        self._tp1_done_by_trade.clear()
        self._history_by_symbol.clear()
        self._decision_traces.clear()
        self._print_lifecycle_diagnostics()
        return await self.stop()

    async def pause_replay(self) -> dict:
        if self.replay:
            await self.replay.pause()
        return self.replay_status()

    async def _sync_profile_from_store(self) -> None:
        get_fn = getattr(self.redis, 'get', None)
        if not callable(get_fn):
            return
        try:
            stored = await get_fn('engine:active_profile')
        except Exception:
            return
        if not stored:
            return
        try:
            self.active_profile = self.profile_manager.apply(str(stored))
        except ValueError:
            return

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
        plan = self.strategy_v1.build_plan(history, self.replay_timeframe, replay_clock)

        async with self.session_factory() as db:
            await self._check_day_rollover(ts=ts_dt, db=db, symbol=symbol, ts_ms=ts_ms)
            position = (
                await db.execute(select(Position).where(Position.symbol == symbol, Position.is_open.is_(True)).limit(1))
            ).scalar_one_or_none()
            trade = await self._get_open_trade_for_symbol(db=db, symbol=symbol)
            blockers: list[dict] = []
            decision = plan.decision
            rationale = ', '.join(plan.reasons) if plan.reasons else 'strategy_evaluated'

            final_action = 'HOLD'
            if position and trade:
                position.unrealized_pnl = self._calc_unrealized(position=position, mark_price=price_close)
                final_action, manage_reason, manage_tags = await self._evaluate_open_trade(
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
                await self._record_lifecycle_event(
                    event_type='POSITION_MANAGE_TICK',
                    timestamp=ts_ms,
                    symbol=symbol,
                    trade=trade,
                    position=position,
                    action=final_action,
                    primary_reason=manage_reason,
                    reason_tags=manage_tags,
                    mark_price=price_close,
                )
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
                    await self._record_lifecycle_event(
                        event_type='ORDER_REJECTED',
                        timestamp=ts_ms,
                        symbol=symbol,
                        trade=None,
                        position=position,
                        action='ENTRY_BLOCKED',
                        primary_reason='kill_switch',
                        reason_tags=[str(guard_reason or 'kill_switch')],
                        mark_price=price_close,
                        entry_qty=Decimal(str(plan.qty)),
                    )
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
                        extra_blockers=plan.blockers,
                        rationale='Entry blocked by kill switch',
                    )
                    await db.commit()
                    return
                accounting = await PortfolioAccounting().snapshot(db)
                if self.active_profile == 'GROWTH_HUNTER' and plan.entry_price is not None and plan.stop_price is not None:
                    entry_px = float(plan.entry_price)
                    stop_px = float(plan.stop_price)
                    stop_distance = abs(entry_px - stop_px)
                    if stop_distance > 0:
                        risk_amount = float(accounting['equity_now']) * float(settings.growth_risk_pct)
                        sized_qty = round(max(0.0, risk_amount / stop_distance), 8)
                        plan.qty = sized_qty
                        plan.base_qty = sized_qty
                        plan.final_qty = sized_qty
                        plan.size_mult = 1.0
                        plan.stop_distance = stop_distance
                        plan.risk_pct_used = float(settings.growth_risk_pct)
                        if plan.side == 'BUY':
                            plan.target_price = entry_px + (stop_distance * float(settings.growth_target_r))
                        else:
                            plan.target_price = entry_px - (stop_distance * float(settings.growth_target_r))
                        plan.tp1_price = None
                        plan.tp2_price = plan.target_price
                        plan.r_multiple = float(settings.growth_target_r)
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
                    final_action = 'ENTER'
                else:
                    decision = 'BLOCKED'
                    blockers = gov['blockers']
                    final_action = 'HOLD'
                    await self._record_lifecycle_event(
                        event_type='ORDER_REJECTED',
                        timestamp=ts_ms,
                        symbol=symbol,
                        trade=None,
                        position=position,
                        action='ENTRY_BLOCKED',
                        primary_reason='governor_block',
                        reason_tags=[str(b.get('name')) for b in (gov.get('blockers') or []) if isinstance(b, dict) and b.get('name')],
                        mark_price=price_close,
                        entry_qty=Decimal(str(plan.qty)),
                    )
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
                if plan.decision in ('enter_long', 'enter_short'):
                    final_action = 'SIGNAL_LONG' if plan.decision == 'enter_long' else 'SIGNAL_SHORT'

            blocker_list = self._build_blockers(plan=plan, blockers=blockers, position_open=bool(position and trade), decision=decision)
            primary_blocker = blocker_list[0] if blocker_list else None
            if final_action == 'HOLD' and plan.decision == 'hold' and 'router_stand_down' in (plan.reasons or []):
                final_action = 'STAND_DOWN'
            trace = self._build_decision_trace(
                tick=tick,
                symbol=symbol,
                replay_clock=replay_clock,
                plan=plan,
                final_action=final_action,
                decision=decision,
                blockers=blocker_list,
                primary_blocker=primary_blocker,
                position_open=bool(position and trade),
                router_reason=str(getattr(plan, 'router_reason', 'fallback_hold')),
                router_selected_strategy=str(getattr(plan, 'router_selected_strategy', 'none')),
            )
            self._record_decision_trace(trace)

            await self._record_decision_event(
                db=db,
                ts_ms=ts_ms,
                symbol=symbol,
                decision=decision.upper() if decision != 'hold' else 'HOLD',
                plan=plan,
                blockers=blockers,
                extra_blockers=plan.blockers,
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
            strategy_profile=self.active_profile,
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
            base_qty=Decimal(str(plan.base_qty)),
            size_mult=Decimal(str(plan.size_mult)),
            final_qty=Decimal(str(plan.final_qty)),
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
        await self._record_lifecycle_event(
            event_type='ORDER_CREATED',
            timestamp=entry_ts_ms,
            symbol=symbol,
            trade=trade,
            position=position,
            action='ENTRY',
            primary_reason='order_created',
            reason_tags=['router_entry_signal'],
            mark_price=entry_price,
            entry_qty=qty,
        )
        await self._record_lifecycle_event(
            event_type='ORDER_ACCEPTED',
            timestamp=entry_ts_ms,
            symbol=symbol,
            trade=trade,
            position=position,
            action='ENTRY',
            primary_reason='order_accepted',
            reason_tags=['replay_fill_assumed'],
            mark_price=entry_price,
            entry_qty=qty,
        )
        await self._record_lifecycle_event(
            event_type='POSITION_OPENED',
            timestamp=entry_ts_ms,
            symbol=symbol,
            trade=trade,
            position=position,
            action='OPEN',
            primary_reason='entry_fill',
            reason_tags=['position_open'],
            mark_price=entry_price,
            entry_qty=qty,
        )
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
                'active_profile': self.active_profile,
                'entry_module': plan.router_selected_strategy,
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
                'active_mode': plan.active_mode,
                'mode_reasons': plan.mode_reasons,
                'breakout_box_high': plan.breakout_box_high,
                'breakout_box_low': plan.breakout_box_low,
                'breakout_compression': plan.breakout_compression,
                'breakout_recent': plan.breakout_recent,
                'pullback_v2_ok': plan.pullback_v2_ok,
                'pullback_v2_reasons': plan.pullback_v2_reasons,
                'base_qty': plan.base_qty,
                'size_mult': plan.size_mult,
                'final_qty': plan.final_qty,
                'risk_pct_used': plan.risk_pct_used,
                'stop_distance': plan.stop_distance,
                'target_price': plan.target_price,
                'R_multiple': plan.r_multiple,
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
    ) -> tuple[str, str, list[str]]:
        stop = Decimal(str(trade.stop_price or 0))
        tp1 = Decimal(str(trade.tp1_price or 0))
        tp2 = Decimal(str(trade.tp2_price or 0))
        tp1_done = bool(self._tp1_done_by_trade.get(trade.id, False))
        bars_held = max(0, replay_clock - self._open_bar_by_symbol.get(trade.symbol, replay_clock))
        time_stop_limit = int(trade.time_stop_bars or settings.time_stop_bars)

        growth_trade = str(trade.strategy_profile or '').upper() == 'GROWTH_HUNTER'
        stop_hit = False
        tp1_hit = False
        tp2_hit = False
        if trade.side == 'BUY':
            stop_hit = low <= stop if stop > 0 else False
            tp1_hit = (not growth_trade) and (high >= tp1 if tp1 > 0 else False)
            tp2_hit = high >= tp2 if tp2 > 0 else False
        else:
            stop_hit = high >= stop if stop > 0 else False
            tp1_hit = (not growth_trade) and (low <= tp1 if tp1 > 0 else False)
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
            return 'EXIT', 'sl_close', ['stop_loss_hit']

        if tp2_hit:
            if (not growth_trade) and (not tp1_done) and tp1 > 0:
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
            return 'EXIT', 'tp_close', ['tp2_hit']

        if (not growth_trade) and tp1_hit and not tp1_done:
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
            return 'PARTIAL', 'tp1_close', ['tp1_hit']

        if (not growth_trade) and bars_held >= time_stop_limit:
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
            return 'EXIT', 'time_close', ['time_stop']

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
            return 'EXIT', 'signal_close', ['opposite_signal']
        return 'HOLD', 'manage_hold', ['hold_no_exit_trigger']

    def _build_blockers(self, *, plan: TradePlan, blockers: list[dict], position_open: bool, decision: str) -> list[str]:
        blocker_set = set(str(b.get('name')) for b in blockers if isinstance(b, dict) and b.get('name'))
        blocker_set.update(str(b) for b in (plan.blockers or []) if b)
        if position_open:
            blocker_set.add('already_in_position')
        if decision == 'BLOCKED' and any(str(b.get('name')) == 'kill_switch' for b in blockers if isinstance(b, dict)):
            blocker_set.add('kill_switch')
        if plan.decision == 'hold' and plan.side is None and 'setup_not_confirmed' in (plan.reasons or []):
            blocker_set.add('waiting_confirm')
        if any(str(r).startswith('breakout_no_') for r in (plan.reasons or [])):
            blocker_set.add('router_hold')
        if decision == 'BLOCKED' and blockers:
            blocker_set.add('governor_block')
        if plan.qty <= 0:
            blocker_set.add('qty_invalid')
        if not blocker_set and plan.decision == 'hold' and str(getattr(plan, 'router_reason', '')) != 'forced_strong_trend':
            blocker_set.add('router_hold')
        return sorted(blocker_set)

    def _router_status(self, plan: TradePlan) -> tuple[str, str]:
        selected = str(getattr(plan, 'router_selected_strategy', 'none') or 'none')
        reason = str(getattr(plan, 'router_reason', 'fallback_hold') or 'fallback_hold')
        if plan.active_mode == 'STAND_DOWN':
            return 'stand_down', selected
        if plan.decision in ('enter_long', 'enter_short'):
            return f'select_{selected}', selected
        return reason, selected

    def _build_decision_trace(
        self,
        *,
        tick: dict,
        symbol: str,
        replay_clock: int,
        plan: TradePlan,
        final_action: str,
        decision: str,
        blockers: list[str],
        primary_blocker: str | None,
        position_open: bool,
        router_reason: str,
        router_selected_strategy: str,
    ) -> dict:
        router_status, selected = self._router_status(plan)
        selected = router_selected_strategy or selected
        setup_confirmed = bool(plan.side in ('BUY', 'SELL') and plan.decision in ('enter_long', 'enter_short'))
        evaluated = [
            {'strategy': 'trend', 'evaluated': True, 'reason': 'baseline'},
            {'strategy': 'breakout', 'evaluated': bool(settings.feature_breakout), 'reason': 'feature_on' if settings.feature_breakout else 'feature_off'},
            {'strategy': 'pb2', 'evaluated': bool(settings.feature_pullback_v2), 'reason': 'feature_on' if settings.feature_pullback_v2 else 'feature_off'},
        ]
        entry_eligibility = bool(
            (plan.side in ('BUY', 'SELL')) and (decision != 'BLOCKED') and not position_open and final_action in {'ENTER', 'SIGNAL_LONG', 'SIGNAL_SHORT'}
        )
        size_reasons = [r for r in (plan.reasons or []) if r in {'trend_bonus', 'chop_penalty', 'vol_high_boost', 'vol_low_penalty', 'vol_cap_applied', 'vol_sizing_on'}]
        return {
            'timestamp': tick.get('timestamp'),
            'symbol': symbol,
            'bar_index': replay_clock,
            'router': {
                'evaluated': evaluated,
                'selected': selected,
                'decision_status': router_status,
                'reason': router_reason,
            },
            'regime_gate': {
                'pass': bool(plan.regime_gate_ok),
                'fail_reasons': list(plan.regime_gate_reasons or []),
                'metrics': dict(plan.regime_gate_metrics or {}),
            },
            'setup_status': {
                'setup_confirmed': setup_confirmed,
                'reasons': [r for r in (plan.reasons or []) if 'setup' in str(r) or 'pb2_' in str(r) or 'breakout_' in str(r)],
            },
            'scoring': {
                'score': float(plan.score_total),
                'score_min': float(settings.score_min),
                'components': dict(plan.score_components or {}),
            },
            'feature_flags': {
                'FEATURE_BREAKOUT': bool(settings.feature_breakout),
                'FEATURE_PULLBACK_V2': bool(settings.feature_pullback_v2),
                'FEATURE_VOL_SIZING': bool(settings.feature_vol_sizing),
            },
            'risk_sizing': {
                'base_qty': float(plan.base_qty),
                'multiplier': float(plan.size_mult),
                'final_qty': float(plan.final_qty),
                'cap_applied': 'vol_cap_applied' in size_reasons,
                'reason_tags': size_reasons,
            },
            'entry_eligibility': entry_eligibility,
            'final_action': final_action,
            'trade_blocker_primary': primary_blocker,
            'trade_blockers': blockers,
        }

    def _record_decision_trace(self, trace: dict) -> None:
        self._decision_traces.append(trace)
        summary = self._trace_summary
        summary['total_bars'] = int(summary['total_bars']) + 1
        if trace.get('regime_gate', {}).get('pass'):
            summary['regime_pass'] = int(summary['regime_pass']) + 1
        else:
            summary['regime_fail'] = int(summary['regime_fail']) + 1
        if trace.get('setup_status', {}).get('setup_confirmed'):
            summary['setup_confirmed_true'] = int(summary['setup_confirmed_true']) + 1
        else:
            summary['setup_confirmed_false'] = int(summary['setup_confirmed_false']) + 1
        router_selected = str(trace.get('router', {}).get('selected') or 'none')
        cast_router = summary['router_selected']
        if isinstance(cast_router, Counter):
            cast_router[router_selected] += 1
        blockers_counter = summary['blockers']
        if isinstance(blockers_counter, Counter):
            for blocker in trace.get('trade_blockers') or []:
                blockers_counter[str(blocker)] += 1
        if trace.get('regime_gate', {}).get('pass') and trace.get('final_action') != 'ENTER':
            pass_blockers = summary.get('entry_blockers_when_regime_pass')
            if isinstance(pass_blockers, Counter):
                for blocker in trace.get('trade_blockers') or ['none']:
                    pass_blockers[str(blocker)] += 1
        gov_counter = summary['governor_blockers']
        if isinstance(gov_counter, Counter) and 'governor_block' in (trace.get('trade_blockers') or []):
            for blocker in trace.get('trade_blockers') or []:
                if blocker.startswith('gov_') or blocker in {'governor_block'}:
                    gov_counter[str(blocker)] += 1
        score_ge_no_trade = summary['score_ge_min_no_trade']
        if isinstance(score_ge_no_trade, Counter):
            if float(trace.get('scoring', {}).get('score') or 0.0) >= float(trace.get('scoring', {}).get('score_min') or 0.0) and trace.get('final_action') not in {'ENTER', 'EXIT', 'PARTIAL'}:
                score_ge_no_trade[str(trace.get('trade_blocker_primary') or 'none')] += 1
        if trace.get('final_action') == 'ENTER':
            bars = summary['entry_bars']
            if isinstance(bars, list):
                bars.append({'bar_index': int(trace.get('bar_index') or 0), 'timestamp': trace.get('timestamp')})

    def observability_snapshot(self, last_n: int = 100) -> dict:
        traces = list(self._decision_traces)[-max(1, int(last_n)) :]
        summary = self._trace_summary
        total_bars = int(summary['total_bars']) if summary.get('total_bars') is not None else 0
        blockers = summary['blockers'] if isinstance(summary.get('blockers'), Counter) else Counter()
        score_ge = summary['score_ge_min_no_trade'] if isinstance(summary.get('score_ge_min_no_trade'), Counter) else Counter()
        gov = summary['governor_blockers'] if isinstance(summary.get('governor_blockers'), Counter) else Counter()
        manage = summary['manage_reasons'] if isinstance(summary.get('manage_reasons'), Counter) else Counter()
        pass_blockers = summary['entry_blockers_when_regime_pass'] if isinstance(summary.get('entry_blockers_when_regime_pass'), Counter) else Counter()
        no_trade = self._diagnose_no_trade_streak()
        lifecycle_events = list(self._lifecycle_events)[-max(1, int(last_n)) :]
        return {
            'replay': self.replay_status(),
            'decision_traces': traces,
            'blocker_counters': {
                'total_bars': total_bars,
                'regime_pass': int(summary.get('regime_pass') or 0),
                'regime_fail': int(summary.get('regime_fail') or 0),
                'setup_confirmed_true': int(summary.get('setup_confirmed_true') or 0),
                'setup_confirmed_false': int(summary.get('setup_confirmed_false') or 0),
                'router_selected': dict(summary.get('router_selected') or {}),
                'blockers_ranked': [
                    {'blocker': name, 'count': count, 'pct_total_bars': (count / total_bars * 100.0) if total_bars else 0.0}
                    for name, count in blockers.most_common()
                ],
                'governor_blocks': dict(gov),
                'score_ge_min_no_trade': dict(score_ge),
                'entry_blockers_when_regime_pass': dict(pass_blockers),
            },
            'lifecycle_events': lifecycle_events,
            'lifecycle_summary': self._build_lifecycle_summary(),
            'manage_reasons_ranked': [{'reason': k, 'count': v} for k, v in manage.most_common(10)],
            'no_trade_streak': no_trade,
        }


    def _build_lifecycle_summary(self) -> dict:
        events = list(self._lifecycle_events)
        opened = [e for e in events if e.get('event_type') == 'POSITION_OPENED']
        closed = [e for e in events if e.get('event_type') == 'POSITION_CLOSED']
        holding_seconds: list[float] = []
        opened_by_trade = {str(e.get('trade_id')): e for e in opened if e.get('trade_id')}
        for close in closed:
            key = str(close.get('trade_id'))
            if key in opened_by_trade:
                try:
                    holding_seconds.append(max(0.0, (float(close.get('timestamp') or 0) - float(opened_by_trade[key].get('timestamp') or 0)) / 1000.0))
                except Exception:
                    continue
        exit_reasons = Counter(str(e.get('primary_reason') or 'unknown') for e in closed)
        manage_reasons = Counter(str(e.get('primary_reason') or 'unknown') for e in events if e.get('event_type') == 'POSITION_MANAGE_TICK')
        return {
            'trades_count': len(opened),
            'avg_holding_seconds': (sum(holding_seconds) / len(holding_seconds)) if holding_seconds else 0.0,
            'mae': None,
            'mfe': None,
            'top_exit_reasons': [{'reason': k, 'count': v} for k, v in exit_reasons.most_common(10)],
            'top_manage_reasons': [{'reason': k, 'count': v} for k, v in manage_reasons.most_common(10)],
        }

    def _print_lifecycle_diagnostics(self) -> None:
        summary = self._build_lifecycle_summary()
        pass_blockers = self._trace_summary.get('entry_blockers_when_regime_pass')
        ranked_pass_blockers = []
        if isinstance(pass_blockers, Counter):
            ranked_pass_blockers = [{'reason': k, 'count': v} for k, v in pass_blockers.most_common(10)]
        print('=== TRADE LIFECYCLE DIAGNOSTICS ===')
        print({'trades_count': summary.get('trades_count'), 'avg_holding_seconds': summary.get('avg_holding_seconds'), 'mae': summary.get('mae'), 'mfe': summary.get('mfe')})
        print('top_exit_reasons', summary.get('top_exit_reasons'))
        print('top_manage_reasons', summary.get('top_manage_reasons'))
        print('top_entry_blockers_when_regime_pass', ranked_pass_blockers)

    async def _record_lifecycle_event(
        self,
        *,
        event_type: str,
        timestamp: int,
        symbol: str,
        trade: Trade | None,
        position: Position | None,
        action: str,
        primary_reason: str,
        reason_tags: list[str],
        mark_price: Decimal,
        entry_qty: Decimal | None = None,
        closed_qty: Decimal | None = None,
        exit_price: Decimal | None = None,
    ) -> None:
        entry_qty_val = float(entry_qty if entry_qty is not None else (trade.quantity if trade is not None else 0))
        current_qty_val = float(position.quantity) if position is not None else 0.0
        closed_qty_val = float(closed_qty) if closed_qty is not None else 0.0
        mark = float(mark_price)
        event = {
            'timestamp': timestamp,
            'symbol': symbol,
            'trade_id': str(trade.id) if trade else None,
            'position_state': 'OPEN' if (position is not None and bool(position.is_open)) else 'FLAT',
            'event_type': event_type,
            'action': action,
            'primary_reason': primary_reason,
            'reason_tags': list(reason_tags or []),
            'entry_qty': entry_qty_val,
            'current_qty': current_qty_val,
            'closed_qty': closed_qty_val,
            'entry_price': float(trade.entry_price) if trade is not None else None,
            'mark_price': mark,
            'stop_price': float(trade.stop_price) if trade is not None and trade.stop_price is not None else None,
            'tp1_price': float(trade.tp1_price) if trade is not None and trade.tp1_price is not None else None,
            'tp2_price': float(trade.tp2_price) if trade is not None and trade.tp2_price is not None else None,
            'exit_price': float(exit_price) if exit_price is not None else None,
            'unrealized_pnl': float(position.unrealized_pnl) if position is not None and position.unrealized_pnl is not None else 0.0,
            'realized_pnl': float(trade.pnl_gross) if trade is not None and trade.pnl_gross is not None else 0.0,
            'fees': float(trade.fees_total) if trade is not None and trade.fees_total is not None else 0.0,
            'net_pnl': (float(trade.pnl_net) if trade is not None and trade.pnl_net is not None else 0.0),
            'notional': float(trade.notional) if trade is not None and trade.notional is not None else abs(entry_qty_val * mark),
            'r_multiple': None,
        }
        self._lifecycle_events.append(event)
        if event_type == 'POSITION_MANAGE_TICK':
            cast_manage = self._trace_summary.get('manage_reasons')
            if isinstance(cast_manage, Counter):
                cast_manage[str(primary_reason or 'unknown')] += 1

    def _diagnose_no_trade_streak(self) -> dict:
        traces = list(self._decision_traces)
        if not traces:
            return {
                'length_bars': 0,
                'length_days': 0.0,
                'start_ts': None,
                'end_ts': None,
                'top_blockers': [],
                'pass_rate_in_streak_pct': None,
                'pass_rate_outside_streak_pct': None,
            }
        best = (0, 0, -1)
        start = 0
        in_run = False
        for i, row in enumerate(traces):
            if row.get('final_action') != 'ENTER':
                if not in_run:
                    in_run = True
                    start = i
            else:
                if in_run and (i - start) > best[0]:
                    best = (i - start, start, i - 1)
                in_run = False
        if in_run and (len(traces) - start) > best[0]:
            best = (len(traces) - start, start, len(traces) - 1)
        if best[0] <= 0:
            return {
                'length_bars': 0,
                'length_days': 0.0,
                'start_ts': None,
                'end_ts': None,
                'top_blockers': [],
                'pass_rate_in_streak_pct': None,
                'pass_rate_outside_streak_pct': None,
            }
        streak = traces[best[1] : best[2] + 1]
        blocker_counter: Counter[str] = Counter()
        for row in streak:
            for blocker in row.get('trade_blockers') or []:
                blocker_counter[str(blocker)] += 1
        pass_in = sum(1 for row in streak if row.get('regime_gate', {}).get('pass'))
        outside = traces[: best[1]] + traces[best[2] + 1 :]
        pass_out = sum(1 for row in outside if row.get('regime_gate', {}).get('pass'))
        start_ts = streak[0].get('timestamp')
        end_ts = streak[-1].get('timestamp')
        length_days = 0.0
        try:
            if start_ts and end_ts:
                length_days = max(0.0, (float(end_ts) - float(start_ts)) / 86400.0)
        except Exception:
            length_days = 0.0
        return {
            'length_bars': best[0],
            'length_days': length_days,
            'start_ts': start_ts,
            'end_ts': end_ts,
            'top_blockers': [{'blocker': k, 'count': v} for k, v in blocker_counter.most_common(5)],
            'pass_rate_in_streak_pct': (pass_in / len(streak) * 100.0) if streak else None,
            'pass_rate_outside_streak_pct': (pass_out / len(outside) * 100.0) if outside else None,
        }

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

        await self._record_lifecycle_event(
            event_type='PARTIAL_TP',
            timestamp=close_ts_ms,
            symbol=trade.symbol,
            trade=trade,
            position=position,
            action='PARTIAL',
            primary_reason=reason,
            reason_tags=['partial_take_profit'],
            mark_price=close_price,
            closed_qty=close_qty,
            exit_price=close_price,
        )
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

        await self._record_lifecycle_event(
            event_type='EXIT_SIGNALLED',
            timestamp=exit_ts_ms,
            symbol=symbol,
            trade=trade,
            position=position,
            action='EXIT',
            primary_reason=reason,
            reason_tags=['exit_triggered'],
            mark_price=exit_price,
            exit_price=exit_price,
            closed_qty=remaining_qty,
        )
        await self._record_lifecycle_event(
            event_type='POSITION_CLOSED',
            timestamp=exit_ts_ms,
            symbol=symbol,
            trade=trade,
            position=position,
            action='CLOSE',
            primary_reason=reason,
            reason_tags=['position_closed'],
            mark_price=exit_price,
            exit_price=exit_price,
            closed_qty=remaining_qty,
        )
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
            'active_profile': self.active_profile,
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
            'strategy_profile': trade.strategy_profile or self.active_profile,
            'score_at_entry': float(trade.score_at_entry) if trade.score_at_entry is not None else None,
            'reduceOnly': decision in {'EXIT', 'PARTIAL'},
            'stop_distance': abs(float(trade.entry_price) - float(trade.stop_price)) if trade.stop_price is not None else None,
            'target_price': float(trade.tp2_price) if trade.tp2_price is not None else None,
            'R_multiple': float(settings.growth_target_r) if str(trade.strategy_profile or '').upper() == 'GROWTH_HUNTER' else None,
            'risk_pct_used': float(settings.growth_risk_pct) if str(trade.strategy_profile or '').upper() == 'GROWTH_HUNTER' else None,
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
        extra_blockers: list[str] | None,
        rationale: str,
    ) -> None:
        merged_blockers: list[str] = []
        merged_blockers.extend([str(b.get('name')) for b in blockers] if blockers else [])
        merged_blockers.extend([str(b) for b in (extra_blockers or []) if b])
        dedup_blockers = sorted(set(merged_blockers))
        db.add(
            DecisionEvent(
                ts=ts_ms,
                symbol=symbol,
                regime=plan.regime,
                signal_score=float(plan.score_total),
                decision='DECISION',
                blockers=dedup_blockers,
                rationale=rationale,
                risk_state_snapshot={
                    'event_type': 'DECISION',
                    'active_profile': self.active_profile,
                    'decision': decision,
                    'score_total': float(plan.score_total),
                    'score_components': plan.score_components,
                    'reasons': plan.reasons,
                    'strategy_name': plan.strategy_name,
                    'setup_name': plan.setup_name,
                    'regime': plan.regime,
                    'regime_state': plan.regime_state,
                    'regime_direction': plan.regime_direction,
                    'regime_gate_ok': plan.regime_gate_ok,
                    'regime_gate_reasons': plan.regime_gate_reasons,
                    'regime_gate_metrics': plan.regime_gate_metrics,
                    'active_mode': plan.active_mode,
                    'mode_reasons': plan.mode_reasons,
                    'breakout_box_high': plan.breakout_box_high,
                    'breakout_box_low': plan.breakout_box_low,
                    'breakout_compression': plan.breakout_compression,
                    'breakout_recent': plan.breakout_recent,
                    'pullback_v2_ok': plan.pullback_v2_ok,
                    'pullback_v2_reasons': plan.pullback_v2_reasons,
                    'base_qty': plan.base_qty,
                    'size_mult': plan.size_mult,
                    'final_qty': plan.final_qty,
                    'blockers': dedup_blockers,
                    'final_action': 'STAND_DOWN' if plan.active_mode == 'STAND_DOWN' else ('SIGNAL_LONG' if plan.decision == 'enter_long' else ('SIGNAL_SHORT' if plan.decision == 'enter_short' else 'HOLD')),
                    'entry_eligibility': bool(plan.decision in ('enter_long', 'enter_short')),
                    'router_selected_strategy': str(getattr(plan, 'router_selected_strategy', 'none')),
                    'router_reason': str(getattr(plan, 'router_reason', 'fallback_hold')),
                    'trade_blocker_primary': dedup_blockers[0] if dedup_blockers else None,
                    'trade_blockers': dedup_blockers,
                    'primary_blocker': dedup_blockers[0] if (decision != 'ENTER' and dedup_blockers) else None,
                },
            )
        )
        self.safety.record_event(
            'DECISION',
            {
                'symbol': symbol,
                'decision': decision,
                'score_total': float(plan.score_total),
                'blockers': dedup_blockers,
                'regime_gate_ok': plan.regime_gate_ok,
                'regime_state': plan.regime_state,
                'active_profile': self.active_profile,
                'active_mode': plan.active_mode,
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
