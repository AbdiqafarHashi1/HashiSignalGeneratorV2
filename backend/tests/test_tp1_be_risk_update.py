from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from app.models.entities import Position, Trade
from app.services.engine import EngineService
from app.strategies.trend_v1 import TradePlan


class FakeRedis:
    async def set(self, _key: str, _value: str) -> None:
        return None

    async def get(self, _key: str) -> str | None:
        return None


class FakeDB:
    def __init__(self) -> None:
        self.added = []

    def add(self, obj) -> None:
        self.added.append(obj)


def _plan() -> TradePlan:
    return TradePlan(
        decision='hold',
        side=None,
        regime='TREND_OK',
        score_total=0.0,
        score_components={},
        reasons=[],
        entry_price=None,
        stop_price=None,
        tp1_price=None,
        tp2_price=None,
        time_stop_bars=120,
        setup_name='test',
        strategy_name='test',
        leverage=1.0,
        qty=1.0,
        atr=1.0,
        regime_state='TREND_OK',
        regime_direction='BULL',
        regime_gate_ok=True,
        regime_gate_reasons=[],
        regime_gate_metrics={},
        blockers=[],
        active_mode='ACTIVE',
        mode_reasons=[],
        breakout_box_high=None,
        breakout_box_low=None,
        breakout_compression=False,
        breakout_recent=False,
        pullback_v2_ok=None,
        pullback_v2_reasons=[],
        base_qty=1.0,
        size_mult=1.0,
        final_qty=1.0,
        router_selected_strategy='trend',
        router_reason='test',
        router_reason_tags=[],
    )


@pytest.mark.asyncio
async def test_tp1_hit_arms_be_and_skips_partial_close(monkeypatch):
    engine = EngineService(redis_client=FakeRedis())
    db = FakeDB()

    trade = Trade(
        id=uuid4(),
        symbol='ETHUSDT',
        side='BUY',
        quantity=Decimal('1'),
        entry_price=Decimal('100'),
        stop_price=Decimal('95'),
        tp1_price=Decimal('101'),
        tp2_price=Decimal('110'),
        strategy_profile='TREND_STABLE',
        status='OPEN',
        tp1_be_armed=False,
    )
    position = Position(symbol='ETHUSDT', side='BUY', quantity=Decimal('1'), average_price=Decimal('100'), unrealized_pnl=Decimal('0'), is_open=True)

    partial_called = False

    async def _partial_close(*_args, **_kwargs):
        nonlocal partial_called
        partial_called = True

    async def _final_close(*_args, **_kwargs):
        raise AssertionError('final close should not trigger on TP1-only bar')

    monkeypatch.setattr(engine, '_partial_close', _partial_close)
    monkeypatch.setattr(engine, '_final_close', _final_close)
    monkeypatch.setattr('app.services.engine.settings.tp1_be_enabled', True)
    monkeypatch.setattr('app.services.engine.settings.tp1_be_offset', 0.0)

    action, reason, tags = await engine._evaluate_open_trade(
        db=db,
        trade=trade,
        position=position,
        high=Decimal('101.2'),
        low=Decimal('99.9'),
        close=Decimal('100.8'),
        ts_ms=1,
        ts_dt=datetime.now(timezone.utc),
        replay_clock=10,
        plan=_plan(),
    )

    assert partial_called is False
    assert action == 'HOLD'
    assert reason == 'tp1_be_arm'
    assert 'stop_moved_to_be' in tags
    assert trade.stop_price == Decimal('100')
    assert trade.tp1_be_armed is True
