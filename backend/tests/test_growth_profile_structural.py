from app.strategies.trend_v1 import TradePlan, TrendPullbackStrategyV1


def _candles(n: int = 320) -> list[dict]:
    out = []
    px = 100.0
    for i in range(n):
        px += 0.2
        out.append({'open': px - 0.2, 'high': px + 0.3, 'low': px - 0.4, 'close': px, 'volume': 1.0, 'timestamp': i})
    return out


def test_growth_profile_uses_structural_router(monkeypatch):
    strategy = TrendPullbackStrategyV1()
    monkeypatch.setattr('app.strategies.trend_v1.settings.active_profile', 'GROWTH_HUNTER')

    plan = strategy.build_plan(_candles(), timeframe='15m', pointer_index=200)

    assert plan.active_mode == 'GROWTH_STRUCTURAL_SWING'
    assert plan.router_selected_strategy == 'growth_structural'
    assert plan.decision == 'hold'


def test_growth_profile_returns_structural_plan_when_available(monkeypatch):
    strategy = TrendPullbackStrategyV1()
    monkeypatch.setattr('app.strategies.trend_v1.settings.active_profile', 'GROWTH_HUNTER')

    expected = TradePlan(
        decision='enter_long',
        side='BUY',
        regime='TREND_OK',
        score_total=100.0,
        score_components={},
        reasons=['growth_structural'],
        entry_price=101.0,
        stop_price=99.0,
        tp1_price=None,
        tp2_price=107.0,
        time_stop_bars=120,
        setup_name='growth_structural_swing',
        strategy_name='strategy_v1_trend_pullback_growth_structural',
        leverage=1.0,
        qty=1.0,
        atr=1.0,
        regime_state='TREND_OK',
        regime_direction='BULL',
        regime_gate_ok=True,
        regime_gate_reasons=[],
        regime_gate_metrics={},
        blockers=[],
        active_mode='GROWTH_STRUCTURAL_SWING',
        mode_reasons=['growth_structural_mode'],
        breakout_box_high=None,
        breakout_box_low=None,
        breakout_compression=False,
        breakout_recent=False,
        pullback_v2_ok=None,
        pullback_v2_reasons=[],
        base_qty=1.0,
        size_mult=1.0,
        final_qty=1.0,
        router_selected_strategy='growth_structural',
        router_reason='growth_structural',
        router_reason_tags=['growth_structural'],
        risk_pct_used=0.025,
        stop_distance=2.0,
        target_price=107.0,
        r_multiple=3.0,
    )

    monkeypatch.setattr(strategy, '_growth_structural_setup', lambda **_kwargs: expected)
    plan = strategy.build_plan(_candles(), timeframe='15m', pointer_index=200)
    assert plan is expected
