from app.strategies.regime_gate import RegimeGateResult
from app.strategies.trend_v1 import TrendPullbackStrategyV1


def _candles(n: int = 320) -> list[dict]:
    out = []
    px = 100.0
    for i in range(n):
        px += 0.3
        out.append({'open': px - 0.2, 'high': px + 0.3, 'low': px - 0.4, 'close': px, 'volume': 1.0, 'timestamp': i})
    return out


def test_router_forces_pb2_in_strong_trend(monkeypatch):
    strategy = TrendPullbackStrategyV1()

    def fake_gate(**_kwargs):
        return RegimeGateResult(
            regime_state='TREND_OK',
            regime_direction='BULL',
            gate_ok=True,
            gate_reason_codes=[],
            gate_reasons=[],
            gate_metrics={'trend_strength': 2.0, 'adx': 35.0, 'chop_ratio': 0.9, 'atr_pct': 0.3},
        )

    monkeypatch.setattr('app.strategies.trend_v1.evaluate_regime_gate', fake_gate)
    monkeypatch.setattr('app.strategies.trend_v1.settings.feature_breakout', True)
    monkeypatch.setattr('app.strategies.trend_v1.settings.feature_pullback_v2', False)
    monkeypatch.setattr('app.strategies.trend_v1.settings.feature_vol_sizing', False)

    plan = strategy.build_plan(_candles(), timeframe='15m', pointer_index=300)

    assert plan.router_selected_strategy == 'pb2'
    assert plan.router_reason == 'forced_strong_trend'
    assert 'forced_strong_trend' in plan.router_reason_tags
