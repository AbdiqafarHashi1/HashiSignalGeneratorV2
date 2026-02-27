from __future__ import annotations

from dataclasses import dataclass

from app.config import settings


@dataclass
class TradePlan:
    decision: str
    side: str | None
    regime: str
    score_total: float
    score_components: dict[str, float]
    reasons: list[str]
    entry_price: float | None
    stop_price: float | None
    tp1_price: float | None
    tp2_price: float | None
    time_stop_bars: int
    setup_name: str
    strategy_name: str
    leverage: float
    qty: float
    atr: float


class TrendPullbackStrategyV1:
    def __init__(self) -> None:
        self.name = 'strategy_v1_trend_pullback'
        self.setup = 'trend_pullback_continuation'

    @staticmethod
    def _ema(values: list[float], length: int) -> float:
        if not values:
            return 0.0
        length = max(1, int(length))
        alpha = 2.0 / (length + 1.0)
        ema = values[0]
        for value in values[1:]:
            ema = (alpha * value) + ((1.0 - alpha) * ema)
        return float(ema)

    @staticmethod
    def _atr(candles: list[dict], length: int) -> float:
        if len(candles) < 2:
            return 0.0
        trs: list[float] = []
        for idx in range(1, len(candles)):
            high = float(candles[idx]['high'])
            low = float(candles[idx]['low'])
            prev_close = float(candles[idx - 1]['close'])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        if not trs:
            return 0.0
        window = trs[-max(1, int(length)) :]
        return float(sum(window) / len(window))

    @staticmethod
    def _aggregate_htf(candles: list[dict], factor: int) -> list[dict]:
        factor = max(2, int(factor))
        out: list[dict] = []
        for i in range(0, len(candles), factor):
            chunk = candles[i : i + factor]
            if len(chunk) < factor:
                break
            out.append(
                {
                    'open': float(chunk[0]['open']),
                    'high': max(float(row['high']) for row in chunk),
                    'low': min(float(row['low']) for row in chunk),
                    'close': float(chunk[-1]['close']),
                    'timestamp': chunk[-1].get('timestamp'),
                }
            )
        return out

    @staticmethod
    def _timeframe_minutes(timeframe: str | None) -> int:
        if not timeframe:
            return 5
        raw = timeframe.strip().lower()
        try:
            if raw.endswith('m'):
                return max(1, int(raw[:-1]))
            if raw.endswith('h'):
                return max(1, int(raw[:-1])) * 60
        except Exception:
            return 5
        return 5

    def build_plan(self, candles: list[dict], timeframe: str | None) -> TradePlan:
        default_plan = TradePlan(
            decision='hold',
            side=None,
            regime='RANGING',
            score_total=0.0,
            score_components={'bias_align': 0.0, 'trend_strength_scaled': 0.0, 'pullback_quality': 0.0, 'confirmation_strength': 0.0},
            reasons=['insufficient_data'],
            entry_price=None,
            stop_price=None,
            tp1_price=None,
            tp2_price=None,
            time_stop_bars=settings.time_stop_bars,
            setup_name=self.setup,
            strategy_name=self.name,
            leverage=settings.leverage,
            qty=settings.replay_order_qty,
            atr=0.0,
        )
        if len(candles) < max(settings.ema_slow + 5, settings.atr_len + 5):
            return default_plan

        closes = [float(row['close']) for row in candles]
        base_ema_fast = self._ema(closes[-(settings.ema_slow + 50) :], settings.ema_fast)
        base_ema_slow = self._ema(closes[-(settings.ema_slow + 50) :], settings.ema_slow)
        atr = max(self._atr(candles[-(settings.atr_len + 20) :], settings.atr_len), 1e-9)
        trend_strength = abs(base_ema_fast - base_ema_slow) / atr
        regime = 'TRENDING' if trend_strength >= settings.regime_trend_min else 'RANGING'
        if regime != 'TRENDING':
            default_plan.reasons = ['regime_ranging']
            return default_plan

        base_minutes = self._timeframe_minutes(timeframe)
        htf_minutes = 15 if base_minutes <= 5 else 60
        htf_factor = max(2, round(htf_minutes / base_minutes))
        htf_candles = self._aggregate_htf(candles, htf_factor)
        if len(htf_candles) < settings.ema_slow + 5:
            default_plan.reasons = ['insufficient_htf_data']
            default_plan.regime = regime
            return default_plan
        htf_closes = [float(row['close']) for row in htf_candles]
        htf_ema_fast = self._ema(htf_closes[-(settings.ema_slow + 50) :], settings.ema_fast)
        htf_ema_slow = self._ema(htf_closes[-(settings.ema_slow + 50) :], settings.ema_slow)

        current = candles[-1]
        lookback = candles[-max(1, settings.pullback_lookback) :]
        current_open = float(current['open'])
        current_close = float(current['close'])
        current_high = float(current['high'])
        current_low = float(current['low'])

        long_bias = htf_ema_fast > htf_ema_slow and base_ema_fast > base_ema_slow
        short_bias = htf_ema_fast < htf_ema_slow and base_ema_fast < base_ema_slow
        long_pullback = any(float(row['low']) <= base_ema_fast for row in lookback)
        short_pullback = any(float(row['high']) >= base_ema_fast for row in lookback)
        long_confirm = current_close > base_ema_fast and current_close > current_open
        short_confirm = current_close < base_ema_fast and current_close < current_open

        side: str | None = None
        pullback_ok = False
        confirm_ok = False
        reasons: list[str] = []
        if long_bias and long_pullback and long_confirm:
            side = 'BUY'
            pullback_ok = True
            confirm_ok = True
            reasons.append('long_trend_pullback_confirmation')
        elif short_bias and short_pullback and short_confirm:
            side = 'SELL'
            pullback_ok = True
            confirm_ok = True
            reasons.append('short_trend_pullback_confirmation')
        else:
            reasons.append('setup_not_confirmed')

        trend_strength_scaled = max(0.0, min(1.0, trend_strength / max(settings.regime_trend_min * 2.0, 1e-9)))
        confirmation_strength = max(0.0, min(1.0, abs(current_close - current_open) / atr))
        components = {
            'bias_align': 1.0 if side else 0.0,
            'trend_strength_scaled': trend_strength_scaled,
            'pullback_quality': 1.0 if pullback_ok else 0.0,
            'confirmation_strength': confirmation_strength if confirm_ok else 0.0,
        }
        score_total = (sum(components.values()) / 4.0) * 100.0
        if not side or score_total < settings.score_min:
            if score_total < settings.score_min:
                reasons.append('score_below_min')
            return TradePlan(
                decision='hold',
                side=None,
                regime=regime,
                score_total=score_total,
                score_components=components,
                reasons=reasons,
                entry_price=None,
                stop_price=None,
                tp1_price=None,
                tp2_price=None,
                time_stop_bars=settings.time_stop_bars,
                setup_name=self.setup,
                strategy_name=self.name,
                leverage=settings.leverage,
                qty=settings.replay_order_qty,
                atr=atr,
            )

        entry_price = current_close  # Deterministic rule: fill at signal candle close.
        risk_r = atr
        if side == 'BUY':
            stop_price = entry_price - risk_r
            tp1 = entry_price + (settings.tp1_r_mult * risk_r)
            tp2 = entry_price + (settings.tp2_r_mult * risk_r)
            decision = 'enter_long'
        else:
            stop_price = entry_price + risk_r
            tp1 = entry_price - (settings.tp1_r_mult * risk_r)
            tp2 = entry_price - (settings.tp2_r_mult * risk_r)
            decision = 'enter_short'

        return TradePlan(
            decision=decision,
            side=side,
            regime=regime,
            score_total=score_total,
            score_components=components,
            reasons=reasons,
            entry_price=entry_price,
            stop_price=stop_price,
            tp1_price=tp1,
            tp2_price=tp2,
            time_stop_bars=settings.time_stop_bars,
            setup_name=self.setup,
            strategy_name=self.name,
            leverage=settings.leverage,
            qty=settings.replay_order_qty,
            atr=atr,
        )
