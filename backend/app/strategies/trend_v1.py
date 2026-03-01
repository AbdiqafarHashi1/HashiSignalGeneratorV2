from __future__ import annotations

from dataclasses import dataclass

from app.config import settings
from app.strategies.breakout import BreakoutSignal, detect_breakout_signal
from app.strategies.regime_gate import evaluate_regime_gate


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
    regime_state: str
    regime_direction: str
    regime_gate_ok: bool
    regime_gate_reasons: list[str]
    regime_gate_metrics: dict[str, float | None]
    blockers: list[str]
    active_mode: str
    mode_reasons: list[str]
    breakout_box_high: float | None
    breakout_box_low: float | None
    breakout_compression: bool
    breakout_recent: bool
    pullback_v2_ok: bool | None
    pullback_v2_reasons: list[str]
    base_qty: float
    size_mult: float
    final_qty: float


class TrendPullbackStrategyV1:
    def __init__(self) -> None:
        self.name = 'strategy_v1_trend_pullback'
        self.setup = 'trend_pullback_continuation'
        self._last_breakout_index: dict[str, int] = {}

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

    @staticmethod
    def _pivot_low(candles: list[dict], idx: int, length: int) -> bool:
        if idx - length < 0 or idx + length >= len(candles):
            return False
        target = float(candles[idx]['low'])
        for j in range(idx - length, idx + length + 1):
            if j == idx:
                continue
            if float(candles[j]['low']) <= target:
                return False
        return True

    @staticmethod
    def _pivot_high(candles: list[dict], idx: int, length: int) -> bool:
        if idx - length < 0 or idx + length >= len(candles):
            return False
        target = float(candles[idx]['high'])
        for j in range(idx - length, idx + length + 1):
            if j == idx:
                continue
            if float(candles[j]['high']) >= target:
                return False
        return True

    def _pullback_v2_check(self, candles: list[dict], side: str, atr: float, base_ema_fast: float) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        if len(candles) < max(settings.pullback_lookback + 20, settings.pb2_pivot_len * 4 + 10):
            return False, ['pb2_no_impulse']

        closes = [float(c['close']) for c in candles]
        highs = [float(c['high']) for c in candles]
        lows = [float(c['low']) for c in candles]
        now_close = closes[-1]
        lookback = max(settings.pullback_lookback, 8)
        atr_safe = max(atr, 1e-12)

        if side == 'BUY':
            swing_low = min(lows[-(lookback * 2) : -lookback] or lows[:-lookback] or [lows[0]])
            impulse = (max(highs[-lookback:]) - swing_low) / atr_safe
            retrace = (max(highs[-lookback:]) - now_close) / max(max(highs[-lookback:]) - swing_low, 1e-12)
            if impulse < settings.pb2_min_impulse_atr:
                reasons.append('pb2_no_impulse')
            if retrace < settings.pb2_min_retrace:
                reasons.append('pb2_retrace_too_shallow')
            if retrace > settings.pb2_max_retrace:
                reasons.append('pb2_retrace_too_deep')
            if settings.pb2_require_hl_lh:
                pivot_len = max(1, settings.pb2_pivot_len)
                pivots = [i for i in range(len(candles) - (pivot_len * 3), len(candles) - pivot_len) if self._pivot_low(candles, i, pivot_len)]
                if len(pivots) >= 2:
                    if float(candles[pivots[-1]]['low']) <= float(candles[pivots[-2]]['low']):
                        reasons.append('pb2_no_structure')
                else:
                    reasons.append('pb2_no_structure')
            if settings.pb2_confirm_bar:
                prev_high = float(candles[-2]['high'])
                if not (now_close > prev_high or now_close > base_ema_fast):
                    reasons.append('pb2_no_confirm')
        else:
            swing_high = max(highs[-(lookback * 2) : -lookback] or highs[:-lookback] or [highs[0]])
            impulse = (swing_high - min(lows[-lookback:])) / atr_safe
            retrace = (now_close - min(lows[-lookback:])) / max(swing_high - min(lows[-lookback:]), 1e-12)
            if impulse < settings.pb2_min_impulse_atr:
                reasons.append('pb2_no_impulse')
            if retrace < settings.pb2_min_retrace:
                reasons.append('pb2_retrace_too_shallow')
            if retrace > settings.pb2_max_retrace:
                reasons.append('pb2_retrace_too_deep')
            if settings.pb2_require_hl_lh:
                pivot_len = max(1, settings.pb2_pivot_len)
                pivots = [i for i in range(len(candles) - (pivot_len * 3), len(candles) - pivot_len) if self._pivot_high(candles, i, pivot_len)]
                if len(pivots) >= 2:
                    if float(candles[pivots[-1]]['high']) >= float(candles[pivots[-2]]['high']):
                        reasons.append('pb2_no_structure')
                else:
                    reasons.append('pb2_no_structure')
            if settings.pb2_confirm_bar:
                prev_low = float(candles[-2]['low'])
                if not (now_close < prev_low or now_close < base_ema_fast):
                    reasons.append('pb2_no_confirm')
        if reasons:
            return False, reasons
        return True, ['pb2_ok']

    def _apply_vol_sizing(self, *, base_qty: float, atr_pct: float, chop_ratio: float | None, strong_trend: bool) -> tuple[float, float, list[str]]:
        reasons = ['vol_sizing_on']
        low = float(settings.vol_sizing_atr_pct_low)
        high = max(low + 1e-9, float(settings.vol_sizing_atr_pct_high))
        # Higher realized volatility gets higher size multiplier in replay trend regime.
        vol_norm = max(0.0, min(1.0, (atr_pct - low) / (high - low)))
        size_mult = float(settings.vol_sizing_min_mult) + vol_norm * (float(settings.vol_sizing_max_mult) - float(settings.vol_sizing_min_mult))
        if vol_norm < 0.34:
            reasons.append('vol_low_penalty')
        elif vol_norm > 0.66:
            reasons.append('vol_high_boost')
        if chop_ratio is not None and chop_ratio < settings.chop_min_ratio:
            size_mult *= float(settings.vol_sizing_chop_penalty)
            reasons.append('chop_penalty')
        if strong_trend:
            size_mult *= float(settings.vol_sizing_trend_bonus)
            reasons.append('trend_bonus')
        if settings.vol_sizing_enable_cap:
            capped = max(float(settings.vol_sizing_min_mult), min(float(settings.vol_sizing_max_mult), size_mult))
            if abs(capped - size_mult) > 1e-12:
                reasons.append('vol_cap_applied')
            size_mult = capped
        final_qty = base_qty * size_mult
        return size_mult, final_qty, reasons

    def _baseline_plan(self, candles: list[dict], timeframe: str | None, pointer_index: int | None) -> TradePlan:
        default_plan = TradePlan(
            decision='hold',
            side=None,
            regime='RANGING',
            score_total=0.0,
            score_components={'bias_align': 0.0, 'trend_strength_scaled': 0.0, 'pullback_quality': 0.0, 'confirmation_strength': 0.0},
            reasons=['ltf_warmup'],
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
            regime_state='INSUFFICIENT_DATA',
            regime_direction='NEUTRAL',
            regime_gate_ok=False,
            regime_gate_reasons=['ltf_warmup'],
            regime_gate_metrics={},
            blockers=['regime_gate:ltf_warmup'],
            active_mode='BASELINE_TREND',
            mode_reasons=['baseline_mode'],
            breakout_box_high=None,
            breakout_box_low=None,
            breakout_compression=False,
            breakout_recent=False,
            pullback_v2_ok=None,
            pullback_v2_reasons=[],
            base_qty=float(settings.replay_order_qty),
            size_mult=1.0,
            final_qty=float(settings.replay_order_qty),
        )
        if len(candles) < max(settings.ema_slow + 5, settings.atr_len + 5):
            return default_plan

        closes = [float(row['close']) for row in candles]
        base_ema_fast = self._ema(closes[-(settings.ema_slow + 50) :], settings.ema_fast)
        base_ema_slow = self._ema(closes[-(settings.ema_slow + 50) :], settings.ema_slow)
        atr = max(self._atr(candles[-(settings.atr_len + 20) :], settings.atr_len), 1e-9)
        trend_strength = abs(base_ema_fast - base_ema_slow) / max(atr, 1e-9)

        base_minutes = self._timeframe_minutes(timeframe)
        htf_minutes = 15 if base_minutes <= 5 else 60
        htf_factor = max(2, round(htf_minutes / base_minutes))
        htf_candles = self._aggregate_htf(candles, htf_factor)
        htf_ema_fast = 0.0
        htf_ema_slow = 0.0
        if len(htf_candles) >= settings.ema_slow + 5:
            htf_closes = [float(row['close']) for row in htf_candles]
            htf_ema_fast = self._ema(htf_closes[-(settings.ema_slow + 50) :], settings.ema_fast)
            htf_ema_slow = self._ema(htf_closes[-(settings.ema_slow + 50) :], settings.ema_slow)
        gate = evaluate_regime_gate(
            base_candles=candles,
            htf_candles=htf_candles,
            base_ema_fast=base_ema_fast,
            base_ema_slow=base_ema_slow,
            base_atr=atr,
            htf_ema_fast=htf_ema_fast,
            htf_ema_slow=htf_ema_slow,
            pointer_index=pointer_index,
        )

        current = candles[-1]
        lookback = candles[-max(1, settings.pullback_lookback) :]
        current_open = float(current['open'])
        current_close = float(current['close'])

        long_bias = gate.regime_direction == 'BULL' and base_ema_fast > base_ema_slow
        short_bias = gate.regime_direction == 'BEAR' and base_ema_fast < base_ema_slow
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
        entry_allowed_by_gate = gate.gate_ok
        if side == 'BUY' and gate.regime_direction not in ('BULL',):
            entry_allowed_by_gate = False
            reasons.append('regime_direction_blocks_long')
        if side == 'SELL' and gate.regime_direction not in ('BEAR',):
            entry_allowed_by_gate = False
            reasons.append('regime_direction_blocks_short')

        blockers = [f'regime_gate:{code}' for code in gate.gate_reason_codes if code != 'htf_missing_fallback_used']
        if not side or score_total < settings.score_min or not entry_allowed_by_gate:
            if score_total < settings.score_min and side:
                reasons.append('score_below_min')
            reasons.extend([f'regime_gate:{code}' for code in gate.gate_reason_codes])
            return TradePlan(
                decision='hold',
                side=None,
                regime=gate.regime_state,
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
                regime_state=gate.regime_state,
                regime_direction=gate.regime_direction,
                regime_gate_ok=gate.gate_ok,
                regime_gate_reasons=gate.gate_reasons,
                regime_gate_metrics=gate.gate_metrics,
                blockers=blockers,
                active_mode='BASELINE_TREND',
                mode_reasons=['baseline_mode'],
                breakout_box_high=None,
                breakout_box_low=None,
                breakout_compression=False,
                breakout_recent=False,
                pullback_v2_ok=None,
                pullback_v2_reasons=[],
                base_qty=float(settings.replay_order_qty),
                size_mult=1.0,
                final_qty=float(settings.replay_order_qty),
            )

        entry_price = current_close
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
            regime=gate.regime_state,
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
            regime_state=gate.regime_state,
            regime_direction=gate.regime_direction,
            regime_gate_ok=gate.gate_ok,
            regime_gate_reasons=gate.gate_reasons,
            regime_gate_metrics=gate.gate_metrics,
            blockers=blockers,
            active_mode='BASELINE_TREND',
            mode_reasons=['baseline_mode'],
            breakout_box_high=None,
            breakout_box_low=None,
            breakout_compression=False,
            breakout_recent=False,
            pullback_v2_ok=None,
            pullback_v2_reasons=[],
            base_qty=float(settings.replay_order_qty),
            size_mult=1.0,
            final_qty=float(settings.replay_order_qty),
        )

    def build_plan(self, candles: list[dict], timeframe: str | None, pointer_index: int | None = None) -> TradePlan:
        # Hard baseline lock: with all features off this must remain identical to current baseline behavior.
        if not settings.feature_breakout and not settings.feature_pullback_v2 and not settings.feature_vol_sizing:
            return self._baseline_plan(candles, timeframe, pointer_index)

        base_plan = self._baseline_plan(candles, timeframe, pointer_index)
        atr = max(base_plan.atr, 1e-9)
        current_close = float(candles[-1]['close']) if candles else 0.0
        gate_metrics = dict(base_plan.regime_gate_metrics or {})
        atr_pct = float(gate_metrics.get('atr_pct') or ((atr / max(abs(current_close), 1e-12)) * 100.0))
        adx_or_chop = gate_metrics.get('chop_ratio') if gate_metrics.get('adx') is None else gate_metrics.get('adx')
        chop_ratio = float(gate_metrics.get('chop_ratio')) if gate_metrics.get('chop_ratio') is not None else None
        strong_trend = bool(base_plan.regime_gate_ok and base_plan.regime_state == 'TREND_OK')
        chop = (float(gate_metrics.get('adx') or 0) < settings.adx_min) if gate_metrics.get('adx') is not None else (chop_ratio is not None and chop_ratio < settings.chop_min_ratio)

        breakout = detect_breakout_signal(candles, atr, self._last_breakout_index.get('default'), pointer_index)
        if breakout.side and pointer_index is not None:
            self._last_breakout_index['default'] = int(pointer_index)
            breakout.breakout_recent = True

        mode_reasons: list[str] = []
        active_mode = 'BASELINE_TREND'
        if settings.feature_breakout and breakout.compression:
            active_mode = 'BREAKOUT_ONLY'
            mode_reasons.append('compression')
        elif strong_trend:
            active_mode = 'TREND_PULLBACK'
            mode_reasons.append('strong_trend')
            if settings.feature_breakout and breakout.breakout_recent:
                mode_reasons.append('recent_breakout_override')
        elif chop:
            active_mode = 'STAND_DOWN'
            mode_reasons.append('chop')

        plan = base_plan
        plan.active_mode = active_mode
        plan.mode_reasons = mode_reasons or ['baseline_mode']
        plan.breakout_box_high = breakout.box_high
        plan.breakout_box_low = breakout.box_low
        plan.breakout_compression = breakout.compression
        plan.breakout_recent = breakout.breakout_recent

        if active_mode == 'STAND_DOWN':
            plan.decision = 'hold'
            plan.side = None
            plan.reasons = list(dict.fromkeys(plan.reasons + ['router_stand_down']))

        if settings.feature_breakout and active_mode == 'BREAKOUT_ONLY':
            if breakout.side and breakout.entry_price and breakout.stop_price and breakout.tp1_price and breakout.tp2_price:
                plan.decision = 'enter_long' if breakout.side == 'BUY' else 'enter_short'
                plan.side = breakout.side
                plan.entry_price = breakout.entry_price
                plan.stop_price = breakout.stop_price
                plan.tp1_price = breakout.tp1_price
                plan.tp2_price = breakout.tp2_price
                plan.strategy_name = f'{self.name}_breakout'
                plan.setup_name = 'breakout_compression'
                plan.reasons = list(dict.fromkeys(plan.reasons + breakout.mode_reasons + ['breakout_route']))
            else:
                plan.decision = 'hold'
                plan.side = None
                plan.reasons = list(dict.fromkeys(plan.reasons + breakout.blockers + ['breakout_no_trigger']))

        if settings.feature_pullback_v2 and active_mode == 'TREND_PULLBACK' and plan.side in ('BUY', 'SELL'):
            pb2_ok, pb2_reasons = self._pullback_v2_check(candles, plan.side, atr, self._ema([float(c['close']) for c in candles[-(settings.ema_slow + 50) :]], settings.ema_fast))
            plan.pullback_v2_ok = pb2_ok
            plan.pullback_v2_reasons = pb2_reasons
            if not pb2_ok:
                plan.decision = 'hold'
                plan.side = None
                plan.reasons = list(dict.fromkeys(plan.reasons + pb2_reasons))
            else:
                plan.reasons = list(dict.fromkeys(plan.reasons + pb2_reasons))
        else:
            plan.pullback_v2_ok = None
            plan.pullback_v2_reasons = []

        base_qty = float(settings.replay_order_qty)
        size_mult = 1.0
        final_qty = base_qty
        if settings.feature_vol_sizing:
            size_mult, final_qty, vol_reasons = self._apply_vol_sizing(
                base_qty=base_qty,
                atr_pct=atr_pct,
                chop_ratio=chop_ratio if isinstance(adx_or_chop, float) else chop_ratio,
                strong_trend=strong_trend,
            )
            plan.reasons = list(dict.fromkeys(plan.reasons + vol_reasons))
        plan.base_qty = base_qty
        plan.size_mult = size_mult
        plan.final_qty = final_qty
        plan.qty = final_qty

        return plan
