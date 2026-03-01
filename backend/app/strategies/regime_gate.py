from __future__ import annotations

from dataclasses import dataclass
import logging
from statistics import median

from app.config import settings

logger = logging.getLogger(__name__)


@dataclass
class RegimeGateResult:
    regime_state: str
    regime_direction: str
    gate_ok: bool
    gate_reason_codes: list[str]
    gate_reasons: list[str]
    gate_metrics: dict[str, float | None]


def _ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    length = max(1, int(length))
    alpha = 2.0 / (length + 1.0)
    ema = values[0]
    for value in values[1:]:
        ema = (alpha * value) + ((1.0 - alpha) * ema)
    return float(ema)


def _ema_series(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    length = max(1, int(length))
    alpha = 2.0 / (length + 1.0)
    out: list[float] = []
    ema = values[0]
    out.append(float(ema))
    for value in values[1:]:
        ema = (alpha * value) + ((1.0 - alpha) * ema)
        out.append(float(ema))
    return out


def _atr_series(candles: list[dict], length: int) -> list[float]:
    if len(candles) < 2:
        return []
    trs: list[float] = []
    for idx in range(1, len(candles)):
        high = float(candles[idx]['high'])
        low = float(candles[idx]['low'])
        prev_close = float(candles[idx - 1]['close'])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(float(tr))
    if not trs:
        return []
    length = max(1, int(length))
    out: list[float] = []
    for idx in range(len(trs)):
        window = trs[max(0, idx - length + 1) : idx + 1]
        out.append(float(sum(window) / len(window)))
    return out


def _adx(candles: list[dict], length: int) -> float | None:
    # Wilder-style ADX approximation; deterministic and dependency-free.
    if len(candles) < (length * 2 + 2):
        return None
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    tr: list[float] = []
    for idx in range(1, len(candles)):
        high = float(candles[idx]['high'])
        low = float(candles[idx]['low'])
        prev_high = float(candles[idx - 1]['high'])
        prev_low = float(candles[idx - 1]['low'])
        prev_close = float(candles[idx - 1]['close'])

        up_move = high - prev_high
        down_move = prev_low - low
        plus_dm.append(up_move if up_move > down_move and up_move > 0 else 0.0)
        minus_dm.append(down_move if down_move > up_move and down_move > 0 else 0.0)
        tr.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))

    if len(tr) < length:
        return None

    def smooth(values: list[float], period: int) -> list[float]:
        if len(values) < period:
            return []
        out: list[float] = []
        first = sum(values[:period])
        out.append(first)
        prev = first
        for v in values[period:]:
            prev = prev - (prev / period) + v
            out.append(prev)
        return out

    tr_s = smooth(tr, length)
    plus_s = smooth(plus_dm, length)
    minus_s = smooth(minus_dm, length)
    if not tr_s or not plus_s or not minus_s:
        return None

    min_len = min(len(tr_s), len(plus_s), len(minus_s))
    tr_s = tr_s[:min_len]
    plus_s = plus_s[:min_len]
    minus_s = minus_s[:min_len]

    dx_vals: list[float] = []
    for i in range(min_len):
        if tr_s[i] <= 0:
            continue
        plus_di = 100.0 * (plus_s[i] / tr_s[i])
        minus_di = 100.0 * (minus_s[i] / tr_s[i])
        denom = plus_di + minus_di
        if denom <= 0:
            continue
        dx_vals.append(100.0 * abs(plus_di - minus_di) / denom)
    if len(dx_vals) < length:
        return None
    return float(sum(dx_vals[-length:]) / length)


def evaluate_regime_gate(
    *,
    base_candles: list[dict],
    htf_candles: list[dict],
    base_ema_fast: float,
    base_ema_slow: float,
    base_atr: float,
    htf_ema_fast: float,
    htf_ema_slow: float,
    pointer_index: int | None = None,
) -> RegimeGateResult:
    max_indicator_lookback = max(
        settings.ema_slow + 5,
        settings.atr_regime_lookback + settings.atr_len + 2,
        settings.chop_lookback + 2,
        settings.adx_len * 2 + 2,
    )
    warmup_threshold = int(settings.regime_min_ltf_bars) + max_indicator_lookback
    pointer_bars = (int(pointer_index) + 1) if pointer_index is not None else 0
    effective_bars = max(len(base_candles), pointer_bars)
    ltf_required = max(warmup_threshold, max_indicator_lookback)

    logger.debug(
        "regime_gate warmup check pointer=%s len_df=%s required_bars=%s warmup_threshold=%s effective_bars=%s",
        pointer_index,
        len(base_candles),
        ltf_required,
        warmup_threshold,
        effective_bars,
    )

    if effective_bars < ltf_required:
        remaining = max(0, ltf_required - effective_bars)
        return RegimeGateResult(
            regime_state='INSUFFICIENT_DATA',
            regime_direction='NEUTRAL',
            gate_ok=False,
            gate_reason_codes=['ltf_warmup'],
            gate_reasons=[f'ltf_warmup_need_{remaining}_more_bars'],
            gate_metrics={
                'trend_strength': None,
                'atr_pct': None,
                'adx': None,
                'chop_ratio': None,
                'atr_expand_ratio': None,
                'htf_slope_pct': None,
                'ltf_bars_have': float(effective_bars),
                'ltf_bars_min': float(ltf_required),
                'ltf_bars_remaining': float(remaining),
            },
        )

    close_now = float(base_candles[-1]['close'])
    atr = max(float(base_atr), 1e-12)
    trend_strength = abs(float(base_ema_fast) - float(base_ema_slow)) / atr
    atr_pct = (atr / max(abs(close_now), 1e-12)) * 100.0

    lookback = max(1, int(settings.htf_slope_lookback_bars))
    htf_slope_pct: float | None = None
    if htf_candles:
        htf_closes = [float(c['close']) for c in htf_candles]
        htf_slow_series = _ema_series(htf_closes, settings.ema_slow)
        if len(htf_slow_series) > lookback:
            prev = htf_slow_series[-1 - lookback]
            htf_slope_pct = (htf_slow_series[-1] - prev) / max(abs(prev), 1e-12)

    blocking_reason_codes: list[str] = []
    blocking_reasons: list[str] = []
    info_reason_codes: list[str] = []
    info_reasons: list[str] = []

    htf_missing = htf_slope_pct is None
    if htf_missing:
        if bool(settings.regime_require_htf) and not bool(settings.regime_allow_ltf_fallback):
            return RegimeGateResult(
                regime_state='INSUFFICIENT_DATA',
                regime_direction='NEUTRAL',
                gate_ok=False,
                gate_reason_codes=['htf_missing'],
                gate_reasons=['htf_missing_no_fallback'],
                gate_metrics={
                    'trend_strength': float(trend_strength),
                    'atr_pct': float(atr_pct),
                    'adx': None,
                    'chop_ratio': None,
                    'atr_expand_ratio': None,
                    'htf_slope_pct': None,
                    'ltf_bars_have': float(effective_bars),
                    'ltf_bars_min': float(ltf_required),
                    'ltf_bars_remaining': 0.0,
                },
            )
        info_reason_codes.append('htf_missing_fallback_used')
        info_reasons.append('htf_missing_fallback_used')

    direction = 'NEUTRAL'
    if htf_slope_pct is not None:
        if htf_ema_fast > htf_ema_slow and htf_slope_pct >= settings.htf_slope_min_pct:
            direction = 'BULL'
        elif htf_ema_fast < htf_ema_slow and htf_slope_pct <= -settings.htf_slope_min_pct:
            direction = 'BEAR'
    else:
        # LTF fallback direction when HTF slope is unavailable.
        if base_ema_fast > base_ema_slow:
            direction = 'BULL'
        elif base_ema_fast < base_ema_slow:
            direction = 'BEAR'

    atr_vals = _atr_series(base_candles[-(settings.atr_regime_lookback + settings.atr_len + 5) :], settings.atr_len)
    atr_expand_ratio: float | None = None
    if len(atr_vals) >= max(5, settings.atr_regime_lookback):
        hist = atr_vals[-settings.atr_regime_lookback :]
        baseline = float(median(hist)) if hist else 0.0
        atr_expand_ratio = float(atr_vals[-1] / max(baseline, 1e-12))

    adx_val = _adx(base_candles[-(settings.adx_len * 4 + 20) :], settings.adx_len)
    chop_ratio: float | None = None
    if adx_val is None:
        look = base_candles[-max(2, settings.chop_lookback) :]
        hh = max(float(c['high']) for c in look)
        ll = min(float(c['low']) for c in look)
        range_pct = ((hh - ll) / max(abs(close_now), 1e-12)) * 100.0
        atr_sum_proxy = atr_pct * len(look)
        chop_ratio = range_pct / max(atr_sum_proxy, 1e-12)

    if direction == 'NEUTRAL':
        blocking_reason_codes.append('direction_neutral')
        blocking_reasons.append('direction_neutral')
    if trend_strength < settings.regime_trend_min:
        blocking_reason_codes.append('trend_strength_below_min')
        blocking_reasons.append('trend_strength_below_min')
    if atr_pct < settings.atr_pct_min:
        blocking_reason_codes.append('atr_pct_below_min')
        blocking_reasons.append('atr_pct_below_min')
    if adx_val is not None:
        if adx_val < settings.adx_min:
            blocking_reason_codes.append('adx_below_min')
            blocking_reasons.append('adx_below_min')
    elif chop_ratio is not None and chop_ratio < settings.chop_min_ratio:
        blocking_reason_codes.append('chop_ratio_below_min')
        blocking_reasons.append('chop_ratio_below_min')
    if atr_expand_ratio is None or atr_expand_ratio < settings.atr_expand_min:
        blocking_reason_codes.append('atr_expand_below_min')
        blocking_reasons.append('atr_expand_below_min')

    gate_ok = len(blocking_reason_codes) == 0
    if gate_ok:
        regime_state = 'TREND_OK'
    elif 'ltf_warmup' in blocking_reason_codes or 'htf_missing' in blocking_reason_codes:
        regime_state = 'INSUFFICIENT_DATA'
    elif 'atr_pct_below_min' in blocking_reason_codes:
        regime_state = 'LOW_VOL'
    else:
        regime_state = 'RANGING'

    reason_codes = blocking_reason_codes + info_reason_codes
    reasons = blocking_reasons + info_reasons

    return RegimeGateResult(
        regime_state=regime_state,
        regime_direction=direction,
        gate_ok=gate_ok,
        gate_reason_codes=reason_codes,
        gate_reasons=reasons,
        gate_metrics={
            'trend_strength': float(trend_strength),
            'atr_pct': float(atr_pct),
            'adx': float(adx_val) if adx_val is not None else None,
            'chop_ratio': float(chop_ratio) if chop_ratio is not None else None,
            'atr_expand_ratio': float(atr_expand_ratio) if atr_expand_ratio is not None else None,
            'htf_slope_pct': float(htf_slope_pct) if htf_slope_pct is not None else None,
            'ltf_bars_have': float(effective_bars),
            'ltf_bars_min': float(ltf_required),
            'ltf_bars_remaining': 0.0,
        },
    )
