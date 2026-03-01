from __future__ import annotations

from dataclasses import dataclass

from app.config import settings


@dataclass
class BreakoutSignal:
    compression: bool
    breakout_recent: bool
    side: str | None
    entry_price: float | None
    stop_price: float | None
    tp1_price: float | None
    tp2_price: float | None
    box_high: float | None
    box_low: float | None
    mode_reasons: list[str]
    blockers: list[str]


def detect_breakout_signal(candles: list[dict], atr: float, last_breakout_index: int | None, pointer_index: int | None) -> BreakoutSignal:
    if len(candles) < max(5, settings.brk_lookback):
        return BreakoutSignal(
            compression=False,
            breakout_recent=False,
            side=None,
            entry_price=None,
            stop_price=None,
            tp1_price=None,
            tp2_price=None,
            box_high=None,
            box_low=None,
            mode_reasons=[],
            blockers=['breakout_insufficient_data'],
        )

    lookback = candles[-settings.brk_lookback :]
    highs = [float(c['high']) for c in lookback]
    lows = [float(c['low']) for c in lookback]
    closes = [float(c['close']) for c in lookback]
    box_high = max(highs)
    box_low = min(lows)
    box_range = max(0.0, box_high - box_low)
    atr_safe = max(float(atr), 1e-12)

    bars_in_box = sum(1 for c in closes if box_low <= c <= box_high)
    compression = box_range <= (settings.brk_max_range_atr * atr_safe) and bars_in_box >= settings.brk_min_bars_in_box
    mode_reasons: list[str] = []
    blockers: list[str] = []
    if compression:
        mode_reasons.append('compression')
        mode_reasons.append('breakout_compression')
    else:
        blockers.append('breakout_no_compression')

    current = candles[-1]
    close = float(current['close'])
    high = float(current['high'])
    low = float(current['low'])
    buffer = float(settings.brk_entry_buffer_atr) * atr_safe
    long_break = close > (box_high + buffer) if settings.brk_confirm_close else high > (box_high + buffer)
    short_break = close < (box_low - buffer) if settings.brk_confirm_close else low < (box_low - buffer)

    side: str | None = None
    entry_price: float | None = None
    stop_price: float | None = None
    tp1_price: float | None = None
    tp2_price: float | None = None
    if compression and long_break:
        side = 'BUY'
        entry_price = close
        stop_price = min(box_low - buffer, entry_price - settings.brk_stop_atr * atr_safe)
        risk = max(1e-12, entry_price - stop_price)
        tp1_price = entry_price + settings.brk_tp1_r * risk
        tp2_price = entry_price + settings.brk_tp2_r * risk
        mode_reasons.append('breakout_confirmed')
    elif compression and short_break:
        side = 'SELL'
        entry_price = close
        stop_price = max(box_high + buffer, entry_price + settings.brk_stop_atr * atr_safe)
        risk = max(1e-12, stop_price - entry_price)
        tp1_price = entry_price - settings.brk_tp1_r * risk
        tp2_price = entry_price - settings.brk_tp2_r * risk
        mode_reasons.append('breakout_confirmed')

    breakout_recent = False
    if last_breakout_index is not None and pointer_index is not None:
        breakout_recent = (pointer_index - last_breakout_index) <= int(settings.brk_recent_window)
        if breakout_recent:
            mode_reasons.append('breakout_recent')

    return BreakoutSignal(
        compression=bool(compression),
        breakout_recent=bool(breakout_recent),
        side=side,
        entry_price=entry_price,
        stop_price=stop_price,
        tp1_price=tp1_price,
        tp2_price=tp2_price,
        box_high=box_high,
        box_low=box_low,
        mode_reasons=mode_reasons,
        blockers=blockers,
    )
