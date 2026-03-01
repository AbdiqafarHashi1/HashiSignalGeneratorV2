#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]
BACKEND_ROOT = ROOT / "backend"
DEFAULT_CSV = ROOT / "data" / "datasets" / "ETHUSDT_15m.csv"
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


@dataclass
class PositionState:
    side: str
    qty: float
    entry_price: float
    stop_price: float
    tp1_price: float
    tp2_price: float
    fee_entry: float
    fee_exit: float
    pnl_gross: float
    entry_idx: int
    entry_ts: str
    tp1_done: bool
    time_stop_bars: int


def _load_rows(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    out: list[dict] = []
    for row in rows:
        ts_raw = str(row.get("timestamp", "")).strip()
        ts_ms = int(float(ts_raw))
        out.append(
            {
                "timestamp": datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
            }
        )
    return out


def _gross_delta(side: str, entry: float, exit_price: float, qty: float) -> float:
    if side == "BUY":
        return (exit_price - entry) * qty
    return (entry - exit_price) * qty


def run_once(csv_path: Path) -> dict:
    from app.config import settings
    from app.strategies.trend_v1 import TrendPullbackStrategyV1

    rows = _load_rows(csv_path)
    strategy = TrendPullbackStrategyV1()
    history: list[dict] = []
    pos: PositionState | None = None
    trades: list[dict] = []
    fee_rate = float(settings.taker_fee_rate)

    for i, bar in enumerate(rows):
        history.append(bar)
        plan = strategy.build_plan(history, "15m", i)
        high = bar["high"]
        low = bar["low"]
        close = bar["close"]
        ts = bar["timestamp"]

        if pos is not None:
            stop_hit = (low <= pos.stop_price) if pos.side == "BUY" else (high >= pos.stop_price)
            tp1_hit = (high >= pos.tp1_price) if pos.side == "BUY" else (low <= pos.tp1_price)
            tp2_hit = (high >= pos.tp2_price) if pos.side == "BUY" else (low <= pos.tp2_price)
            bars_held = i - pos.entry_idx
            if stop_hit:
                fee = abs(pos.qty * pos.stop_price * fee_rate)
                gross = _gross_delta(pos.side, pos.entry_price, pos.stop_price, pos.qty)
                pos.fee_exit += fee
                pos.pnl_gross += gross
                pnl_net = pos.pnl_gross - (pos.fee_entry + pos.fee_exit)
                trades.append({"side": pos.side, "entry_ts": pos.entry_ts, "exit_ts": ts, "entry_px": pos.entry_price, "exit_px": pos.stop_price, "reason": "sl_close", "fees": pos.fee_entry + pos.fee_exit, "pnl_net": pnl_net})
                pos = None
                continue
            if tp2_hit:
                if not pos.tp1_done:
                    q1 = pos.qty * float(settings.partial_pct)
                    fee1 = abs(q1 * pos.tp1_price * fee_rate)
                    gross1 = _gross_delta(pos.side, pos.entry_price, pos.tp1_price, q1)
                    pos.fee_exit += fee1
                    pos.pnl_gross += gross1
                    pos.qty -= q1
                    pos.tp1_done = True
                fee2 = abs(pos.qty * pos.tp2_price * fee_rate)
                gross2 = _gross_delta(pos.side, pos.entry_price, pos.tp2_price, pos.qty)
                pos.fee_exit += fee2
                pos.pnl_gross += gross2
                pnl_net = pos.pnl_gross - (pos.fee_entry + pos.fee_exit)
                trades.append({"side": pos.side, "entry_ts": pos.entry_ts, "exit_ts": ts, "entry_px": pos.entry_price, "exit_px": pos.tp2_price, "reason": "tp_close", "fees": pos.fee_entry + pos.fee_exit, "pnl_net": pnl_net})
                pos = None
                continue
            if tp1_hit and not pos.tp1_done:
                q1 = pos.qty * float(settings.partial_pct)
                fee1 = abs(q1 * pos.tp1_price * fee_rate)
                gross1 = _gross_delta(pos.side, pos.entry_price, pos.tp1_price, q1)
                pos.fee_exit += fee1
                pos.pnl_gross += gross1
                pos.qty -= q1
                pos.tp1_done = True
            if bars_held >= int(plan.time_stop_bars):
                fee = abs(pos.qty * close * fee_rate)
                gross = _gross_delta(pos.side, pos.entry_price, close, pos.qty)
                pos.fee_exit += fee
                pos.pnl_gross += gross
                pnl_net = pos.pnl_gross - (pos.fee_entry + pos.fee_exit)
                trades.append({"side": pos.side, "entry_ts": pos.entry_ts, "exit_ts": ts, "entry_px": pos.entry_price, "exit_px": close, "reason": "time_close", "fees": pos.fee_entry + pos.fee_exit, "pnl_net": pnl_net})
                pos = None
                continue
            opposite = (pos.side == "BUY" and plan.side == "SELL") or (pos.side == "SELL" and plan.side == "BUY")
            if opposite and plan.score_total >= float(settings.score_min):
                fee = abs(pos.qty * close * fee_rate)
                gross = _gross_delta(pos.side, pos.entry_price, close, pos.qty)
                pos.fee_exit += fee
                pos.pnl_gross += gross
                pnl_net = pos.pnl_gross - (pos.fee_entry + pos.fee_exit)
                trades.append({"side": pos.side, "entry_ts": pos.entry_ts, "exit_ts": ts, "entry_px": pos.entry_price, "exit_px": close, "reason": "signal_close", "fees": pos.fee_entry + pos.fee_exit, "pnl_net": pnl_net})
                pos = None
                continue

        if pos is None and plan.side in ("BUY", "SELL") and plan.entry_price is not None and plan.stop_price is not None and plan.tp1_price is not None and plan.tp2_price is not None:
            qty = float(plan.qty)
            fee = abs(qty * float(plan.entry_price) * fee_rate)
            pos = PositionState(
                side=str(plan.side),
                qty=qty,
                entry_price=float(plan.entry_price),
                stop_price=float(plan.stop_price),
                tp1_price=float(plan.tp1_price),
                tp2_price=float(plan.tp2_price),
                fee_entry=fee,
                fee_exit=0.0,
                pnl_gross=0.0,
                entry_idx=i,
                entry_ts=ts,
                tp1_done=False,
                time_stop_bars=int(plan.time_stop_bars),
            )

    total_pnl = sum(t["pnl_net"] for t in trades)
    total_fees = sum(t["fees"] for t in trades)
    wins = sum(1 for t in trades if t["pnl_net"] > 0)
    win_rate = (wins / len(trades) * 100.0) if trades else 0.0
    signature_rows = [f'{t["side"]}|{t["entry_ts"]}|{t["exit_ts"]}|{t["entry_px"]:.6f}|{t["exit_px"]:.6f}|{t["reason"]}' for t in trades]
    signature = hashlib.sha256("\n".join(signature_rows).encode("utf-8")).hexdigest()
    return {
        "trade_count": len(trades),
        "total_net_pnl": round(total_pnl, 8),
        "total_fees": round(total_fees, 8),
        "win_rate": round(win_rate, 8),
        "trade_hash": signature,
    }


def _run_subprocess(csv_path: Path, flags: dict[str, str]) -> dict:
    env = os.environ.copy()
    env.update(flags)
    env["PYTHONPATH"] = str(BACKEND_ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    cmd = [sys.executable, str(Path(__file__).resolve()), "--single", "--csv", str(csv_path)]
    res = subprocess.run(cmd, env=env, cwd=str(ROOT), capture_output=True, text=True, check=True)
    return json.loads(res.stdout.strip())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--single", action="store_true")
    parser.add_argument("--csv", default=str(DEFAULT_CSV))
    args = parser.parse_args()
    csv_path = Path(args.csv).resolve()
    if args.single:
        print(json.dumps(run_once(csv_path)))
        return 0

    baseline_flags = {"FEATURE_BREAKOUT": "false", "FEATURE_PULLBACK_V2": "false", "FEATURE_VOL_SIZING": "false"}
    baseline_a = _run_subprocess(csv_path, baseline_flags)
    baseline_b = _run_subprocess(csv_path, baseline_flags)
    assert baseline_a == baseline_b, f"Baseline mismatch between runs: {baseline_a} vs {baseline_b}"

    scenarios = {
        "baseline_off": baseline_flags,
        "breakout_only": {"FEATURE_BREAKOUT": "true", "FEATURE_PULLBACK_V2": "false", "FEATURE_VOL_SIZING": "false"},
        "pullback_v2_only": {"FEATURE_BREAKOUT": "false", "FEATURE_PULLBACK_V2": "true", "FEATURE_VOL_SIZING": "false"},
        "vol_sizing_only": {"FEATURE_BREAKOUT": "false", "FEATURE_PULLBACK_V2": "false", "FEATURE_VOL_SIZING": "true"},
        "all_enabled": {"FEATURE_BREAKOUT": "true", "FEATURE_PULLBACK_V2": "true", "FEATURE_VOL_SIZING": "true"},
    }
    results: dict[str, dict] = {}
    for name, flags in scenarios.items():
        results[name] = _run_subprocess(csv_path, flags)
    assert results["baseline_off"] == baseline_a, "baseline_off fingerprint changed from deterministic baseline"

    print("strategy feature smoke summary")
    for name, result in results.items():
        print(name, result)
    print("baseline_equality=OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
