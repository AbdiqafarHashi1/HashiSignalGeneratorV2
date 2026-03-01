from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Iterable


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "uploads" / "ETHUSDT_5m_master.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "datasets" / "ETHUSDT_15m.csv"

TS_HEADERS = ("timestamp", "ts", "time", "open_time")
OPEN_HEADERS = ("open",)
HIGH_HEADERS = ("high",)
LOW_HEADERS = ("low",)
CLOSE_HEADERS = ("close",)
VOL_HEADERS = ("volume", "vol")


@dataclass(frozen=True)
class Candle5m:
    timestamp_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def _canonical_headers(headers: Iterable[str]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for raw in headers:
        normalized = raw.strip().lower()
        if normalized:
            mapping[normalized] = raw
    return mapping


def _find_header(header_map: dict[str, str], candidates: tuple[str, ...], field_name: str) -> str:
    for name in candidates:
        if name in header_map:
            return header_map[name]
    raise ValueError(f"missing required column for {field_name}; expected one of: {', '.join(candidates)}")


def _parse_timestamp_ms(value: str) -> int:
    raw = (value or "").strip()
    if not raw:
        raise ValueError("empty timestamp value")

    # Numeric timestamps (seconds/ms/us)
    try:
        num = float(raw)
        if num.is_integer():
            n = int(num)
            abs_n = abs(n)
            if abs_n >= 10**15:  # microseconds
                return n // 1000
            if abs_n >= 10**12:  # milliseconds
                return n
            if abs_n >= 10**9:  # seconds
                return n * 1000
    except ValueError:
        pass

    iso = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(iso)
    except ValueError as exc:
        raise ValueError(f"unsupported timestamp format: {raw}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _floor_15m_ms(timestamp_ms: int) -> int:
    bucket_ms = 15 * 60 * 1000
    return (timestamp_ms // bucket_ms) * bucket_ms


def load_5m_rows(path: Path) -> list[Candle5m]:
    if not path.exists():
        raise FileNotFoundError(f"input CSV not found: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            raise ValueError("CSV is missing headers")

        header_map = _canonical_headers(reader.fieldnames)
        ts_col = _find_header(header_map, TS_HEADERS, "timestamp")
        open_col = _find_header(header_map, OPEN_HEADERS, "open")
        high_col = _find_header(header_map, HIGH_HEADERS, "high")
        low_col = _find_header(header_map, LOW_HEADERS, "low")
        close_col = _find_header(header_map, CLOSE_HEADERS, "close")
        vol_col = _find_header(header_map, VOL_HEADERS, "volume")

        rows: list[Candle5m] = []
        for idx, row in enumerate(reader, start=2):
            try:
                candle = Candle5m(
                    timestamp_ms=_parse_timestamp_ms(str(row.get(ts_col, ""))),
                    open=float(row.get(open_col, "")),
                    high=float(row.get(high_col, "")),
                    low=float(row.get(low_col, "")),
                    close=float(row.get(close_col, "")),
                    volume=float(row.get(vol_col, "")),
                )
            except Exception as exc:
                raise ValueError(f"invalid row at line {idx}: {exc}") from exc
            rows.append(candle)

    if not rows:
        raise ValueError("input CSV has no rows")

    rows.sort(key=lambda r: r.timestamp_ms)
    return rows


def resample_15m(rows_5m: list[Candle5m]) -> list[dict[str, float | int]]:
    grouped: dict[int, list[Candle5m]] = {}
    for row in rows_5m:
        bucket = _floor_15m_ms(row.timestamp_ms)
        grouped.setdefault(bucket, []).append(row)

    out: list[dict[str, float | int]] = []
    for bucket_ts in sorted(grouped.keys()):
        bucket_rows = sorted(grouped[bucket_ts], key=lambda r: r.timestamp_ms)
        if len(bucket_rows) != 3:
            # deterministic: only full aligned 15m buckets built from exactly 3 x 5m bars
            continue
        out.append(
            {
                "timestamp": bucket_ts,
                "open": bucket_rows[0].open,
                "high": max(r.high for r in bucket_rows),
                "low": min(r.low for r in bucket_rows),
                "close": bucket_rows[-1].close,
                "volume": sum(r.volume for r in bucket_rows),
            }
        )
    return out


def write_15m_csv(path: Path, rows_15m: list[dict[str, float | int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["timestamp", "open", "high", "low", "close", "volume"])
        writer.writeheader()
        writer.writerows(rows_15m)


def main() -> int:
    input_path = Path(sys.argv[1]).resolve() if len(sys.argv) > 1 else DEFAULT_INPUT
    output_path = Path(sys.argv[2]).resolve() if len(sys.argv) > 2 else DEFAULT_OUTPUT
    try:
        rows_5m = load_5m_rows(input_path)
        rows_15m = resample_15m(rows_5m)
        if not rows_15m:
            raise ValueError("no complete 15m buckets produced; check source data alignment")
        write_15m_csv(output_path, rows_15m)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    start_iso = datetime.fromtimestamp(rows_15m[0]["timestamp"] / 1000, tz=timezone.utc).isoformat()
    end_iso = datetime.fromtimestamp(rows_15m[-1]["timestamp"] / 1000, tz=timezone.utc).isoformat()
    print(
        f"Built 15m dataset: in_rows={len(rows_5m)} out_rows={len(rows_15m)} "
        f"start={start_iso} end={end_iso} output={output_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
