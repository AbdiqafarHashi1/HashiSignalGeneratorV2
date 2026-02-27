import csv
import os
import re
import uuid
from pathlib import Path

from fastapi import HTTPException, UploadFile

from app.config import settings

REQUIRED_COLUMNS = {'timestamp', 'open', 'high', 'low', 'close', 'volume'}


def ensure_upload_dir(data_dir: str) -> Path:
    upload_dir = Path(data_dir) / 'uploads'
    upload_dir.mkdir(parents=True, exist_ok=True)
    return upload_dir


def detect_timestamp_ms(value: str) -> int:
    stripped = str(value).strip()
    if not stripped:
        raise ValueError('timestamp value is empty')
    if stripped.isdigit():
        num = int(stripped)
        return num * 1000 if num < 1_000_000_000_000 else num
    # ISO datetime fallback
    from datetime import datetime

    return int(datetime.fromisoformat(stripped.replace('Z', '+00:00')).timestamp() * 1000)


def infer_timeframe_from_filename(filename: str) -> str | None:
    match = re.search(r'(\d+)\s*([mhd])(?:in)?(?:ute|our|ay)?', filename.lower())
    if not match:
        return None
    amount, unit = match.groups()
    return f'{amount}{unit}'


def infer_timeframe_from_spacing(samples: list[int]) -> str | None:
    if len(samples) < 2:
        return None
    samples = sorted(samples)
    diffs = [samples[i] - samples[i - 1] for i in range(1, len(samples)) if samples[i] > samples[i - 1]]
    if not diffs:
        return None
    spacing_ms = diffs[0]
    units = (
        ('d', 86_400_000),
        ('h', 3_600_000),
        ('m', 60_000),
    )
    for unit, ms in units:
        if spacing_ms % ms == 0:
            amount = spacing_ms // ms
            if amount > 0:
                return f'{amount}{unit}'
    return None


def validate_csv(path: Path) -> dict:
    try:
        with path.open(newline='', encoding='utf-8') as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError('CSV has no headers')
            header_map = {h.lower().strip(): h for h in reader.fieldnames}
            missing = [c for c in REQUIRED_COLUMNS if c not in header_map]
            if missing:
                raise ValueError(f"CSV missing required columns: {', '.join(sorted(missing))}")
            rows_count = 0
            start_ts = None
            end_ts = None
            symbol = None
            timeframe = None
            ts_samples: list[int] = []
            for row in reader:
                rows_count += 1
                raw_ts = row.get(header_map['timestamp'])
                ts_ms = detect_timestamp_ms(raw_ts)
                start_ts = ts_ms if start_ts is None else min(start_ts, ts_ms)
                end_ts = ts_ms if end_ts is None else max(end_ts, ts_ms)
                if len(ts_samples) < 5:
                    ts_samples.append(ts_ms)
                if symbol is None:
                    symbol = (row.get(header_map.get('symbol', ''), None) or '').strip() if 'symbol' in header_map else None
                if timeframe is None:
                    timeframe = (row.get(header_map.get('timeframe', ''), None) or '').strip() if 'timeframe' in header_map else None
            if rows_count == 0:
                raise ValueError('CSV contains no data rows')
            inferred_tf = infer_timeframe_from_filename(path.name) or infer_timeframe_from_spacing(ts_samples)
            timeframe = timeframe or inferred_tf
            symbol = symbol or settings.default_symbol
    except HTTPException:
        raise
    except Exception as exc:
        raise ValueError(str(exc)) from exc

    return {
        'rows_count': rows_count,
        'start_ts': start_ts,
        'end_ts': end_ts,
        'symbol': symbol,
        'timeframe': timeframe,
    }


async def persist_upload(file: UploadFile, data_dir: str) -> tuple[str, str, dict]:
    if not file.filename:
        raise HTTPException(status_code=400, detail='Missing filename in upload')

    upload_dir = ensure_upload_dir(data_dir)
    dataset_id = str(uuid.uuid4())
    safe_name = os.path.basename(file.filename)
    stored_name = f'{dataset_id}_{safe_name}'
    stored_path = upload_dir / stored_name

    content = await file.read()
    stored_path.write_bytes(content)
    try:
        metadata = validate_csv(stored_path)
    except ValueError as exc:
        stored_path.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f'Invalid CSV: {exc}') from exc

    return dataset_id, str(stored_path), metadata
