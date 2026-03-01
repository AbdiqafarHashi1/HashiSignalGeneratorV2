import csv
from datetime import datetime, timezone


class CSVReplayProvider:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def load_rows(self) -> list[dict]:
        with open(self.csv_path, newline='', encoding='utf-8') as handle:
            reader = csv.DictReader(handle)
            rows = [row for row in reader]
        return rows

    @staticmethod
    def normalize_row(row: dict, default_symbol: str | None = None) -> dict:
        lowered = {str(k).lower(): v for k, v in row.items()}
        ts_value = lowered.get('timestamp') or lowered.get('ts')
        timestamp = None
        if ts_value:
            ts_raw = str(ts_value).strip()
            if ts_raw.isdigit():
                num = int(ts_raw)
                if num < 1_000_000_000_000:
                    num *= 1000
                timestamp = datetime.fromtimestamp(num / 1000, tz=timezone.utc).isoformat()
            else:
                timestamp = datetime.fromisoformat(ts_raw.replace('Z', '+00:00')).isoformat()
        return {
            'timestamp': timestamp,
            'symbol': lowered.get('symbol') or default_symbol or 'UNKNOWN',
            'price': float(lowered.get('close', lowered.get('price', 0)) or 0),
            'open': float(lowered.get('open', lowered.get('price', 0)) or 0),
            'high': float(lowered.get('high', lowered.get('price', 0)) or 0),
            'low': float(lowered.get('low', lowered.get('price', 0)) or 0),
            'close': float(lowered.get('close', lowered.get('price', 0)) or 0),
            'volume': float(lowered.get('volume', 0) or 0),
        }
