import csv
from datetime import datetime


class CSVReplayProvider:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path

    def load_rows(self) -> list[dict]:
        with open(self.csv_path, newline='', encoding='utf-8') as handle:
            reader = csv.DictReader(handle)
            rows = [row for row in reader]
        return rows

    @staticmethod
    def normalize_row(row: dict) -> dict:
        ts_value = row.get('timestamp') or row.get('ts')
        return {
            'timestamp': datetime.fromisoformat(ts_value).isoformat() if ts_value else None,
            'symbol': row.get('symbol', 'UNKNOWN'),
            'price': float(row.get('close', row.get('price', 0))),
            'volume': float(row.get('volume', 0)),
        }
