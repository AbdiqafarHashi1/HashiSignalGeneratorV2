#!/usr/bin/env python3
import csv
import tempfile
import time

import requests

BASE = 'http://localhost:8000'


def make_csv(rows: int = 80) -> str:
    fd = tempfile.NamedTemporaryFile('w', suffix='_5m.csv', delete=False, newline='')
    writer = csv.DictWriter(fd, fieldnames=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol'])
    writer.writeheader()
    now = int(time.time())
    for i in range(rows):
        close = 102 if i % 2 == 0 else 98
        writer.writerow(
            {
                'timestamp': now + i * 60,
                'open': close,
                'high': close + 1,
                'low': close - 1,
                'close': close,
                'volume': 10 + i,
                'symbol': 'ETHUSDT',
            }
        )
    fd.close()
    return fd.name


def main() -> None:
    requests.get(f'{BASE}/health', timeout=5).raise_for_status()
    csv_path = make_csv()
    with open(csv_path, 'rb') as f:
        up = requests.post(f'{BASE}/replay/upload', files={'file': ('swing_5m.csv', f, 'text/csv')}, timeout=15)
    up.raise_for_status()
    dataset_id = up.json()['dataset_id']

    start = requests.post(f'{BASE}/replay/start', json={'dataset_id': dataset_id, 'speed': 5, 'resume': False}, timeout=10)
    start.raise_for_status()

    for _ in range(120):
        requests.post(f'{BASE}/replay/step', timeout=10).raise_for_status()

    trades = requests.get(f'{BASE}/trades', params={'limit': 200, 'offset': 0}, timeout=10).json().get('items', [])
    assert len(trades) > 0, 'Expected closed trades from replay'

    events = requests.get(f'{BASE}/events', params={'limit': 300, 'offset': 0}, timeout=10).json().get('items', [])
    trade_events = [e for e in events if str(e.get('regime', '')).upper() == 'TRADE']
    assert any(str(e.get('decision', '')).upper() == 'ENTRY' for e in trade_events), 'Expected TRADE ENTRY events'
    assert any(str(e.get('decision', '')).upper() == 'EXIT' for e in trade_events), 'Expected TRADE EXIT events'

    positions = requests.get(f'{BASE}/positions', timeout=10).json()
    assert len(positions) == 0, 'Expected no open positions after enough replay steps'

    requests.post(f'{BASE}/replay/stop', timeout=5).raise_for_status()
    print('replay trade-flow smoke ok', {'trades': len(trades), 'trade_events': len(trade_events)})


if __name__ == '__main__':
    main()
