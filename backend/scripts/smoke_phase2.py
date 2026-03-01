#!/usr/bin/env python3
import csv
import tempfile
import time

import requests

BASE = 'http://localhost:8000'


def make_csv() -> str:
    fd = tempfile.NamedTemporaryFile('w', suffix='.csv', delete=False, newline='')
    writer = csv.DictWriter(fd, fieldnames=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'symbol'])
    writer.writeheader()
    now = int(time.time())
    for i in range(10):
        writer.writerow({'timestamp': now + i, 'open': 100 + i, 'high': 101 + i, 'low': 99 + i, 'close': 100.5 + i, 'volume': 10 + i, 'symbol': 'BTCUSDT'})
    fd.close()
    return fd.name


def main() -> None:
    print('health', requests.get(f'{BASE}/health', timeout=5).json())
    print('overview', requests.get(f'{BASE}/overview', timeout=5).status_code)
    csv_path = make_csv()
    with open(csv_path, 'rb') as f:
        up = requests.post(f'{BASE}/replay/upload', files={'file': ('sample.csv', f, 'text/csv')}, timeout=10)
    up.raise_for_status()
    dataset_id = up.json()['dataset_id']
    print('dataset', dataset_id)
    st = requests.post(f'{BASE}/replay/start', json={'dataset_id': dataset_id, 'speed_multiplier': 5, 'resume': False}, timeout=10)
    st.raise_for_status()
    first = requests.get(f'{BASE}/replay/status', timeout=5).json().get('pointer_index', 0)
    time.sleep(2)
    second = requests.get(f'{BASE}/replay/status', timeout=5).json().get('pointer_index', 0)
    assert second >= first, 'Replay pointer did not advance'
    requests.post(f'{BASE}/replay/stop', timeout=5).raise_for_status()
    print('smoke ok', first, second)


if __name__ == '__main__':
    main()
