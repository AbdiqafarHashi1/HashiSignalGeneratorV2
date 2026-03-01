#!/usr/bin/env python3
"""Replay smoke for observability diagnostics.

Runs ~30 days from ETHUSDT_15m dataset (or full dataset when shorter), steps through replay,
and prints decision-trace observability summaries.
"""

from __future__ import annotations

import os

import requests

BASE = os.getenv('API_BASE', 'http://localhost:8000')
DATASET_PATH = os.getenv('REPLAY_DATASET', 'data/datasets/ETHUSDT_15m.csv')
CANDLES_PER_DAY_15M = 96
TARGET_DAYS = int(os.getenv('SMOKE_DAYS', '30'))


def _get(path: str, **kwargs):
    return requests.get(f'{BASE}{path}', timeout=30, **kwargs)


def _post(path: str, **kwargs):
    return requests.post(f'{BASE}{path}', timeout=30, **kwargs)


def main() -> None:
    _get('/health').raise_for_status()

    datasets = _get('/replay/datasets').json()
    target = next((d for d in datasets if str(d.get('stored_path')) == DATASET_PATH or str(d.get('filename')) == os.path.basename(DATASET_PATH)), None)
    if not target:
        raise RuntimeError(f'dataset not found; expected {DATASET_PATH}. Upload it first via /replay/upload')

    dataset_id = target['id']
    rows_count = int(target.get('rows_count') or 0)
    target_bars = TARGET_DAYS * CANDLES_PER_DAY_15M
    steps = max(1, min(rows_count, target_bars)) if rows_count else target_bars

    _post('/replay/start', json={'dataset_id': dataset_id, 'speed': 1000, 'resume': False}).raise_for_status()
    try:
        for _ in range(steps):
            _post('/replay/step').raise_for_status()

        obs = _get('/replay/observability', params={'n': 2000}).json()
        trades = _get('/trades', params={'limit': 2000, 'offset': 0}).json().get('items', [])
        counters = obs.get('blocker_counters', {})
        top_blockers = counters.get('blockers_ranked', [])[:10]
        no_trade = obs.get('no_trade_streak', {})

        print('=== OBSERVABILITY SMOKE REPORT ===')
        print(f"dataset_id={dataset_id} steps={steps} rows_count={rows_count}")
        print('summary', {
            'total_bars': counters.get('total_bars'),
            'regime_pass': counters.get('regime_pass'),
            'regime_fail': counters.get('regime_fail'),
            'setup_confirmed_true': counters.get('setup_confirmed_true'),
            'setup_confirmed_false': counters.get('setup_confirmed_false'),
            'router_selected': counters.get('router_selected'),
        })
        print('top_blockers', top_blockers)
        print('trades_count', len(trades))
        print('longest_no_entry_streak', no_trade)
    finally:
        _post('/replay/stop').raise_for_status()


if __name__ == '__main__':
    main()
