#!/usr/bin/env python3
"""30-day profile replay smoke metrics."""

from __future__ import annotations

import os
from collections import Counter

import json
from urllib import error, parse, request

BASE = os.getenv('API_BASE', 'http://localhost:8000')
DATASET_PATH = os.getenv('REPLAY_DATASET', 'data/datasets/ETHUSDT_15m.csv')
TARGET_DAYS = int(os.getenv('SMOKE_DAYS', '30'))
CANDLES_PER_DAY_15M = 96
PROFILES = ('TREND_STABLE', 'GROWTH_HUNTER', 'PROP_HUNTER')


def _get(path: str, **kwargs):
    params = kwargs.get('params')
    url = f'{BASE}{path}'
    if params:
        url = f"{url}?{parse.urlencode(params)}"
    req = request.Request(url, method='GET')
    return _call(req)


def _post(path: str, **kwargs):
    payload = kwargs.get('json')
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode('utf-8')
        headers['Content-Type'] = 'application/json'
    req = request.Request(f'{BASE}{path}', method='POST', data=data, headers=headers)
    return _call(req)


def _call(req: request.Request):
    try:
        with request.urlopen(req, timeout=30) as resp:  # noqa: S310
            body = resp.read().decode('utf-8')
            parsed = json.loads(body) if body else {}
            return _Resp(status_code=resp.status, payload=parsed)
    except error.HTTPError as exc:
        body = exc.read().decode('utf-8') if hasattr(exc, 'read') else ''
        raise RuntimeError(f'HTTP {exc.code}: {body}') from exc


class _Resp:
    def __init__(self, status_code: int, payload: dict | list):
        self.status_code = status_code
        self._payload = payload

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f'HTTP {self.status_code}')

    def json(self):
        return self._payload


def _metric_summary(trades: list[dict], events: list[dict]) -> dict:
    closed = [t for t in trades if str(t.get('status', '')).upper() == 'CLOSED']
    pnls = [float(t.get('pnl_net') if t.get('pnl_net') is not None else (t.get('pnl') or 0.0)) for t in closed]
    wins = [v for v in pnls if v > 0]
    losses = [v for v in pnls if v < 0]
    entries = [e for e in events if str(e.get('event_type', '')).upper() == 'ENTRY']
    modules = Counter(str((e.get('risk_state_snapshot') or {}).get('entry_module') or 'unknown') for e in entries)
    avg_win = sum(wins) / len(wins) if wins else 0.0
    avg_loss = sum(losses) / len(losses) if losses else 0.0
    win_rate = (len(wins) / len(closed) * 100.0) if closed else 0.0
    expectancy = (sum(pnls) / len(pnls)) if pnls else 0.0
    return {
        'trades_count': len(closed),
        'win_rate_pct': win_rate,
        'expectancy': expectancy,
        'avg_win': avg_win,
        'avg_loss': avg_loss,
        'entries_by_module': dict(modules),
    }


def main() -> None:
    _get('/health').raise_for_status()
    datasets = _get('/replay/datasets').json()
    target = next((d for d in datasets if str(d.get('stored_path')) == DATASET_PATH or str(d.get('filename')) == os.path.basename(DATASET_PATH)), None)
    if not target:
        raise RuntimeError(f'dataset not found; expected {DATASET_PATH}')

    dataset_id = target['id']
    rows_count = int(target.get('rows_count') or 0)
    steps = min(rows_count, TARGET_DAYS * CANDLES_PER_DAY_15M) if rows_count else TARGET_DAYS * CANDLES_PER_DAY_15M

    for profile in PROFILES:
        _post('/control/set_profile', json={'profile': profile}).raise_for_status()
        _post('/replay/reset').raise_for_status()
        _post('/replay/start', json={'dataset_id': dataset_id, 'speed': 1000, 'resume': False}).raise_for_status()
        try:
            for _ in range(max(1, steps)):
                _post('/replay/step').raise_for_status()
        finally:
            _post('/replay/stop').raise_for_status()

        trades = _get('/trades', params={'limit': 5000, 'offset': 0}).json().get('items', [])
        events = _get('/events', params={'limit': 5000, 'offset': 0}).json().get('items', [])
        overview = _get('/overview').json()
        summary = _metric_summary(trades=trades, events=events)

        print('=== PROFILE SMOKE REPORT ===')
        print({'profile': profile, 'steps': steps, 'dataset_id': dataset_id})
        print(summary)
        print({'overview_active_profile': overview.get('active_profile'), 'overview_profile_stats': overview.get('profile_stats')})


if __name__ == '__main__':
    main()
