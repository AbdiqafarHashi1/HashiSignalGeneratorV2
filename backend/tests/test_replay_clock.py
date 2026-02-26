import csv

import pytest

from app.replay.replay_engine import ReplayEngine


@pytest.mark.asyncio
async def test_replay_clock_progression(tmp_path) -> None:
    csv_path = tmp_path / 'candles.csv'
    with open(csv_path, 'w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=['timestamp', 'symbol', 'close', 'volume'])
        writer.writeheader()
        writer.writerow({'timestamp': '2024-01-01T00:00:00', 'symbol': 'BTCUSDT', 'close': '100', 'volume': '1'})
        writer.writerow({'timestamp': '2024-01-01T00:01:00', 'symbol': 'BTCUSDT', 'close': '101', 'volume': '2'})

    replay = ReplayEngine(str(csv_path), speed_multiplier=2.0)
    await replay.start()
    first = await replay.next_tick()
    second = await replay.next_tick()
    assert first is not None
    assert second is not None
    assert second['replay_clock'] == 1
    assert replay.status()['cursor'] == 2
