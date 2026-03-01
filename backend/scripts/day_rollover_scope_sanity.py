from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class DayScopedCounter:
    scope: str = 'daily'
    day_key: str | None = None
    daily_consecutive_losses: int = 0
    global_consecutive_losses: int = 0

    @staticmethod
    def _day_key(ts: datetime) -> str:
        return ts.astimezone(timezone.utc).strftime('%Y-%m-%d')

    def rollover(self, ts: datetime) -> None:
        key = self._day_key(ts)
        if self.day_key is None:
            self.day_key = key
            return
        if key != self.day_key:
            self.day_key = key
            self.daily_consecutive_losses = 0

    def on_trade_close(self, pnl_net: float) -> None:
        if pnl_net < 0:
            self.daily_consecutive_losses += 1
            self.global_consecutive_losses += 1
        else:
            self.daily_consecutive_losses = 0
            self.global_consecutive_losses = 0

    def blocked(self, limit: int) -> bool:
        if self.scope == 'global':
            return self.global_consecutive_losses >= limit
        return self.daily_consecutive_losses >= limit


def assert_true(name: str, cond: bool) -> None:
    if not cond:
        raise AssertionError(f'FAIL: {name}')
    print(f'PASS: {name}')


def main() -> None:
    counter = DayScopedCounter(scope='daily')
    day1 = datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc)
    counter.rollover(day1)
    for _ in range(3):
        counter.on_trade_close(-10.0)
    assert_true('Day1 blocked after 3 losses', counter.blocked(limit=3))

    day2 = day1 + timedelta(days=1, minutes=1)
    counter.rollover(day2)
    assert_true('Daily counter reset on rollover', counter.daily_consecutive_losses == 0)
    assert_true('Unblocked after rollover (daily scope)', not counter.blocked(limit=3))

    counter_global = DayScopedCounter(scope='global')
    counter_global.rollover(day1)
    for _ in range(3):
        counter_global.on_trade_close(-10.0)
    assert_true('Global blocked after 3 losses', counter_global.blocked(limit=3))
    counter_global.rollover(day2)
    assert_true('Global scope stays blocked across rollover', counter_global.blocked(limit=3))
    print('Day rollover scope sanity checks complete')


if __name__ == '__main__':
    main()
