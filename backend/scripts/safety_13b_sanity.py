from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone


@dataclass
class MockSafety:
    mode: str = 'off'
    mismatch_cycles: int = 0
    first_mismatch_at: datetime | None = None

    def arm(self, mode: str) -> None:
        self.mode = mode

    def execution_guard(self, action: str, reduce_only: bool) -> tuple[bool, str | None]:
        if self.mode == 'hard' and action == 'entry':
            return False, 'kill_switch_hard'
        if self.mode == 'soft' and action == 'entry':
            return False, 'kill_switch_soft'
        if self.mode in {'soft', 'hard'} and action in {'close', 'reduce'} and not reduce_only:
            return False, 'reduce_only_required'
        return True, None

    def record_reconciler(self, mismatch: bool) -> None:
        if mismatch:
            self.mismatch_cycles += 1
            if not self.first_mismatch_at:
                self.first_mismatch_at = datetime.now(timezone.utc)
        else:
            self.mismatch_cycles = 0
            self.first_mismatch_at = None

    def should_trip_hard_for_recon(self, stale_ms: int) -> bool:
        if self.mismatch_cycles <= 0 or not self.first_mismatch_at:
            return False
        persisted_ms = int((datetime.now(timezone.utc) - self.first_mismatch_at).total_seconds() * 1000)
        return persisted_ms >= stale_ms


def assert_true(name: str, condition: bool) -> None:
    if not condition:
        raise AssertionError(f'FAIL: {name}')
    print(f'PASS: {name}')


def main() -> None:
    svc = MockSafety()

    svc.arm('soft')
    allowed_entry, reason = svc.execution_guard(action='entry', reduce_only=False)
    assert_true('SOFT blocks entries', not allowed_entry and reason is not None)
    allowed_close, _ = svc.execution_guard(action='close', reduce_only=True)
    assert_true('SOFT allows reduce-only closes', allowed_close)

    svc.arm('hard')
    allowed_entry_hard, reason_hard = svc.execution_guard(action='entry', reduce_only=False)
    assert_true('HARD blocks entries', not allowed_entry_hard and reason_hard is not None)
    allowed_reduce_hard, _ = svc.execution_guard(action='reduce', reduce_only=True)
    assert_true('HARD allows reduce-only', allowed_reduce_hard)

    svc.record_reconciler(mismatch=True)
    if svc.first_mismatch_at is not None:
        svc.first_mismatch_at = svc.first_mismatch_at - timedelta(seconds=10)
    assert_true('Persistent mismatch triggers hard trip condition', svc.should_trip_hard_for_recon(stale_ms=2500))

    print('Safety 13B sanity checks complete')


if __name__ == '__main__':
    main()
