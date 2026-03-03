from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any
from uuid import uuid4

from app.config import settings


class KillSwitchMode(StrEnum):
    OFF = 'off'
    SOFT = 'soft'
    HARD = 'hard'


class SafetyReasonCode(StrEnum):
    MANUAL_TRIP = 'manual_trip'
    STALE_DATA_FLAT = 'stale_data_flat'
    STALE_DATA_OPEN_POSITION = 'stale_data_open_position'
    ERROR_RATE_THRESHOLD = 'error_rate_threshold'
    CONSECUTIVE_ERRORS = 'consecutive_errors'
    KILL_SWITCH_SOFT = 'kill_switch_soft'
    KILL_SWITCH_HARD = 'kill_switch_hard'
    RECON_MISMATCH_PERSISTENT = 'recon_mismatch_persistent'
    ERROR_STORM = 'error_storm'


@dataclass
class RuntimeDecision:
    trip: bool
    mode: KillSwitchMode
    reason: str
    evidence: dict[str, Any]


class SafetyService:
    def __init__(self) -> None:
        self.enabled = bool(settings.live_safety_enabled)
        self.auto = bool(settings.kill_switch_auto)
        self.armed_mode = self._normalize_mode(settings.kill_switch_mode)
        self.last_trip_reason: str | None = None
        self.last_trip_mode: KillSwitchMode | None = None
        self.last_trip_at: str | None = None
        self.consecutive_errors = 0
        self.error_window: deque[int] = deque(maxlen=200)
        self.last_cycle_ok_at: datetime | None = None
        self.last_cycle_err_at: datetime | None = None
        self.last_error: str | None = None
        self._events_ring: deque[dict[str, Any]] = deque(maxlen=max(10, int(settings.incident_ring_size)))
        self._incidents: dict[str, dict[str, Any]] = {}
        self._incident_order: deque[str] = deque(maxlen=300)
        self._last_governor: dict[str, Any] | None = None
        self._recon_mismatch_cycles = 0
        self._recon_first_mismatch_at: datetime | None = None
        self._recon_last_detail: dict[str, Any] | None = None
        self._recon_mismatch_ts: deque[datetime] = deque(maxlen=1000)

    @staticmethod
    def _normalize_mode(raw: str | None) -> KillSwitchMode:
        normalized = str(raw or 'off').strip().lower()
        if normalized == KillSwitchMode.SOFT:
            return KillSwitchMode.SOFT
        if normalized == KillSwitchMode.HARD:
            return KillSwitchMode.HARD
        return KillSwitchMode.OFF

    @staticmethod
    def _mode_rank(mode: KillSwitchMode) -> int:
        if mode == KillSwitchMode.HARD:
            return 2
        if mode == KillSwitchMode.SOFT:
            return 1
        return 0

    def effective_mode(self) -> KillSwitchMode:
        return self.armed_mode

    def is_tripped_soft(self) -> bool:
        return self.effective_mode() == KillSwitchMode.SOFT

    def is_tripped_hard(self) -> bool:
        return self.effective_mode() == KillSwitchMode.HARD

    def set_governor_status(self, payload: dict[str, Any] | None) -> None:
        self._last_governor = payload or None

    def record_event(self, event_type: str, payload: dict[str, Any]) -> None:
        self._events_ring.append(
            {
                'ts': datetime.now(timezone.utc).isoformat(),
                'event_type': event_type,
                'payload': payload,
            }
        )

    def record_cycle_ok(self) -> None:
        self.last_cycle_ok_at = datetime.now(timezone.utc)
        self.consecutive_errors = 0
        self.error_window.append(0)
        self.last_error = None

    def record_cycle_error(self, error: str) -> None:
        self.last_cycle_err_at = datetime.now(timezone.utc)
        self.consecutive_errors += 1
        self.error_window.append(1)
        self.last_error = error
        self.record_event('ENGINE_ERROR', {'error': error})

    def current_error_rate(self) -> float:
        if not self.error_window:
            return 0.0
        return float(sum(self.error_window) / len(self.error_window))

    def staleness_ms(self) -> int | None:
        if not self.last_cycle_ok_at:
            return None
        return int((datetime.now(timezone.utc) - self.last_cycle_ok_at).total_seconds() * 1000)

    def pre_trade_allowed(self) -> tuple[bool, list[dict[str, Any]]]:
        if not self.enabled:
            return True, []
        mode = self.effective_mode()
        if mode == KillSwitchMode.OFF:
            return True, []
        reason = SafetyReasonCode.KILL_SWITCH_HARD if mode == KillSwitchMode.HARD else SafetyReasonCode.KILL_SWITCH_SOFT
        return (
            False,
            [
                {
                    'name': 'kill_switch',
                    'reason': reason,
                    'threshold': mode.value,
                    'current': mode.value,
                    'detail': f'Kill switch mode is {mode.value}',
                }
            ],
        )

    def execution_guard(self, action: str, reduce_only: bool = False) -> tuple[bool, str | None]:
        if not self.enabled:
            return True, None
        mode = self.effective_mode()
        normalized = str(action or '').lower()
        is_entry = normalized in {'entry', 'open', 'open_position', 'new_entry'}
        is_reduce_action = reduce_only or normalized in {'close', 'reduce', 'partial_close', 'exit', 'cancel'}
        if mode == KillSwitchMode.HARD and is_entry:
            return False, SafetyReasonCode.KILL_SWITCH_HARD
        if mode == KillSwitchMode.SOFT and is_entry:
            return False, SafetyReasonCode.KILL_SWITCH_SOFT
        if mode in {KillSwitchMode.SOFT, KillSwitchMode.HARD} and is_reduce_action:
            return True, None
        return True, None

    def record_reconciler(self, mismatch: bool, detail: dict[str, Any] | None = None) -> None:
        self._recon_last_detail = detail or {}
        now = datetime.now(timezone.utc)
        if mismatch:
            self._recon_mismatch_cycles += 1
            if not self._recon_first_mismatch_at:
                self._recon_first_mismatch_at = now
            self._recon_mismatch_ts.append(now)
        else:
            self._recon_mismatch_cycles = 0
            self._recon_first_mismatch_at = None
        self._trim_reconciler_window(now)

    def _trim_reconciler_window(self, now: datetime) -> None:
        while self._recon_mismatch_ts and (now - self._recon_mismatch_ts[0]).total_seconds() > 10:
            self._recon_mismatch_ts.popleft()

    def reset_reconciler(self) -> None:
        self._recon_mismatch_cycles = 0
        self._recon_first_mismatch_at = None
        self._recon_last_detail = None
        self._recon_mismatch_ts.clear()

    def reconciler_status(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        self._trim_reconciler_window(now)
        persisted_ms = 0
        if self._recon_first_mismatch_at:
            persisted_ms = int((now - self._recon_first_mismatch_at).total_seconds() * 1000)
        mismatch_rate_10s = len(self._recon_mismatch_ts)
        last_mismatch_ts = self._recon_mismatch_ts[-1].isoformat() if self._recon_mismatch_ts else None
        return {
            'ok': mismatch_rate_10s == 0,
            'status': 'OK' if mismatch_rate_10s == 0 else 'WARN',
            'mismatch_cycles': self._recon_mismatch_cycles,
            'mismatch_rate_10s': mismatch_rate_10s,
            'last_mismatch_ts': last_mismatch_ts,
            'persisted_ms': persisted_ms,
            'detail': self._recon_last_detail or {},
        }

    def evaluate_runtime(self, has_open_positions: bool) -> RuntimeDecision | None:
        if not self.enabled or not self.auto:
            return None
        recon = self.reconciler_status()
        if recon['mismatch_cycles'] > 0 and recon['persisted_ms'] >= int(settings.kill_switch_stale_ms):
            return RuntimeDecision(
                trip=True,
                mode=KillSwitchMode.HARD,
                reason=SafetyReasonCode.RECON_MISMATCH_PERSISTENT,
                evidence={
                    'reconciler': recon,
                    'has_open_positions': has_open_positions,
                },
            )
        stale = self.staleness_ms()
        if stale is not None and stale > int(settings.kill_switch_stale_ms):
            mode = KillSwitchMode.HARD if has_open_positions else KillSwitchMode.SOFT
            reason = (
                SafetyReasonCode.STALE_DATA_OPEN_POSITION
                if has_open_positions
                else SafetyReasonCode.STALE_DATA_FLAT
            )
            return RuntimeDecision(
                trip=True,
                mode=mode,
                reason=reason,
                evidence={'staleness_ms': stale, 'has_open_positions': has_open_positions},
            )
        error_rate = self.current_error_rate()
        if self.consecutive_errors >= int(settings.kill_switch_max_consec_errors):
            mode = KillSwitchMode.HARD if has_open_positions else KillSwitchMode.SOFT
            return RuntimeDecision(
                trip=True,
                mode=mode,
                reason=SafetyReasonCode.ERROR_STORM,
                evidence={
                    'consecutive_errors': self.consecutive_errors,
                    'max_consecutive': int(settings.kill_switch_max_consec_errors),
                    'error_rate': error_rate,
                },
            )
        if error_rate >= float(settings.kill_switch_max_error_rate) and len(self.error_window) >= 10:
            mode = KillSwitchMode.HARD if has_open_positions else KillSwitchMode.SOFT
            return RuntimeDecision(
                trip=True,
                mode=mode,
                reason=SafetyReasonCode.ERROR_RATE_THRESHOLD,
                evidence={
                    'error_rate': error_rate,
                    'max_error_rate': float(settings.kill_switch_max_error_rate),
                },
            )
        return None

    def arm(self, mode: str) -> dict[str, Any]:
        self.armed_mode = self._normalize_mode(mode)
        if self.armed_mode == KillSwitchMode.OFF:
            self.last_trip_reason = None
            self.last_trip_mode = None
            self.last_trip_at = None
        self.record_event('SAFETY_ARM', {'mode': self.armed_mode.value})
        return self.status()

    def trip(self, mode: str, reason: str, evidence: dict[str, Any] | None = None) -> tuple[bool, KillSwitchMode]:
        requested = self._normalize_mode(mode)
        if requested == KillSwitchMode.OFF:
            return False, self.armed_mode
        if self._mode_rank(requested) <= self._mode_rank(self.armed_mode):
            # Already at same or higher severity.
            self.last_trip_reason = reason
            self.last_trip_mode = self.armed_mode
            self.last_trip_at = datetime.now(timezone.utc).isoformat()
            return False, self.armed_mode
        self.armed_mode = requested
        self.last_trip_reason = reason
        self.last_trip_mode = requested
        self.last_trip_at = datetime.now(timezone.utc).isoformat()
        self.record_event(
            'KILL_SWITCH_TRIP',
            {
                'mode': requested.value,
                'reason': reason,
                'evidence': evidence or {},
            },
        )
        return True, requested

    def create_incident(self, reason: str, mode: KillSwitchMode, snapshot: dict[str, Any]) -> str:
        incident_id = str(uuid4())
        payload = {
            'id': incident_id,
            'ts': datetime.now(timezone.utc).isoformat(),
            'reason': reason,
            'mode': mode.value,
            'snapshot': snapshot,
        }
        self._incidents[incident_id] = payload
        self._incident_order.appendleft(incident_id)
        return incident_id

    def list_incidents(self) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for incident_id in self._incident_order:
            incident = self._incidents.get(incident_id)
            if not incident:
                continue
            items.append(
                {
                    'id': incident['id'],
                    'ts': incident['ts'],
                    'reason': incident['reason'],
                    'mode': incident['mode'],
                }
            )
        return items

    def get_incident(self, incident_id: str) -> dict[str, Any] | None:
        return self._incidents.get(incident_id)

    def status(self) -> dict[str, Any]:
        return {
            'enabled': self.enabled,
            'kill_mode': self.armed_mode.value,
            'last_trip_reason': self.last_trip_reason,
            'last_trip_mode': self.last_trip_mode.value if self.last_trip_mode else None,
            'last_trip_at': self.last_trip_at,
            'governor_last': self._last_governor,
            'consecutive_errors': self.consecutive_errors,
            'error_rate': self.current_error_rate(),
            'staleness_ms': self.staleness_ms(),
            'ring_size': len(self._events_ring),
            'reconciler': self.reconciler_status(),
            'last_error': self.last_error,
        }

    def ring_snapshot(self) -> list[dict[str, Any]]:
        return list(self._events_ring)
