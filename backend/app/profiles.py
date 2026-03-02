from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

from app.config import Settings

PROFILE_REGISTRY: dict[str, dict[str, Any]] = {
    'TREND_STABLE': {},
    'GROWTH_HUNTER': {
        'feature_breakout': True,
        'feature_pullback_v2': True,
        'feature_vol_sizing': True,
        'regime_trend_min': 0.60,
        'score_min': 54,
        'adx_min': 14,
        'atr_pct_min': 0.14,
        'chop_min_ratio': 0.48,
        'atr_expand_min': 1.02,
        'brk_confirm_close': False,
        'brk_min_bars_in_box': 12,
        'brk_max_range_atr': 2.4,
        'brk_entry_buffer_atr': 0.05,
        'pb2_require_hl_lh': False,
        'pb2_confirm_bar': False,
        'pb2_min_retrace': 0.12,
        'time_stop_bars': 160,
        'tp1_r_mult': 0.9,
        'tp2_r_mult': 2.4,
        'partial_pct': 0.60,
        'vol_sizing_min_mult': 0.60,
        'vol_sizing_max_mult': 1.60,
        'vol_sizing_trend_bonus': 1.18,
        'vol_sizing_chop_penalty': 0.82,
        'gov_max_trades_per_day': 999999,
        'gov_max_daily_loss_pct': 100.0,
        'gov_max_global_dd_pct': 100.0,
        'gov_daily_lock_on_breach': False,
    },
    'PROP_HUNTER': {
        'feature_breakout': True,
        'feature_pullback_v2': True,
        'feature_vol_sizing': True,
        'regime_trend_min': 0.78,
        'score_min': 66,
        'adx_min': 18,
        'atr_pct_min': 0.16,
        'chop_min_ratio': 0.54,
        'atr_expand_min': 1.05,
        'brk_confirm_close': True,
        'brk_min_bars_in_box': 18,
        'brk_max_range_atr': 1.9,
        'brk_entry_buffer_atr': 0.10,
        'pb2_require_hl_lh': True,
        'pb2_confirm_bar': True,
        'pb2_min_retrace': 0.20,
        'time_stop_bars': 120,
        'tp1_r_mult': 1.0,
        'tp2_r_mult': 1.9,
        'partial_pct': 0.50,
        'vol_sizing_min_mult': 0.35,
        'vol_sizing_max_mult': 1.10,
        'vol_sizing_trend_bonus': 1.08,
        'vol_sizing_chop_penalty': 0.70,
        'gov_max_trades_per_day': 6,
        'gov_max_daily_loss_pct': 1.5,
        'gov_max_global_dd_pct': 6.0,
        'gov_max_consecutive_losses': 2,
        'gov_cooldown_minutes': 60,
        'gov_daily_lock_on_breach': True,
    },
}


class ProfileManager:
    """Applies profile overrides over env-loaded settings values."""

    def __init__(self, cfg: Settings):
        self.cfg = cfg
        self._base_values = self._capture_base_values(cfg)

    @staticmethod
    def _capture_base_values(cfg: Settings) -> dict[str, Any]:
        keys = {k for overrides in PROFILE_REGISTRY.values() for k in overrides.keys()}
        return {key: deepcopy(getattr(cfg, key)) for key in keys if hasattr(cfg, key)}

    def allowed_profiles(self) -> set[str]:
        return set(PROFILE_REGISTRY.keys())

    def apply(self, profile: str) -> str:
        normalized = str(profile).upper()
        if normalized not in PROFILE_REGISTRY:
            raise ValueError(f'Unsupported profile: {profile}')
        for key, value in self._base_values.items():
            setattr(self.cfg, key, deepcopy(value))
        overrides = PROFILE_REGISTRY[normalized]
        for key, value in overrides.items():
            setattr(self.cfg, key, deepcopy(value))
        self.cfg.active_profile = normalized
        self.cfg.strategy_profile = normalized
        return normalized


profile_manager: ProfileManager | None = None


def get_profile_manager(cfg: Settings) -> ProfileManager:
    global profile_manager
    if profile_manager is None or profile_manager.cfg is not cfg:
        profile_manager = ProfileManager(cfg)
    return profile_manager


def profile_overrides(profile: str) -> Mapping[str, Any]:
    return PROFILE_REGISTRY[str(profile).upper()]
