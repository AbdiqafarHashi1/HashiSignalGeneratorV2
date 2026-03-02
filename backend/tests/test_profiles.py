from app.config import settings
from app.profiles import get_profile_manager


def test_profile_overrides_apply_and_reset() -> None:
    manager = get_profile_manager(settings)
    original = {
        'score_min': settings.score_min,
        'gov_max_trades_per_day': settings.gov_max_trades_per_day,
        'feature_breakout': settings.feature_breakout,
    }

    manager.apply('GROWTH_HUNTER')
    assert settings.active_profile == 'GROWTH_HUNTER'
    assert settings.score_min == 54
    assert settings.gov_max_trades_per_day == 999999
    assert settings.feature_breakout is True

    manager.apply('PROP_HUNTER')
    assert settings.active_profile == 'PROP_HUNTER'
    assert settings.score_min == 66
    assert settings.gov_max_trades_per_day == 6

    manager.apply('TREND_STABLE')
    assert settings.active_profile == 'TREND_STABLE'
    assert settings.score_min == original['score_min']
    assert settings.gov_max_trades_per_day == original['gov_max_trades_per_day']
    assert settings.feature_breakout == original['feature_breakout']
