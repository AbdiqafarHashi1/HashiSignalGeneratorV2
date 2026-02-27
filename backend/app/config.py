from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    app_name: str = 'Hashi Signal Generator V2'
    api_prefix: str = '/api'

    database_url: str = 'postgresql+asyncpg://postgres:postgres@postgres:5432/hashisignal'
    redis_url: str = 'redis://redis:6379/0'

    engine_mode: str = 'live'
    leverage: float = 1.0

    telegram_bot_token: str = ''
    telegram_chat_id: str = ''
    data_dir: str = '/data'
    equity_start: float = 100000.0
    monthly_target_pct: float = 6.0
    cors_origins: str = 'http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001'
    default_symbol: str = 'ETHUSDT'
    taker_fee_rate: float = 0.0006
    maker_fee_rate: float = 0.0002
    replay_order_qty: float = 1.0
    replay_max_bars_in_trade: int = 500
    gov_max_trades_per_day: int = 999999
    gov_max_daily_loss_pct: float = 100.0
    gov_max_global_dd_pct: float = 100.0
    gov_max_consecutive_losses: int = 3
    gov_max_consec_losses_scope: str = 'daily'
    gov_cooldown_minutes: int = 10
    gov_daily_lock_on_breach: bool = True
    strategy_profile: str = 'TREND_STABLE'
    ema_fast: int = 20
    ema_slow: int = 50
    atr_len: int = 14
    regime_trend_min: float = 0.8
    pullback_lookback: int = 10
    score_min: float = 65.0
    time_stop_bars: int = 120
    tp1_r_mult: float = 1.0
    tp2_r_mult: float = 1.8
    partial_pct: float = 0.5
    live_safety_enabled: bool = False
    kill_switch_mode: str = 'off'
    kill_switch_auto: bool = True
    kill_switch_stale_ms: int = 2500
    kill_switch_max_error_rate: float = 0.2
    kill_switch_max_consec_errors: int = 5
    gov_emergency_unrealized_pct: float = 100.0
    gov_max_position_hold_secs: int = 999999
    incident_ring_size: int = 500
    incident_snapshot_on_kill: bool = True
    trading_day_tz: str = 'UTC'


settings = Settings()
