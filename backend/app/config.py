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
    telegram_enabled: bool | None = None
    telegram_status_interval_min: int = 60
    telegram_no_trade_interval_min: int = 30
    data_dir: str = '/data'
    equity_start: float = 100000.0
    monthly_target_pct: float = 6.0
    cors_origins: str = 'http://localhost:3000,http://127.0.0.1:3000,http://localhost:3001'
    default_symbol: str = 'ETHUSDT'
    taker_fee_rate: float = 0.0006
    maker_fee_rate: float = 0.0002
    replay_order_qty: float = 1.0
    replay_max_bars_in_trade: int = 500
    replay_dataset_default: str = 'data/datasets/ETHUSDT_15m.csv'
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
    atr_pct_min: float = 0.18
    htf_slope_lookback_bars: int = 8
    htf_slope_min_pct: float = 0.0020
    adx_len: int = 14
    adx_min: float = 18.0
    chop_lookback: int = 48
    chop_min_ratio: float = 0.55
    atr_regime_lookback: int = 96
    atr_expand_min: float = 1.08
    regime_require_htf: bool = False
    regime_htf_timeframe: str = '4h'
    regime_min_ltf_bars: int = 250
    regime_allow_ltf_fallback: bool = True
    feature_breakout: bool = False
    feature_pullback_v2: bool = False
    feature_vol_sizing: bool = False
    brk_lookback: int = 64
    brk_max_range_atr: float = 1.8
    brk_min_bars_in_box: int = 20
    brk_confirm_close: bool = True
    brk_entry_buffer_atr: float = 0.10
    brk_stop_atr: float = 1.2
    brk_tp1_r: float = 1.2
    brk_tp2_r: float = 3.0
    brk_recent_window: int = 12
    pb2_min_impulse_atr: float = 1.2
    pb2_max_retrace: float = 0.65
    pb2_min_retrace: float = 0.25
    pb2_require_hl_lh: bool = True
    pb2_confirm_bar: bool = True
    pb2_pivot_len: int = 3
    vol_sizing_min_mult: float = 0.35
    vol_sizing_max_mult: float = 1.30
    vol_sizing_atr_pct_low: float = 0.18
    vol_sizing_atr_pct_high: float = 0.60
    vol_sizing_chop_penalty: float = 0.70
    vol_sizing_trend_bonus: float = 1.10
    vol_sizing_enable_cap: bool = True
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
