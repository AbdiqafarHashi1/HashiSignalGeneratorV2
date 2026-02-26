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


settings = Settings()
