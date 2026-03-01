from redis.asyncio import Redis

from app.config import settings
from app.db.session import SessionLocal
from app.services.engine import EngineService


redis_client = Redis.from_url(settings.redis_url, decode_responses=True)
engine_service = EngineService(redis_client=redis_client, session_factory=SessionLocal)


def get_engine_service() -> EngineService:
    return engine_service
