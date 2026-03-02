import asyncio

import pytest

from app.services.engine import EngineService


class FakeRedis:
    def __init__(self):
        self.storage = {}

    async def set(self, key: str, value: str) -> None:
        self.storage[key] = value

    async def get(self, key: str) -> str | None:
        return self.storage.get(key)


@pytest.mark.asyncio
async def test_engine_boot_cleanly() -> None:
    engine = EngineService(redis_client=FakeRedis())
    await engine.start()
    await asyncio.sleep(0.1)
    status = engine.status()
    assert status['running'] is True
    assert status['mode'] == 'live'
    await engine.stop()
